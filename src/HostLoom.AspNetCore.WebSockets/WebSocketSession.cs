using System.Buffers;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Security.Claims;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class WebSocketSession : IWebSocketEventSink
{
    private readonly WebSocket _socket;
    private readonly IWebSocketHubProtocol _protocol;
    private readonly ClaimsPrincipal _user;
    private readonly GatewayConfiguration _configuration;
    private readonly WebSocketRequestRouter _router;
    private readonly WebSocketSessionRegistry _registry;
    private readonly ByteBoundedOutboundQueue _outbound;
    private readonly CancellationTokenSource _stop = new();
    private readonly ConcurrentDictionary<ulong, CancellationTokenSource> _requests = [];
    private readonly ConcurrentQueue<CancellationTokenSource> _requestCancellations = [];
    private readonly ConcurrentQueue<Task> _requestTasks = [];
    private readonly ConcurrentDictionary<ulong, SubscriptionState> _subscriptions = [];
    private WebSocketCloseStatus _closeStatus = WebSocketCloseStatus.NormalClosure;
    private string _closeReason = "Session completed.";
    private int _aborted;

    public WebSocketSession(
        WebSocket socket,
        IWebSocketHubProtocol protocol,
        ClaimsPrincipal user,
        GatewayConfiguration configuration,
        WebSocketRequestRouter router,
        WebSocketSessionRegistry registry
    )
    {
        _socket = socket;
        _protocol = protocol;
        _user = user;
        _configuration = configuration;
        _router = router;
        _registry = registry;
        _outbound = new ByteBoundedOutboundQueue(
            configuration.Options.MaximumQueuedBytesPerConnection,
            configuration.Options.MaximumQueuedFramesPerConnection
        );
        Id = Guid.NewGuid().ToString("N");
    }

    public string Id { get; }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _stop.Token
        );
        var writer = WriteLoopAsync(cancellationToken);
        if (
            !TryQueue(
                new HubFrame
                {
                    Kind = HubFrameKind.Welcome,
                    SessionId = Id,
                    MaximumMessageSize = _configuration.Options.MaximumMessageSize,
                    MaximumConcurrentRequests = _configuration
                        .Options
                        .MaximumConcurrentRequestsPerConnection,
                    Credit = _configuration.Options.MaximumCreditPerSubscription,
                }
            )
        )
        {
            Abort();
        }

        try
        {
            while (!linked.IsCancellationRequested && _socket.State is WebSocketState.Open)
            {
                var inbound = await ReceiveAsync(linked.Token).ConfigureAwait(false);
                if (inbound.IsClose)
                {
                    _closeStatus = inbound.CloseStatus ?? WebSocketCloseStatus.NormalClosure;
                    _closeReason =
                        _closeStatus is WebSocketCloseStatus.MessageTooBig
                            ? "The message exceeded the configured limit."
                            : "Peer closed the session.";
                    break;
                }

                if (inbound.MessageType != _protocol.MessageType)
                {
                    RequestClose(
                        WebSocketCloseStatus.InvalidMessageType,
                        "The frame type does not match the negotiated subprotocol."
                    );
                    break;
                }

                HubFrame frame;
                try
                {
                    frame = _protocol.Decode(inbound.Payload.Span);
                }
                catch (InvalidDataException)
                {
                    RequestClose(
                        WebSocketCloseStatus.InvalidPayloadData,
                        "The application frame could not be decoded."
                    );
                    break;
                }

                await HandleAsync(frame).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (linked.IsCancellationRequested) { }
        catch (InvalidDataException)
        {
            RequestClose(
                WebSocketCloseStatus.InvalidPayloadData,
                "The fragmented message was invalid."
            );
        }
        catch (WebSocketException)
        {
            Abort();
        }
        finally
        {
            await _stop.CancelAsync().ConfigureAwait(false);
            foreach (var request in _requests.Values)
            {
                await request.CancelAsync().ConfigureAwait(false);
            }

            foreach (var subscription in _subscriptions.Values)
            {
                _registry.Unsubscribe(this, subscription.Topic, subscription.Key);
            }

            _subscriptions.Clear();
            _outbound.Complete();
            await Task.WhenAll(_requestTasks.ToArray()).ConfigureAwait(false);
            await writer.ConfigureAwait(false);
            foreach (var request in _requestCancellations)
            {
                request.Dispose();
            }

            _stop.Dispose();
        }
    }

    public bool TryQueueEvent(
        string topic,
        string? subscriptionKey,
        string? eventKey,
        ReadOnlyMemory<byte> payload,
        string eventId,
        long sequence
    )
    {
        var accepted = true;
        foreach (var subscription in _subscriptions.Values)
        {
            if (
                !string.Equals(subscription.Topic, topic, StringComparison.Ordinal)
                || !string.Equals(subscription.Key, subscriptionKey, StringComparison.Ordinal)
                || !subscription.TryConsumeCredit()
            )
            {
                continue;
            }

            accepted &= TryQueue(
                new HubFrame
                {
                    Kind = HubFrameKind.Event,
                    StreamId = subscription.StreamId,
                    Topic = topic,
                    Key = eventKey,
                    EventId = eventId,
                    Sequence = sequence,
                    Payload = payload,
                }
            );
        }

        return accepted;
    }

    public void Abort()
    {
        if (Interlocked.Exchange(ref _aborted, 1) != 0)
        {
            return;
        }

        _socket.Abort();
    }

    private async ValueTask HandleAsync(HubFrame frame)
    {
        switch (frame.Kind)
        {
            case HubFrameKind.Request:
                StartRequest(frame);
                break;
            case HubFrameKind.Cancel:
                if (_requests.TryGetValue(frame.StreamId, out var request))
                {
                    await request.CancelAsync().ConfigureAwait(false);
                }

                break;
            case HubFrameKind.Subscribe:
                await SubscribeAsync(frame).ConfigureAwait(false);
                break;
            case HubFrameKind.Credit:
                AddCredit(frame);
                break;
            case HubFrameKind.Ack:
                Acknowledge(frame);
                break;
            case HubFrameKind.Unsubscribe:
                Unsubscribe(frame.StreamId, sendComplete: true);
                break;
            default:
                _ = TryQueue(
                    WebSocketRequestRouter.Fault(
                        frame.StreamId,
                        HubFaultCodes.InvalidFrame,
                        "This frame kind cannot be sent by a client."
                    )
                );
                break;
        }
    }

    private void StartRequest(HubFrame frame)
    {
        if (frame.StreamId == 0)
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    0,
                    HubFaultCodes.InvalidFrame,
                    "A non-zero stream id is required."
                )
            );
            return;
        }

        if (_requests.Count >= _configuration.Options.MaximumConcurrentRequestsPerConnection)
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.CapacityExceeded,
                    "Too many requests are active."
                )
            );
            return;
        }

        var cancellation = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        if (
            _subscriptions.ContainsKey(frame.StreamId)
            || !_requests.TryAdd(frame.StreamId, cancellation)
        )
        {
            cancellation.Dispose();
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.DuplicateStream,
                    "The request stream is already active."
                )
            );
            return;
        }

        _requestCancellations.Enqueue(cancellation);
#pragma warning disable CA2025 // RunAsync joins every queued task before disposing any queued cancellation source.
        var task = ProcessRequestAsync(frame, cancellation);
#pragma warning restore CA2025
        _requestTasks.Enqueue(task);
    }

    private async Task ProcessRequestAsync(HubFrame frame, CancellationTokenSource cancellation)
    {
        try
        {
            var response = await _router
                .RouteAsync(frame, _user, cancellation.Token)
                .ConfigureAwait(false);
            if (!_stop.IsCancellationRequested && !TryQueue(response))
            {
                Abort();
            }
        }
        finally
        {
            _requests.TryRemove(frame.StreamId, out _);
        }
    }

    private async ValueTask SubscribeAsync(HubFrame frame)
    {
        if (
            frame.StreamId == 0
            || string.IsNullOrWhiteSpace(frame.Topic)
            || !_configuration.TryGetTopic(frame.Topic, out var topic)
        )
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.TopicNotFound,
                    "The requested topic is not registered."
                )
            );
            return;
        }

        if (frame.Key is { Length: > 256 })
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.InvalidFrame,
                    "The subscription key is too long."
                )
            );
            return;
        }

        var credit = frame.Credit ?? 0;
        if (credit <= 0 || credit > _configuration.Options.MaximumCreditPerSubscription)
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.InvalidFrame,
                    "Initial credit is outside the allowed range."
                )
            );
            return;
        }

        if (_subscriptions.Count >= _configuration.Options.MaximumSubscriptionsPerConnection)
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.CapacityExceeded,
                    "Too many subscriptions are active."
                )
            );
            return;
        }

        if (!await _router.AuthorizeTopicAsync(topic, _user).ConfigureAwait(false))
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.Forbidden,
                    "The caller is not authorized for this topic."
                )
            );
            return;
        }

        var state = new SubscriptionState(frame.StreamId, topic.Name, frame.Key, credit);
        if (_requests.ContainsKey(frame.StreamId) || !_subscriptions.TryAdd(frame.StreamId, state))
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.DuplicateStream,
                    "The subscription stream is already active."
                )
            );
            return;
        }

        _registry.Subscribe(this, topic.Name, frame.Key);
        _ = TryQueue(
            new HubFrame
            {
                Kind = HubFrameKind.Subscribed,
                StreamId = frame.StreamId,
                Topic = topic.Name,
                Key = frame.Key,
                Credit = credit,
            }
        );
    }

    private void AddCredit(HubFrame frame)
    {
        if (
            !_subscriptions.TryGetValue(frame.StreamId, out var subscription)
            || frame.Credit is not { } credit
            || !subscription.TryAddCredit(
                credit,
                _configuration.Options.MaximumCreditPerSubscription
            )
        )
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.InvalidFrame,
                    "Credit is invalid for this subscription."
                )
            );
        }
    }

    private void Acknowledge(HubFrame frame)
    {
        if (
            !_subscriptions.TryGetValue(frame.StreamId, out var subscription)
            || frame.Sequence is not { } sequence
            || sequence <= 0
        )
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.InvalidFrame,
                    "The acknowledgement is invalid."
                )
            );
            return;
        }

        subscription.Acknowledge(sequence);
    }

    private void Unsubscribe(ulong streamId, bool sendComplete)
    {
        if (!_subscriptions.TryRemove(streamId, out var subscription))
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    streamId,
                    HubFaultCodes.InvalidFrame,
                    "The subscription does not exist."
                )
            );
            return;
        }

        _registry.Unsubscribe(this, subscription.Topic, subscription.Key);
        if (sendComplete)
        {
            _ = TryQueue(new HubFrame { Kind = HubFrameKind.Complete, StreamId = streamId });
        }
    }

    private bool TryQueue(HubFrame frame)
    {
        var payload = _protocol.Encode(frame);
        if (payload.Length > _configuration.Options.MaximumMessageSize)
        {
            return false;
        }

        return _outbound.TryWrite(payload, _protocol.MessageType);
    }

    private async Task WriteLoopAsync(CancellationToken cancellationToken)
    {
        try
        {
            await foreach (
                var frame in _outbound.Reader.ReadAllAsync(cancellationToken).ConfigureAwait(false)
            )
            {
                try
                {
                    await _socket
                        .SendAsync(frame.Payload, frame.MessageType, true, cancellationToken)
                        .ConfigureAwait(false);
                }
                finally
                {
                    _outbound.Release(frame);
                }
            }

            if (
                Volatile.Read(ref _aborted) == 0
                && _socket.State is WebSocketState.Open or WebSocketState.CloseReceived
            )
            {
                await _socket
                    .CloseOutputAsync(_closeStatus, _closeReason, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (WebSocketException)
        {
            Abort();
        }
    }

    private async ValueTask<InboundMessage> ReceiveAsync(CancellationToken cancellationToken)
    {
        var buffer = ArrayPool<byte>.Shared.Rent(_configuration.Options.ReceiveBufferSize);
        try
        {
            var writer = new ArrayBufferWriter<byte>();
            WebSocketMessageType? messageType = null;
            while (true)
            {
                var result = await _socket
                    .ReceiveAsync(buffer, cancellationToken)
                    .ConfigureAwait(false);
                if (result.MessageType is WebSocketMessageType.Close)
                {
                    return new InboundMessage(
                        ReadOnlyMemory<byte>.Empty,
                        WebSocketMessageType.Close,
                        true,
                        result.CloseStatus
                    );
                }

                messageType ??= result.MessageType;
                if (messageType != result.MessageType)
                {
                    throw new InvalidDataException("A fragmented message changed frame type.");
                }

                if (writer.WrittenCount + result.Count > _configuration.Options.MaximumMessageSize)
                {
                    return new InboundMessage(
                        ReadOnlyMemory<byte>.Empty,
                        result.MessageType,
                        true,
                        WebSocketCloseStatus.MessageTooBig
                    );
                }

                writer.Write(buffer.AsSpan(0, result.Count));
                if (result.EndOfMessage)
                {
                    return new InboundMessage(
                        writer.WrittenMemory.ToArray(),
                        result.MessageType,
                        false,
                        null
                    );
                }
            }
        }
        finally
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void RequestClose(WebSocketCloseStatus status, string reason)
    {
        _closeStatus = status;
        _closeReason = reason;
    }

    private readonly record struct InboundMessage(
        ReadOnlyMemory<byte> Payload,
        WebSocketMessageType MessageType,
        bool IsClose,
        WebSocketCloseStatus? CloseStatus
    );
}
