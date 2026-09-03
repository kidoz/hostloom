using System.Buffers;
using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Security.Claims;
using Microsoft.Extensions.Logging;

namespace HostLoom.AspNetCore.WebSockets;

internal sealed class WebSocketSession : IWebSocketSessionHandle
{
    private readonly WebSocket _socket;
    private readonly IWebSocketHubProtocol _protocol;
    private readonly ClaimsPrincipal _user;
    private readonly GatewayConfiguration _configuration;
    private readonly WebSocketRequestRouter _router;
    private readonly WebSocketSessionRegistry _registry;
    private readonly ByteBoundedOutboundQueue _outbound;
    private readonly TimeProvider _timeProvider;
    private readonly DateTimeOffset _connectedAt;
    private readonly DateTimeOffset _expiresAt;
    private readonly string? _subject;
    private readonly ControlFrameRateLimiter _controlFrames;
    private readonly ILogger<WebSocketSession> _logger;
    private readonly CancellationTokenSource _stop = new();
    private readonly TaskCompletionSource _completion = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    // Both are keyed by stream id and emptied as each request completes. Append-only collections
    // here would grow with the total number of requests a connection ever made, not the number in
    // flight, which is the opposite of the bounded per-connection memory the gateway promises.
    private readonly ConcurrentDictionary<ulong, CancellationTokenSource> _requests = [];
    private readonly ConcurrentDictionary<ulong, Task> _requestTasks = [];
    private readonly ConcurrentDictionary<ulong, SubscriptionState> _subscriptions = [];
    private readonly ConcurrentDictionary<ulong, Task> _subscriptionTasks = [];
    private WebSocketCloseStatus _closeStatus = WebSocketCloseStatus.NormalClosure;
    private string _closeReason = "Session completed.";
    private int _aborted;
    private int _closeAssigned;
    private int _slowClientAbortLogged;

    public WebSocketSession(
        WebSocket socket,
        IWebSocketHubProtocol protocol,
        ClaimsPrincipal user,
        GatewayConfiguration configuration,
        WebSocketRequestRouter router,
        WebSocketSessionRegistry registry,
        TimeProvider timeProvider,
        DateTimeOffset connectedAt,
        DateTimeOffset expiresAt,
        string? subject,
        ILogger<WebSocketSession> logger
    )
    {
        _socket = socket;
        _protocol = protocol;
        _user = user;
        _configuration = configuration;
        _router = router;
        _registry = registry;
        _timeProvider = timeProvider;
        _connectedAt = connectedAt;
        _expiresAt = expiresAt;
        _subject = subject;
        _logger = logger;
        _controlFrames = new(timeProvider, configuration.Options.MaximumControlFramesPerSecond);
        _outbound = new ByteBoundedOutboundQueue(
            configuration.Options.MaximumQueuedBytesPerConnection,
            configuration.Options.MaximumQueuedFramesPerConnection
        );
        Id = Guid.NewGuid().ToString("N");
    }

    public string Id { get; }

    public Task Completion => _completion.Task;

    public WebSocketSessionInfo GetInfo() =>
        new(Id, _subject, _protocol.SubProtocol, _connectedAt, _expiresAt, _subscriptions.Count);

    public void RequestDisconnect(WebSocketCloseStatus status, string reason)
    {
        RequestClose(status, reason);
        try
        {
            _stop.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The session completed between a directory snapshot and the disconnect request.
        }
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        _registry.Register(this);
        WebSocketDiagnostics.SessionOpened(_protocol.SubProtocol);
        WebSocketLog.SessionOpened(_logger, Id, _protocol.SubProtocol, _subject);
        using var expiryCancellation = new CancellationTokenSource();
        var expiry = ExpireAsync(expiryCancellation.Token);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _stop.Token
        );
        var writer = WriteLoopAsync(cancellationToken);
        try
        {
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

            while (!linked.IsCancellationRequested && _socket.State is WebSocketState.Open)
            {
                var inbound = await ReceiveAsync(linked.Token).ConfigureAwait(false);
                if (inbound.IsClose)
                {
                    var closeStatus = inbound.CloseStatus ?? WebSocketCloseStatus.NormalClosure;
                    RequestClose(
                        closeStatus,
                        closeStatus is WebSocketCloseStatus.MessageTooBig
                            ? "The message exceeded the configured limit."
                            : "Peer closed the session."
                    );
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
            try
            {
                await expiryCancellation.CancelAsync().ConfigureAwait(false);
                try
                {
                    await expiry.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (expiryCancellation.IsCancellationRequested)
                { }

                await _stop.CancelAsync().ConfigureAwait(false);
                foreach (var request in _requests.Values)
                {
                    try
                    {
                        await request.CancelAsync().ConfigureAwait(false);
                    }
                    catch (ObjectDisposedException)
                    {
                        // The request completed and disposed its own source between the snapshot and
                        // this call. Cancelling a finished request has nothing left to do anyway.
                    }
                }

                foreach (var subscription in _subscriptions.Values)
                {
                    _registry.Unsubscribe(this, subscription.Topic, subscription.Key);
                    subscription.Stop(_outbound.Release);
                    WebSocketDiagnostics.SubscriptionRemoved(subscription.Topic);
                }

                _subscriptions.Clear();
                await Task.WhenAll([.. _subscriptionTasks.Values]).ConfigureAwait(false);
                _outbound.Complete();
                await Task.WhenAll([.. _requestTasks.Values]).ConfigureAwait(false);
                await writer.ConfigureAwait(false);

                // Whatever is still registered never reached its own cleanup. TryRemove decides the
                // owner, so a source is disposed exactly once however the session ended.
                foreach (var streamId in _requests.Keys)
                {
                    if (_requests.TryRemove(streamId, out var request))
                    {
                        request.Dispose();
                    }
                }
            }
            finally
            {
                _registry.Unregister(this);
                var duration = Math.Max(0, (_timeProvider.GetUtcNow() - _connectedAt).TotalSeconds);
                var closeReason = GetDiagnosticCloseReason();
                WebSocketDiagnostics.SessionClosed(_protocol.SubProtocol, duration, closeReason);
                WebSocketLog.SessionClosed(
                    _logger,
                    Id,
                    _protocol.SubProtocol,
                    _subject,
                    closeReason,
                    _closeStatus,
                    duration * 1000
                );
                _stop.Dispose();
                _completion.TrySetResult();
            }
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
            )
            {
                continue;
            }

            var encoded = _protocol.Encode(
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
            if (encoded.Length > _configuration.Options.MaximumMessageSize)
            {
                WebSocketDiagnostics.EventDropped(topic, "message_too_large");
                accepted = false;
                continue;
            }

            switch (
                subscription.AcceptLiveEvent(
                    _outbound,
                    encoded,
                    _protocol.MessageType,
                    out var frame
                )
            )
            {
                case LiveEventDisposition.Buffered:
                    break;
                case LiveEventDisposition.Active:
                    if (!_outbound.TryWriteReserved(frame))
                    {
                        WebSocketDiagnostics.EventDropped(topic, "queue_unavailable");
                        accepted = false;
                    }

                    break;
                case LiveEventDisposition.Dropped:
                    WebSocketDiagnostics.EventDropped(topic, "no_credit");
                    break;
                case LiveEventDisposition.Stopped:
                    WebSocketDiagnostics.EventDropped(topic, "subscription_stopped");
                    break;
                case LiveEventDisposition.CapacityExceeded:
                    WebSocketDiagnostics.EventDropped(topic, "queue_capacity");
                    AbortSlowClient(HubFrameKind.Event, topic);
                    accepted = false;
                    break;
                default:
                    throw new InvalidOperationException(
                        "The subscription delivery state is invalid."
                    );
            }
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
        if (IsControlFrame(frame.Kind) && !_controlFrames.TryAcquire())
        {
            RequestDisconnect(WebSocketCloseStatus.PolicyViolation, "rate_limited");
            return;
        }

        switch (frame.Kind)
        {
            case HubFrameKind.Request:
                StartRequest(frame);
                break;
            case HubFrameKind.Cancel:
                if (_requests.TryGetValue(frame.StreamId, out var request))
                {
                    try
                    {
                        await request.CancelAsync().ConfigureAwait(false);
                    }
                    catch (ObjectDisposedException)
                    {
                        // The request completed and disposed its own source between the lookup
                        // and this call. Cancelling a finished request has nothing left to do,
                        // and a client sending request/cancel pairs widens the window at will —
                        // so this must not escape into the connection's fault path.
                    }
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

#pragma warning disable CA2025 // The request disposes its own source in ProcessRequestAsync, and RunAsync joins every tracked task before disposing what is left.
        var task = ProcessRequestAsync(frame, cancellation);
#pragma warning restore CA2025
        _requestTasks[frame.StreamId] = task;
        if (task.IsCompleted)
        {
            // A request that finished before this assignment already ran its cleanup, so it would
            // otherwise leave the entry behind for the life of the connection.
            _requestTasks.TryRemove(frame.StreamId, out _);
        }
    }

    private async Task ProcessRequestAsync(HubFrame frame, CancellationTokenSource cancellation)
    {
        try
        {
            var response = await _router
                .RouteAsync(frame, _user, cancellation.Token, _protocol.SubProtocol)
                .ConfigureAwait(false);
            if (!_stop.IsCancellationRequested && !TryQueue(response))
            {
                Abort();
            }
        }
        finally
        {
            // Releasing here, rather than at session end, is what keeps a long-lived connection's
            // memory proportional to requests in flight. TryRemove decides the owner so the
            // shutdown path cannot dispose the same source a second time.
            if (_requests.TryRemove(frame.StreamId, out var cancellationSource))
            {
                cancellationSource.Dispose();
            }

            _requestTasks.TryRemove(frame.StreamId, out _);
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
            DenySubscription(
                frame.StreamId,
                topic: null,
                "topic_not_found",
                HubFaultCodes.TopicNotFound,
                "The requested topic is not registered."
            );
            return;
        }

        if (frame.Key is { Length: > 256 })
        {
            DenySubscription(
                frame.StreamId,
                topic.Name,
                "key_too_long",
                HubFaultCodes.InvalidFrame,
                "The subscription key is too long."
            );
            return;
        }

        var credit = frame.Credit ?? 0;
        if (credit <= 0 || credit > _configuration.Options.MaximumCreditPerSubscription)
        {
            DenySubscription(
                frame.StreamId,
                topic.Name,
                "invalid_credit",
                HubFaultCodes.InvalidFrame,
                "Initial credit is outside the allowed range."
            );
            return;
        }

        if (_subscriptions.Count >= _configuration.Options.MaximumSubscriptionsPerConnection)
        {
            DenySubscription(
                frame.StreamId,
                topic.Name,
                "capacity",
                HubFaultCodes.CapacityExceeded,
                "Too many subscriptions are active."
            );
            return;
        }

        if (!await _router.AuthorizeTopicAsync(topic, frame.Key, _user).ConfigureAwait(false))
        {
            DenySubscription(
                frame.StreamId,
                topic.Name,
                "forbidden",
                HubFaultCodes.Forbidden,
                "The caller is not authorized for this topic."
            );
            return;
        }

        var state = new SubscriptionState(
            frame.StreamId,
            topic.Name,
            frame.Key,
            credit,
            _stop.Token
        );
        if (_requests.ContainsKey(frame.StreamId) || !_subscriptions.TryAdd(frame.StreamId, state))
        {
            DenySubscription(
                frame.StreamId,
                topic.Name,
                "duplicate_stream",
                HubFaultCodes.DuplicateStream,
                "The subscription stream is already active."
            );
            return;
        }

        _registry.Subscribe(this, topic.Name, frame.Key);
        WebSocketDiagnostics.SubscriptionAdded(topic.Name);
        if (
            !TryQueue(
                new HubFrame
                {
                    Kind = HubFrameKind.Subscribed,
                    StreamId = frame.StreamId,
                    Topic = topic.Name,
                    Key = frame.Key,
                    Credit = credit,
                }
            )
        )
        {
            Abort();
            return;
        }

        StartSubscriptionInitialization(topic, state);
    }

    private void StartSubscriptionInitialization(TopicRoute topic, SubscriptionState state)
    {
#pragma warning disable CA2025 // RunAsync joins every tracked initialization task before releasing session state.
        var task = InitializeSubscriptionAsync(topic, state);
#pragma warning restore CA2025
        _subscriptionTasks[state.StreamId] = task;
        if (task.IsCompleted)
        {
            _subscriptionTasks.TryRemove(state.StreamId, out _);
        }
    }

    private async Task InitializeSubscriptionAsync(TopicRoute topic, SubscriptionState state)
    {
        try
        {
            var context = new WebSocketTopicSnapshotContext(topic.Name, state.Key, _user);
            await foreach (
                var item in _router
                    .GetTopicSnapshotAsync(topic, context, state.SnapshotCancellationToken)
                    .WithCancellation(state.SnapshotCancellationToken)
                    .ConfigureAwait(false)
            )
            {
                if (
                    state.Key is not null
                    && !string.Equals(state.Key, item.Key, StringComparison.Ordinal)
                )
                {
                    continue;
                }

                await state
                    .WaitForCreditAsync(state.SnapshotCancellationToken)
                    .ConfigureAwait(false);
                var write = state.WriteSnapshot(() =>
                    TryQueue(
                        new HubFrame
                        {
                            Kind = HubFrameKind.Event,
                            StreamId = state.StreamId,
                            Topic = topic.Name,
                            Key = item.Key,
                            EventId = Guid.NewGuid().ToString("N"),
                            Sequence = 0,
                            Payload = item.Payload,
                        }
                    )
                );
                if (write is SnapshotWriteDisposition.Failed)
                {
                    Abort();
                    return;
                }

                if (write is SnapshotWriteDisposition.Stopped)
                {
                    return;
                }
            }

            if (!state.CompleteInitialization(_outbound.TryWriteReserved, _outbound.Release))
            {
                Abort();
            }
        }
        catch (OperationCanceledException)
            when (state.SnapshotCancellationToken.IsCancellationRequested) { }
        catch (Exception) when (state.SnapshotCancellationToken.IsCancellationRequested) { }
        catch (Exception exception)
        {
            WebSocketLog.SnapshotFailed(_logger, topic.Name, Id, exception);
            if (
                _subscriptions.TryRemove(
                    new KeyValuePair<ulong, SubscriptionState>(state.StreamId, state)
                )
            )
            {
                _registry.Unsubscribe(this, state.Topic, state.Key);
                state.Stop(_outbound.Release);
                WebSocketDiagnostics.SubscriptionRemoved(state.Topic);
                if (
                    !TryQueue(
                        WebSocketRequestRouter.Fault(
                            state.StreamId,
                            HubFaultCodes.SnapshotFailed,
                            "The topic snapshot could not be loaded."
                        )
                    )
                )
                {
                    Abort();
                }
            }
        }
        finally
        {
            _subscriptionTasks.TryRemove(state.StreamId, out _);
        }
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
        subscription.Stop(_outbound.Release);
        WebSocketDiagnostics.SubscriptionRemoved(subscription.Topic);
        if (sendComplete)
        {
            _ = TryQueue(new HubFrame { Kind = HubFrameKind.Complete, StreamId = streamId });
        }
    }

    private void DenySubscription(
        ulong streamId,
        string? topic,
        string reason,
        string faultCode,
        string faultMessage
    )
    {
        WebSocketLog.SubscriptionDenied(_logger, Id, topic, reason);
        _ = TryQueue(WebSocketRequestRouter.Fault(streamId, faultCode, faultMessage));
    }

    private bool TryQueue(HubFrame frame)
    {
        if (frame.Kind is HubFrameKind.Fault && frame.Code is { } code)
        {
            WebSocketDiagnostics.FaultGenerated(code);
        }

        if (!TryReserve(frame, out var reserved))
        {
            return false;
        }

        if (_outbound.TryWriteReserved(reserved))
        {
            return true;
        }

        if (frame.Kind is HubFrameKind.Event && frame.Topic is { } topic)
        {
            WebSocketDiagnostics.EventDropped(topic, "queue_unavailable");
        }

        return false;
    }

    private bool TryReserve(HubFrame frame, out OutboundFrame reserved)
    {
        var payload = _protocol.Encode(frame);
        if (payload.Length > _configuration.Options.MaximumMessageSize)
        {
            if (frame.Kind is HubFrameKind.Event && frame.Topic is { } topic)
            {
                WebSocketDiagnostics.EventDropped(topic, "message_too_large");
            }

            reserved = default;
            return false;
        }

        var eventTopic = frame.Kind is HubFrameKind.Event ? frame.Topic : null;
        if (_outbound.TryReserve(payload, _protocol.MessageType, eventTopic, out reserved))
        {
            return true;
        }

        if (eventTopic is not null)
        {
            WebSocketDiagnostics.EventDropped(eventTopic, "queue_capacity");
        }

        AbortSlowClient(frame.Kind, eventTopic);

        return false;
    }

    private void AbortSlowClient(HubFrameKind frameKind, string? topic)
    {
        if (Interlocked.Exchange(ref _slowClientAbortLogged, 1) != 0)
        {
            return;
        }

        WebSocketLog.SlowClientAborted(
            _logger,
            Id,
            frameKind,
            topic,
            _configuration.Options.MaximumQueuedFramesPerConnection,
            _configuration.Options.MaximumQueuedBytesPerConnection
        );
        Abort();
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
                    if (frame.EventTopic is { } topic)
                    {
                        WebSocketDiagnostics.EventSent(topic);
                    }
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
        if (Interlocked.CompareExchange(ref _closeAssigned, 1, 0) != 0)
        {
            return;
        }

        _closeStatus = status;
        _closeReason = reason;
    }

    private async Task ExpireAsync(CancellationToken cancellationToken)
    {
        var delay = _expiresAt - _timeProvider.GetUtcNow();
        if (delay > TimeSpan.Zero)
        {
            await Task.Delay(delay, _timeProvider, cancellationToken).ConfigureAwait(false);
        }

        RequestDisconnect(WebSocketCloseStatus.PolicyViolation, "session_expired");
    }

    private string GetDiagnosticCloseReason()
    {
        if (Volatile.Read(ref _aborted) != 0)
        {
            return "aborted";
        }

        return _closeReason switch
        {
            "session_expired" => "session_expired",
            "server_shutdown" => "server_shutdown",
            "rate_limited" => "rate_limited",
            "The message exceeded the configured limit." => "message_too_large",
            "The frame type does not match the negotiated subprotocol." => "invalid_message_type",
            "The application frame could not be decoded."
            or "The fragmented message was invalid." => "invalid_payload",
            "Peer closed the session." => "peer_closed",
            "Session completed." => "completed",
            _ when _closeStatus is WebSocketCloseStatus.PolicyViolation => "policy_violation",
            _ when _closeStatus is WebSocketCloseStatus.EndpointUnavailable =>
                "endpoint_unavailable",
            _ => "other",
        };
    }

    private static bool IsControlFrame(HubFrameKind kind) =>
        kind
            is HubFrameKind.Cancel
                or HubFrameKind.Subscribe
                or HubFrameKind.Credit
                or HubFrameKind.Ack
                or HubFrameKind.Unsubscribe;

    private readonly record struct InboundMessage(
        ReadOnlyMemory<byte> Payload,
        WebSocketMessageType MessageType,
        bool IsClose,
        WebSocketCloseStatus? CloseStatus
    );
}
