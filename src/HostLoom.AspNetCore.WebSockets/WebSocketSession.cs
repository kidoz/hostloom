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
    private readonly FixedWindowRateLimiter _controlFrames;
    private readonly FixedWindowRateLimiter _requestFrames;
    private readonly ILogger<WebSocketSession> _logger;
    private readonly CancellationTokenSource _stop = new();
    private readonly TaskCompletionSource _completion = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    // Both are keyed by stream id and emptied as each request completes. Append-only collections
    // here would grow with the total number of requests a connection ever made, not the number in
    // flight, which is the opposite of the bounded per-connection memory the gateway promises.
    private readonly ConcurrentDictionary<Guid, CancellationTokenSource> _requests = [];
    private readonly ConcurrentDictionary<Guid, Task> _requestTasks = [];
    private readonly ConcurrentDictionary<Guid, SubscriptionState> _subscriptions = [];
    private readonly ConcurrentDictionary<Guid, Task> _subscriptionTasks = [];
    private readonly Lock _closeGate = new();
    private CloseRequest? _close;
    private ITimer? _closeTimeout;
    private bool _closing;
    private int _aborted;
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
        _requestFrames = new(timeProvider, configuration.Options.MaximumRequestsPerSecond);
        _outbound = new ByteBoundedOutboundQueue(
            configuration.Options.MaximumQueuedBytesPerConnection,
            configuration.Options.MaximumQueuedFramesPerConnection
        );
        Id = Guid.NewGuid();
    }

    public Guid Id { get; }

    public Task Completion => _completion.Task;

    public WebSocketSessionInfo GetInfo() =>
        new(Id, _subject, _protocol.SubProtocol, _connectedAt, _expiresAt, _subscriptions.Count);

    /// <summary>
    /// Starts a close from this side: request and snapshot work stops, the writer sends the frames
    /// already queued and then the close frame, and the receive loop keeps reading until the peer
    /// answers. The pending receive is never cancelled for this, because the runtime's socket
    /// aborts when it is and the peer would see an abnormal closure instead of this status.
    /// </summary>
    public void RequestDisconnect(WebSocketCloseStatus status, string reason)
    {
        RequestClose(status, reason);
        lock (_closeGate)
        {
            if (_closing)
            {
                return;
            }

            // Armed before the close frame can be queued, so a peer that has read the frame is
            // always inside the timeout. The session counts as closing only once the timer
            // exists: a clock that cannot create one leaves it open rather than closing unbounded.
            _closeTimeout = _timeProvider.CreateTimer(
                static state => ((WebSocketSession)state!).AbortUnansweredClose(),
                this,
                _configuration.Options.CloseTimeout,
                Timeout.InfiniteTimeSpan
            );
            _closing = true;
        }

        try
        {
            _stop.Cancel();
        }
        catch (ObjectDisposedException)
        {
            // The session finished its own teardown between the gate and this call.
        }

        _outbound.Complete();
    }

    public async Task RunAsync(CancellationToken cancellationToken)
    {
        var shutdown = _registry.Register(this);
        WebSocketDiagnostics.SessionOpened(_protocol.SubProtocol);
        WebSocketLog.SessionOpened(_logger, Id, _protocol.SubProtocol, _subject);
        using var expiryCancellation = new CancellationTokenSource();
        var expiry = ExpireAsync(expiryCancellation.Token);
        var writer = WriteLoopAsync(cancellationToken);
        try
        {
            if (shutdown is { } close)
            {
                // Accepted after the host began stopping: the close replaces the welcome.
                RequestDisconnect(close.Status, close.Reason);
            }

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
                AbortUnlessClosing();
            }

            // Only the host's token reaches the receive: cancelling it aborts the runtime's
            // socket. A close started here is sent by the writer instead, and the loop keeps
            // reading until the peer's close frame arrives or the close timeout aborts the socket.
            while (_socket.State is WebSocketState.Open or WebSocketState.CloseSent)
            {
                var inbound = await ReceiveAsync(cancellationToken).ConfigureAwait(false);
                if (inbound.Kind is InboundKind.Close)
                {
                    // Either the peer started the close, or this answers one started here and
                    // the status already chosen is kept.
                    RequestClose(
                        inbound.CloseStatus ?? WebSocketCloseStatus.NormalClosure,
                        "Peer closed the session."
                    );
                    break;
                }

                if (Volatile.Read(ref _closing))
                {
                    // The close frame is queued or sent. Whatever the peer sent before reading
                    // it is discarded while the loop waits for the answer.
                    continue;
                }

                if (inbound.Kind is InboundKind.TooLarge)
                {
                    RequestDisconnect(
                        WebSocketCloseStatus.MessageTooBig,
                        "The message exceeded the configured limit."
                    );
                    continue;
                }

                if (inbound.Kind is InboundKind.InvalidFragment)
                {
                    RequestDisconnect(
                        WebSocketCloseStatus.InvalidPayloadData,
                        "The fragmented message was invalid."
                    );
                    continue;
                }

                if (inbound.MessageType != _protocol.MessageType)
                {
                    RequestDisconnect(
                        WebSocketCloseStatus.InvalidMessageType,
                        "The frame type does not match the negotiated subprotocol."
                    );
                    continue;
                }

                try
                {
                    await ProcessAsync(inbound.Payload).ConfigureAwait(false);
                }
                catch (Exception exception) when (IsUnexpected(exception, cancellationToken))
                {
                    // A defect in a codec or in frame handling ends the session through the close
                    // handshake like any other close started here, so the loop keeps reading
                    // until the peer answers.
                    Fail(exception);
                }
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception exception)
            when ((exception is OperationCanceledException or ObjectDisposedException) && IsAborted)
        {
            // An abort fails the pending receive, whether it came from this session (slow client,
            // close timeout, writer failure) or from the runtime's keep-alive timeout.
            Abort();
        }
        catch (WebSocketException)
        {
            Abort();
        }
        catch (Exception exception)
        {
            // Last resort for a failure outside frame processing. The writer still sends the
            // close frame below, so the peer sees 1011 instead of a normal closure, and the
            // exception does not reach the server as an unhandled request failure.
            Fail(exception);
        }
        finally
        {
            ITimer? closeTimeout;
            lock (_closeGate)
            {
                // Teardown owns the queue from here, so a disconnect that arrives late arms no
                // timer and completes nothing.
                _closing = true;
                closeTimeout = _closeTimeout;
            }

            try
            {
                await expiryCancellation.CancelAsync().ConfigureAwait(false);
                try
                {
                    await expiry.ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (expiryCancellation.IsCancellationRequested)
                { }
                catch (Exception exception)
                {
                    // A failed timer must not skip the cleanup below: in-flight requests would
                    // keep running, registry membership would leak, and the writer would never
                    // complete. The session ends through its normal path and the cause is logged.
                    WebSocketLog.SessionExpiryFailed(_logger, Id, exception);
                }

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
                // Released only after the writer finished, so the timeout also bounds a writer
                // still draining toward a peer that stopped reading.
                closeTimeout?.Dispose();
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
                    CurrentClose.Status,
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
        Guid eventId,
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

            if (Volatile.Read(ref _closing))
            {
                // A closing session delivers nothing more. Buffering for a subscription whose
                // snapshot was cancelled would also hold outbound capacity until teardown.
                WebSocketDiagnostics.EventDropped(topic, "subscription_stopped");
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
                // The producer, not this client, chose the size, so dropping the frame is the
                // whole remedy. Reporting it as a failure would let one oversized event abort
                // every subscriber on the topic.
                WebSocketDiagnostics.EventDropped(topic, "message_too_large");
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

                        // A close that started after the check above refuses the write by design;
                        // reporting it would make the publisher abort the close handshake.
                        if (!Volatile.Read(ref _closing))
                        {
                            accepted = false;
                        }
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

    private bool IsAborted =>
        Volatile.Read(ref _aborted) != 0 || _socket.State is WebSocketState.Aborted;

    private CloseRequest CurrentClose => Volatile.Read(ref _close) ?? CloseRequest.Completed;

    /// <summary>
    /// Aborts after the outbound queue refused a frame. Once the session is closing the queue
    /// refuses frames by design, and aborting then would cut the close handshake short.
    /// </summary>
    private void AbortUnlessClosing()
    {
        if (!Volatile.Read(ref _closing))
        {
            Abort();
        }
    }

    private void AbortUnansweredClose()
    {
        // The handshake can finish just as the timer fires; a socket that already closed or
        // aborted needs nothing more.
        if (_socket.State is WebSocketState.Closed or WebSocketState.Aborted)
        {
            return;
        }

        WebSocketLog.CloseTimedOut(
            _logger,
            Id,
            _configuration.Options.CloseTimeout.TotalMilliseconds
        );
        Abort();
    }

    private async ValueTask ProcessAsync(ReadOnlyMemory<byte> payload)
    {
        HubFrame frame;
        try
        {
            frame = _protocol.Decode(payload.Span);
        }
        catch (InvalidDataException)
        {
            RequestDisconnect(
                WebSocketCloseStatus.InvalidPayloadData,
                "The application frame could not be decoded."
            );
            return;
        }

        await HandleAsync(frame).ConfigureAwait(false);
    }

    /// <summary>
    /// False for the failures the receive loop already handles: the host's cancellation, the
    /// aftermath of an abort, and transport errors.
    /// </summary>
    private bool IsUnexpected(Exception exception, CancellationToken cancellationToken) =>
        exception switch
        {
            OperationCanceledException when cancellationToken.IsCancellationRequested => false,
            OperationCanceledException or ObjectDisposedException when IsAborted => false,
            WebSocketException => false,
            _ => true,
        };

    private void Fail(Exception exception)
    {
        WebSocketLog.SessionFailed(_logger, Id, exception);
        RequestDisconnect(WebSocketCloseStatus.InternalServerError, "internal_error");
    }

    private async ValueTask HandleAsync(HubFrame frame)
    {
        // Requests have their own budget, checked before any scope, authorization, or payload
        // work so an unregistered operation costs the same as a registered one. Every other kind
        // shares the control budget, including kinds a client may not send: the invalid_frame
        // reply is still work this session performs on demand.
        var budget = frame.Kind is HubFrameKind.Request ? _requestFrames : _controlFrames;
        if (!budget.TryAcquire())
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
            case HubFrameKind.Ping:
                Pong(frame);
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

    private void Pong(HubFrame frame)
    {
        if (frame.StreamId == Guid.Empty)
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    Guid.Empty,
                    HubFaultCodes.InvalidFrame,
                    "A stream identifier other than the session identifier is required."
                )
            );
            return;
        }

        // A ping carries no state: the reply echoes only the client's correlation id, so a ping
        // never reserves a stream, a request slot, or a subscription. It shares the control-frame
        // rate window with the other client control kinds.
        _ = TryQueue(new HubFrame { Kind = HubFrameKind.Pong, StreamId = frame.StreamId });
    }

    private void StartRequest(HubFrame frame)
    {
        if (frame.StreamId == Guid.Empty)
        {
            _ = TryQueue(
                WebSocketRequestRouter.Fault(
                    Guid.Empty,
                    HubFaultCodes.InvalidFrame,
                    "A stream identifier other than the session identifier is required."
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
            if (_stop.IsCancellationRequested)
            {
                return;
            }

            var encoded = _protocol.Encode(response);
            var maximumMessageSize = _configuration.Options.MaximumMessageSize;
            if (encoded.Length > maximumMessageSize)
            {
                // The handler, not this client, produced the size, so the stream ends with a
                // fault the client can classify and the connection stays open. Only a registered
                // operation name is logged; caller-supplied text never becomes a log property.
                WebSocketLog.ResponseTooLarge(
                    _logger,
                    Id,
                    frame.Operation is { } operation
                    && _configuration.TryGetRequest(operation, out _)
                        ? operation
                        : null,
                    encoded.Length,
                    maximumMessageSize
                );
                response = WebSocketRequestRouter.Fault(
                    frame.StreamId,
                    HubFaultCodes.MessageTooLarge,
                    "The response exceeded the maximum message size."
                );
                encoded = _protocol.Encode(response);
            }

            if (!TryQueue(response, encoded))
            {
                AbortUnlessClosing();
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
            frame.StreamId == Guid.Empty
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

        if (topic.Keyed && !topic.AllowTopicWideSubscription && string.IsNullOrEmpty(frame.Key))
        {
            // A keyless subscriber to a keyed topic would receive every key's events, so unless
            // the registration opted in, a missing key is denied before the policy runs: a policy
            // that only checks scope would otherwise approve a cross-key wildcard.
            DenySubscription(
                frame.StreamId,
                topic.Name,
                "key_required",
                HubFaultCodes.Forbidden,
                "A subscription key is required for this topic."
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
            state.Stop(_outbound.Release);
            state.InitializationFinished();
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
            state.Stop(_outbound.Release);
            state.InitializationFinished();
            AbortUnlessClosing();
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
        // The timeout bounds the whole initialization, including every wait for credit. Without
        // it a client that never adds credit would hold the provider's enumerator and its DI scope
        // open for the life of the session.
        var timeout = _configuration.Options.SnapshotInitializationTimeout;
        using var stalled = new CancellationTokenSource(timeout, _timeProvider);
        using var initialization = CancellationTokenSource.CreateLinkedTokenSource(
            state.SnapshotCancellationToken,
            stalled.Token
        );
        var cancellationToken = initialization.Token;
        try
        {
            var context = new WebSocketTopicSnapshotContext(topic.Name, state.Key, _user);
            await foreach (
                var item in _router
                    .GetTopicSnapshotAsync(topic, context, cancellationToken)
                    .WithCancellation(cancellationToken)
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

                var frame = new HubFrame
                {
                    Kind = HubFrameKind.Event,
                    StreamId = state.StreamId,
                    Topic = topic.Name,
                    Key = item.Key,
                    EventId = Guid.NewGuid(),
                    Sequence = 0,
                    Payload = item.Payload,
                };
                var encoded = _protocol.Encode(frame);
                var maximumMessageSize = _configuration.Options.MaximumMessageSize;
                if (encoded.Length > maximumMessageSize)
                {
                    // The provider produced a value this session can never deliver. That is a
                    // failure of this one stream, handled below like any other provider fault,
                    // rather than a reason to abort the connection.
                    WebSocketDiagnostics.EventDropped(topic.Name, "message_too_large");
                    throw new InvalidDataException(
                        $"A WebSocket topic snapshot value encoded to {encoded.Length} bytes, above the {maximumMessageSize} byte limit."
                    );
                }

                await state.WaitForCreditAsync(cancellationToken).ConfigureAwait(false);
                var write = state.WriteSnapshot(() => TryQueue(frame, encoded));
                if (write is SnapshotWriteDisposition.Failed)
                {
                    // Size was checked above, so a failed write means the outbound budget was
                    // exhausted (the slow-client path already aborted) or the session is closing.
                    AbortUnlessClosing();
                    return;
                }

                if (write is SnapshotWriteDisposition.Stopped)
                {
                    return;
                }
            }

            if (!state.CompleteInitialization(_outbound.TryWriteReserved, _outbound.Release))
            {
                AbortUnlessClosing();
            }
        }
        catch (OperationCanceledException)
            when (state.SnapshotCancellationToken.IsCancellationRequested) { }
        catch (Exception) when (state.SnapshotCancellationToken.IsCancellationRequested) { }
        catch (Exception) when (stalled.IsCancellationRequested)
        {
            WebSocketLog.SnapshotStalled(_logger, topic.Name, Id, timeout.TotalMilliseconds);
            RemoveFailedSubscription(
                state,
                HubFaultCodes.SnapshotStalled,
                "The topic snapshot did not finish within the initialization timeout."
            );
        }
        catch (Exception exception)
        {
            WebSocketLog.SnapshotFailed(_logger, topic.Name, Id, exception);
            RemoveFailedSubscription(
                state,
                HubFaultCodes.SnapshotFailed,
                "The topic snapshot could not be loaded."
            );
        }
        finally
        {
            state.InitializationFinished();
            _subscriptionTasks.TryRemove(state.StreamId, out _);
        }
    }

    /// <summary>
    /// Ends a subscription whose initialization failed: membership and state go first so no live
    /// event can follow the terminal fault on the stream.
    /// </summary>
    private void RemoveFailedSubscription(SubscriptionState state, string code, string message)
    {
        if (
            !_subscriptions.TryRemove(
                new KeyValuePair<Guid, SubscriptionState>(state.StreamId, state)
            )
        )
        {
            return;
        }

        _registry.Unsubscribe(this, state.Topic, state.Key);
        state.Stop(_outbound.Release);
        WebSocketDiagnostics.SubscriptionRemoved(state.Topic);
        if (!TryQueue(WebSocketRequestRouter.Fault(state.StreamId, code, message)))
        {
            AbortUnlessClosing();
        }
    }

    private void AddCredit(HubFrame frame)
    {
        if (
            _subscriptions.TryGetValue(frame.StreamId, out var subscription)
            && frame.Credit is { } credit
            && subscription.TryAddCredit(
                credit,
                _configuration.Options.MaximumCreditPerSubscription
            )
        )
        {
            return;
        }

        FaultSubscription(frame.StreamId, "Credit is invalid for this subscription.");
    }

    private void Acknowledge(HubFrame frame)
    {
        if (
            _subscriptions.TryGetValue(frame.StreamId, out var subscription)
            && frame.Sequence is { } sequence
            && sequence > 0
        )
        {
            subscription.Acknowledge(sequence);
            return;
        }

        FaultSubscription(frame.StreamId, "The acknowledgement is invalid.");
    }

    /// <summary>
    /// A fault after <c>subscribed</c> is terminal for the peer, so the subscription is removed
    /// before the fault is queued and no <c>complete</c> follows. Leaving it live would keep
    /// delivering events on a stream the client has already discarded.
    /// </summary>
    private void FaultSubscription(Guid streamId, string message)
    {
        _ = TryRemoveSubscription(streamId);
        _ = TryQueue(WebSocketRequestRouter.Fault(streamId, HubFaultCodes.InvalidFrame, message));
    }

    private bool TryRemoveSubscription(Guid streamId)
    {
        if (!_subscriptions.TryRemove(streamId, out var subscription))
        {
            return false;
        }

        _registry.Unsubscribe(this, subscription.Topic, subscription.Key);
        subscription.Stop(_outbound.Release);
        WebSocketDiagnostics.SubscriptionRemoved(subscription.Topic);
        return true;
    }

    private void Unsubscribe(Guid streamId, bool sendComplete)
    {
        if (!TryRemoveSubscription(streamId))
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

        if (sendComplete)
        {
            _ = TryQueue(new HubFrame { Kind = HubFrameKind.Complete, StreamId = streamId });
        }
    }

    private void DenySubscription(
        Guid streamId,
        string? topic,
        string reason,
        string faultCode,
        string faultMessage
    )
    {
        WebSocketLog.SubscriptionDenied(_logger, Id, topic, reason);
        _ = TryQueue(WebSocketRequestRouter.Fault(streamId, faultCode, faultMessage));
    }

    private bool TryQueue(HubFrame frame) => TryQueue(frame, _protocol.Encode(frame));

    private bool TryQueue(HubFrame frame, byte[] encoded)
    {
        if (frame.Kind is HubFrameKind.Fault && frame.Code is { } code)
        {
            WebSocketDiagnostics.FaultGenerated(code);
        }

        if (!TryReserve(frame, encoded, out var reserved))
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

    private bool TryReserve(HubFrame frame, byte[] payload, out OutboundFrame reserved)
    {
        if (payload.Length > _configuration.Options.MaximumMessageSize)
        {
            // Size is a property of the frame, not of this client's pace, so an oversized frame
            // is refused without the slow-client abort below.
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
                var close = CurrentClose;
                await _socket
                    .CloseOutputAsync(close.Status, close.Reason, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested) { }
        catch (Exception exception)
            when ((exception is OperationCanceledException or ObjectDisposedException) && IsAborted)
        {
            // A send pending when the socket was aborted fails instead of completing; the abort
            // is the outcome, not a fault in the writer.
        }
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
                        InboundKind.Close,
                        ReadOnlyMemory<byte>.Empty,
                        result.MessageType,
                        result.CloseStatus
                    );
                }

                // Violations are returned rather than thrown so the loop can start a close and
                // keep reading until the peer answers it.
                messageType ??= result.MessageType;
                if (messageType != result.MessageType)
                {
                    return new InboundMessage(
                        InboundKind.InvalidFragment,
                        ReadOnlyMemory<byte>.Empty,
                        result.MessageType,
                        null
                    );
                }

                if (writer.WrittenCount + result.Count > _configuration.Options.MaximumMessageSize)
                {
                    return new InboundMessage(
                        InboundKind.TooLarge,
                        ReadOnlyMemory<byte>.Empty,
                        result.MessageType,
                        null
                    );
                }

                writer.Write(buffer.AsSpan(0, result.Count));
                if (result.EndOfMessage)
                {
                    return new InboundMessage(
                        InboundKind.Message,
                        writer.WrittenMemory.ToArray(),
                        result.MessageType,
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

    /// <summary>
    /// Records the close status and reason; the first request wins. Status and reason are
    /// published together, so a writer that sees a close in progress never reads half of one.
    /// </summary>
    private void RequestClose(WebSocketCloseStatus status, string reason) =>
        _ = Interlocked.CompareExchange(ref _close, new CloseRequest(status, reason), null);

    private async Task ExpireAsync(CancellationToken cancellationToken)
    {
        if (_expiresAt == DateTimeOffset.MaxValue)
        {
            // No expiry applies; the only way out is the session ending, which cancels this wait.
            await Task.Delay(Timeout.InfiniteTimeSpan, _timeProvider, cancellationToken)
                .ConfigureAwait(false);
            return;
        }

        while (true)
        {
            var remaining = _expiresAt - _timeProvider.GetUtcNow();
            if (remaining <= TimeSpan.Zero)
            {
                break;
            }

            // Task.Delay rejects anything above about 49.7 days, so a longer lifetime waits in
            // chunks and re-reads the clock between them instead of faulting the timer.
            await Task.Delay(
                    remaining > TimerLimits.MaximumDelay ? TimerLimits.MaximumDelay : remaining,
                    _timeProvider,
                    cancellationToken
                )
                .ConfigureAwait(false);
        }

        RequestDisconnect(WebSocketCloseStatus.PolicyViolation, "session_expired");
    }

    private string GetDiagnosticCloseReason()
    {
        if (Volatile.Read(ref _aborted) != 0)
        {
            return "aborted";
        }

        var close = CurrentClose;
        return close.Reason switch
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
            _ when close.Status is WebSocketCloseStatus.InternalServerError => "internal_error",
            _ when close.Status is WebSocketCloseStatus.PolicyViolation => "policy_violation",
            _ when close.Status is WebSocketCloseStatus.EndpointUnavailable =>
                "endpoint_unavailable",
            _ => "other",
        };
    }

    private enum InboundKind
    {
        Message,
        Close,
        TooLarge,
        InvalidFragment,
    }

    private readonly record struct InboundMessage(
        InboundKind Kind,
        ReadOnlyMemory<byte> Payload,
        WebSocketMessageType MessageType,
        WebSocketCloseStatus? CloseStatus
    );

    private sealed record CloseRequest(WebSocketCloseStatus Status, string Reason)
    {
        public static readonly CloseRequest Completed = new(
            WebSocketCloseStatus.NormalClosure,
            "Session completed."
        );
    }
}
