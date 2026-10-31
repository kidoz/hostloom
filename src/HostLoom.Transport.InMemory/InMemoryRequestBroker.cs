using System.Collections.Concurrent;
using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace HostLoom.Transport.InMemory;

/// <summary>
/// Process-local transport for tests and single-process applications: a request runs its
/// listener's handler directly, and a publication runs every matching subscription's handler.
/// </summary>
/// <remarks>
/// <para>
/// A request to an address with no listener waits out its timeout and throws
/// <see cref="RequestTimeoutException"/>, as it would against a broker with no consumer. So does
/// a request whose listener stops while its handler is running: the handler's token is
/// cancelled, no reply will come, and the requester waits for its own timeout rather than
/// seeing the listener's cancellation. A stream with no listener times out the same way. A
/// stream whose listener stops, or a stream still open when the transport is disposed, ends
/// with <see cref="ObjectDisposedException"/> because the caller is already reading items and
/// there is no later reply to time out. An exception the listener's frame handler throws, which
/// the HostLoom dispatcher does only for a malformed frame, reaches the requester unchanged.
/// A stream handler's exception reaches the reader unchanged: the items never crossed a
/// process boundary, so there is no fault envelope to sanitize. Disposing the transport ends
/// every waiting request with <see cref="ObjectDisposedException"/>.
/// </para>
/// <para>
/// Stopping a listener or subscription, and disposing the transport, cancel the handlers still
/// running and wait for them to return, for up to <see cref="HandlerDrainBound"/> on the
/// transport's clock, so a handler does not outlive the services it was resolved from. A handler
/// that ignores its cancellation longer than that is logged and left running.
/// </para>
/// <para>
/// The health probe answers from local state only: <see cref="IsReachable"/> and whether the
/// transport has been disposed.
/// </para>
/// </remarks>
public sealed class InMemoryRequestBroker
    : IRequestBroker,
        IStreamRequestBroker,
        IEventBroker,
        IBrokerHealthProbe
{
    /// <summary>How long stopping waits for the handlers it cancelled before it gives up on them.</summary>
    internal static readonly TimeSpan HandlerDrainBound = TimeSpan.FromSeconds(5);
    private readonly int _maxFrameBytes;

    private readonly ConcurrentDictionary<RequestAddress, RequestSubscription> _handlers = new();
    private readonly ConcurrentDictionary<RequestAddress, StreamSubscription> _streams = new();
    private readonly ConcurrentDictionary<TaskCompletionSource, byte> _streamWaits = new(
        ReferenceEqualityComparer.Instance
    );
    private readonly ConcurrentDictionary<
        TaskCompletionSource<ReadOnlyMemory<byte>>,
        byte
    > _waiting = new(ReferenceEqualityComparer.Instance);
    private readonly ConcurrentDictionary<
        (RequestAddress Topic, string Name),
        EventSubscription
    > _topics = new();

    // Every subscription until its stop has finished, including one that has already left the
    // routing tables above, so disposing the transport also waits for the handlers it is draining.
    private readonly ConcurrentDictionary<Subscription, byte> _open = new(
        ReferenceEqualityComparer.Instance
    );
    private readonly ILogger<InMemoryRequestBroker> _logger;
    private readonly TimeProvider _clock;
    private volatile bool _disposed;
    private readonly Lock _lifecycleGate = new();

    public InMemoryRequestBroker()
        : this(null) { }

    public InMemoryRequestBroker(ILogger<InMemoryRequestBroker>? logger)
        : this(logger, timeProvider: null) { }

    /// <summary>
    /// Takes the clock that bounds requests, so a test registering a controllable
    /// <see cref="TimeProvider"/> drives the request timeout instead of waiting it out.
    /// </summary>
    public InMemoryRequestBroker(
        ILogger<InMemoryRequestBroker>? logger,
        TimeProvider? timeProvider = null
    )
        : this(logger, timeProvider, messagingOptions: null) { }

    /// <summary>Composes the transport with the messaging frame limit.</summary>
    public InMemoryRequestBroker(
        ILogger<InMemoryRequestBroker>? logger = null,
        TimeProvider? timeProvider = null,
        IOptions<HostLoomOptions>? messagingOptions = null
    )
    {
        _maxFrameBytes =
            messagingOptions?.Value.MaxFrameBytes ?? HostLoomOptions.DefaultMaxFrameBytes;
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(_maxFrameBytes);
        _logger = logger ?? NullLogger<InMemoryRequestBroker>.Instance;
        _clock = timeProvider ?? TimeProvider.System;
    }

    /// <summary>Simulates an unreachable broker, so readiness behaviour is testable in process.</summary>
    public bool IsReachable { get; set; } = true;

    public ValueTask<BrokerHealth> CheckHealthAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(
            IsReachable && !_disposed
                ? BrokerHealth.Healthy("In-memory transport is reachable in process.")
                : BrokerHealth.Unhealthy("In-memory transport is unavailable.")
        );
    }

    public async ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(handler);
        var subscription = new RequestSubscription(this, address, handler);
        bool added;
        lock (_lifecycleGate)
            added = !_disposed && _handlers.TryAdd(address, subscription);
        if (!added)
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            ObjectDisposedException.ThrowIf(_disposed, this);
            throw new InvalidOperationException(
                $"The in-memory endpoint '{address}' already has a listener."
            );
        }
        return subscription;
    }

    /// <inheritdoc />
    public async ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        StreamRequestHandler handler,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(handler);
        var subscription = new StreamSubscription(this, address, handler);
        bool added;
        lock (_lifecycleGate)
            added = !_disposed && _streams.TryAdd(address, subscription);
        if (!added)
        {
            await subscription.DisposeAsync().ConfigureAwait(false);
            ObjectDisposedException.ThrowIf(_disposed, this);
            throw new InvalidOperationException(
                $"The in-memory endpoint '{address}' already has a stream listener."
            );
        }

        return subscription;
    }

    /// <inheritdoc />
    public IAsyncEnumerable<object?> StreamAsync(
        RequestAddress address,
        object request,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        ArgumentNullException.ThrowIfNull(request);
        if (timeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout),
                "The request timeout must be positive."
            );
        }

        return Read(cancellationToken);

        // The compiler links this argument with the token passed to GetAsyncEnumerator.
        async IAsyncEnumerable<object?> Read(
            [EnumeratorCancellation] CancellationToken enumeratorToken = default
        )
        {
            StreamSubscription? subscription;
            bool entered;
            lock (_lifecycleGate)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                entered =
                    _streams.TryGetValue(address, out subscription) && subscription!.TryEnter();
            }

            if (!entered)
            {
                await ThrowWhenUnboundAsync(address, timeout, enumeratorToken)
                    .ConfigureAwait(false);
                yield break;
            }

            // The listener's token is not the caller's. Linking them makes a stop cancel the
            // handler, and the catch below tells a caller cancellation from a listener stop.
            using var delivery = CancellationTokenSource.CreateLinkedTokenSource(
                enumeratorToken,
                subscription!.Token
            );
            IAsyncEnumerator<object?>? enumerator = null;
            try
            {
                var sequence = subscription.Handler(request, delivery.Token);
                if (sequence is null)
                {
                    throw new InvalidOperationException(
                        $"The stream listener on '{address}' returned no sequence."
                    );
                }

                enumerator = sequence.GetAsyncEnumerator(delivery.Token);
                while (true)
                {
                    bool more;
                    try
                    {
                        more = await enumerator.MoveNextAsync().ConfigureAwait(false);
                    }
                    catch (OperationCanceledException) when (delivery.Token.IsCancellationRequested)
                    {
                        if (enumeratorToken.IsCancellationRequested)
                        {
                            throw;
                        }

                        throw new ObjectDisposedException(nameof(InMemoryRequestBroker));
                    }

                    if (!more)
                    {
                        yield break;
                    }

                    yield return enumerator.Current;
                }
            }
            finally
            {
                if (enumerator is not null)
                {
                    await enumerator.DisposeAsync().ConfigureAwait(false);
                }

                subscription!.Exit();
            }
        }
    }

    /// <summary>
    /// Waits out <paramref name="timeout"/> on the transport clock. Nothing completes the wait,
    /// so it ends as a timeout, as a cancellation, or as disposal — the same three endings an
    /// unbound request has.
    /// </summary>
    private async Task ThrowWhenUnboundAsync(
        RequestAddress address,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        lock (_lifecycleGate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _streamWaits.TryAdd(gate, 0);
        }

        try
        {
            await gate.Task.WaitAsync(timeout, _clock, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new RequestTimeoutException(address, timeout, exception);
        }
        finally
        {
            _streamWaits.TryRemove(gate, out _);
        }
    }

    public async ValueTask<IAsyncDisposable> SubscribeAsync(
        RequestAddress topic,
        string subscription,
        EventFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        ArgumentNullException.ThrowIfNull(handler);
        cancellationToken.ThrowIfCancellationRequested();
        var key = (topic, subscription);
        var handle = new EventSubscription(this, key, handler);
        bool added;
        lock (_lifecycleGate)
            added = !_disposed && _topics.TryAdd(key, handle);
        if (!added)
        {
            await handle.DisposeAsync().ConfigureAwait(false);
            ObjectDisposedException.ThrowIf(_disposed, this);
            throw new InvalidOperationException(
                $"Topic '{topic}' already has a subscription named '{subscription}'."
            );
        }
        return handle;
    }

    /// <summary>
    /// Awaits local delivery for deterministic tests. Subscriber failures are logged at the
    /// receiver and never reported as publication failures to the publisher or outbox relay.
    /// </summary>
    public async ValueTask PublishAsync(
        RequestAddress topic,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateFrameSize(frame.Length);
        var owned = frame.ToArray();
        var deliveries = _topics
            .Where(pair => pair.Key.Topic == topic)
            .Select(pair => DeliverEventAsync(pair.Value, owned))
            .ToArray();
        await Task.WhenAll(deliveries).WaitAsync(cancellationToken).ConfigureAwait(false);
    }

    private async Task DeliverEventAsync(EventSubscription subscription, byte[] frame)
    {
        // A subscription that has begun to stop takes no new delivery, as if already detached.
        if (!subscription.TryEnter())
        {
            return;
        }

        try
        {
            await Task.Run(async () =>
                    await subscription.Handler(frame, subscription.Token).ConfigureAwait(false)
                )
                .ConfigureAwait(false);
        }
        catch (OperationCanceledException) when (subscription.Token.IsCancellationRequested) { }
        catch (ReceiveSuspendedException)
        {
            // The subscription is paused. The publisher, including an outbox relay, must see it
            // and leave the message pending. Other subscriber failures stay local.
            throw;
        }
        catch (Exception exception)
        {
            _logger.LogError(
                new EventId(1402, "InMemoryEventFailed"),
                exception,
                "In-memory event subscription {Subscription} failed; the publication remains accepted.",
                subscription.Key.Name
            );
        }
        finally
        {
            subscription.Exit();
        }
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        RequestAddress address,
        ReadOnlyMemory<byte> request,
        Guid requestId,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        ObjectDisposedException.ThrowIf(_disposed, this);
        cancellationToken.ThrowIfCancellationRequested();
        ValidateFrameSize(request.Length);
        var reply = new TaskCompletionSource<ReadOnlyMemory<byte>>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        // Registered under the gate disposal takes, so disposal either sees this wait and ends
        // it or has already made this call throw.
        lock (_lifecycleGate)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            _waiting.TryAdd(reply, 0);
        }

        try
        {
            // Without a listener nothing ever completes the reply, so the wait below runs out
            // the whole budget on the clock, the same timeout a broker gives an unbound address.
            // A listener that has begun to stop counts as none.
            if (_handlers.TryGetValue(address, out var subscription) && subscription.TryEnter())
            {
                _ = DeliverRequestAsync(subscription, request.ToArray(), reply);
            }

            return await reply
                .Task.WaitAsync(timeout, _clock, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new RequestTimeoutException(address, timeout, exception);
        }
        finally
        {
            _waiting.TryRemove(reply, out _);
        }
    }

    /// <summary>
    /// Runs the listener's handler and completes <paramref name="reply"/> with its answer or its
    /// failure. A handler cancelled because its listener stopped produces no reply, so the
    /// requester keeps waiting for its own timeout, as it would for a broker whose consumer went
    /// away mid-request, instead of receiving a cancellation that belongs to the listener.
    /// </summary>
    private async Task DeliverRequestAsync(
        RequestSubscription subscription,
        byte[] request,
        TaskCompletionSource<ReadOnlyMemory<byte>> reply
    )
    {
        try
        {
            var response = await RunRequestAsync(subscription, request, reply).ConfigureAwait(false);
            if (response is null)
            {
                return;
            }

            ValidateFrameSize(response.Value.Length);
            reply.TrySetResult(response.Value);
        }
        catch (OperationCanceledException) { }
        catch (Exception exception)
        {
            // Observed here as well, because a requester that stopped waiting never reads it.
            if (reply.TrySetException(exception))
            {
                _ = reply.Task.Exception;
            }
        }
        finally
        {
            subscription.Exit();
        }
    }

    /// <summary>
    /// Runs the listener, waiting out a pause without starting the handler once the caller has
    /// stopped waiting. An expired request is dropped the same way: no reply, so the caller
    /// reaches its own timeout.
    /// </summary>
    private async Task<ReadOnlyMemory<byte>?> RunRequestAsync(
        RequestSubscription subscription,
        byte[] request,
        TaskCompletionSource<ReadOnlyMemory<byte>> reply
    )
    {
        while (true)
        {
            try
            {
                return await subscription.Handler(request, subscription.Token).ConfigureAwait(false);
            }
            catch (RequestExpiredException)
            {
                return null;
            }
            catch (ReceiveSuspendedException suspended)
            {
                var delay = suspended.ResumeAt - _clock.GetUtcNow();
                if (delay > TimeSpan.Zero)
                {
                    try
                    {
                        await Task.Delay(delay, _clock, subscription.Token).ConfigureAwait(false);
                    }
                    catch (OperationCanceledException)
                    {
                        return null;
                    }
                }

                if (!_waiting.ContainsKey(reply) || subscription.Token.IsCancellationRequested)
                {
                    return null;
                }
            }
        }
    }

    private void ValidateFrameSize(int length)
    {
        if (length > _maxFrameBytes)
            throw new MalformedEnvelopeException(
                $"The broker frame exceeds HostLoomOptions.MaxFrameBytes ({_maxFrameBytes} bytes)."
            );
    }

    /// <summary>
    /// Ends every waiting request, then stops every listener and subscription, those already
    /// stopping included, and waits for their handlers as a stop does.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        lock (_lifecycleGate)
            _disposed = true;
        foreach (var reply in _waiting.Keys)
        {
            if (reply.TrySetException(new ObjectDisposedException(nameof(InMemoryRequestBroker))))
            {
                _ = reply.Task.Exception;
            }
        }

        foreach (var wait in _streamWaits.Keys)
        {
            if (wait.TrySetException(new ObjectDisposedException(nameof(InMemoryRequestBroker))))
            {
                _ = wait.Task.Exception;
            }
        }
        // Concurrently, so the handlers of every subscription share one drain bound.
        await Task.WhenAll(_open.Keys.Select(static subscription => subscription.StopAsync()))
            .ConfigureAwait(false);
    }

    /// <summary>
    /// Waits for <paramref name="drained"/>, the handlers <paramref name="subscription"/>
    /// cancelled, for up to <see cref="HandlerDrainBound"/>.
    /// </summary>
    private async Task DrainAsync(Subscription subscription, Task drained, int running)
    {
        try
        {
            await drained.WaitAsync(HandlerDrainBound, _clock).ConfigureAwait(false);
        }
        catch (TimeoutException)
        {
            _logger.LogWarning(
                new EventId(1404, "InMemoryHandlersAbandoned"),
                "The in-memory {Subscription} stopped without its handlers: of the {Running} it cancelled, some were still running after {Bound} and may outlive the transport.",
                subscription.Name,
                running,
                HandlerDrainBound
            );
        }
    }

    private abstract class Subscription : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stopping = new();
        private readonly Lock _gate = new();
        private Task? _stopped;
        private int _running;
        private TaskCompletionSource? _drained;

        protected Subscription(InMemoryRequestBroker owner)
        {
            Owner = owner;
            Token = _stopping.Token;
            owner._open.TryAdd(this, 0);
        }

        public CancellationToken Token { get; }

        protected InMemoryRequestBroker Owner { get; }

        /// <summary>What a log line calls this subscription.</summary>
        public abstract string Name { get; }

        protected abstract void Remove();

        /// <summary>
        /// Admits one delivery, which must call <see cref="Exit"/> when its handler returns.
        /// Refused once the subscription has begun to stop, so no handler starts after the
        /// stop's wait began.
        /// </summary>
        public bool TryEnter()
        {
            lock (_gate)
            {
                if (_stopped is not null)
                    return false;
                _running++;
                return true;
            }
        }

        public void Exit()
        {
            lock (_gate)
            {
                if (--_running == 0)
                    _drained?.TrySetResult();
            }
        }

        public ValueTask DisposeAsync() => new(StopAsync());

        /// <summary>The one stop, shared by every disposal and by the transport's.</summary>
        public Task StopAsync()
        {
            TaskCompletionSource stopped;
            var drained = Task.CompletedTask;
            int running;
            lock (_gate)
            {
                if (_stopped is not null)
                    return _stopped;
                stopped = new TaskCompletionSource(
                    TaskCreationOptions.RunContinuationsAsynchronously
                );
                _stopped = stopped.Task;
                running = _running;
                if (running > 0)
                {
                    _drained = new TaskCompletionSource(
                        TaskCreationOptions.RunContinuationsAsynchronously
                    );
                    drained = _drained.Task;
                }
            }

            // Run outside the gate, so a handler's Exit is never blocked behind the cancellation.
            _ = RunStopAsync(stopped, drained, running);
            return stopped.Task;
        }

        private async Task RunStopAsync(TaskCompletionSource stopped, Task drained, int running)
        {
            try
            {
                Remove();
                try
                {
                    await _stopping.CancelAsync().ConfigureAwait(false);
                }
                finally
                {
                    if (!drained.IsCompleted)
                        await Owner.DrainAsync(this, drained, running).ConfigureAwait(false);
                    _stopping.Dispose();
                    Owner._open.TryRemove(this, out _);
                }

                stopped.TrySetResult();
            }
            catch (Exception exception)
            {
                stopped.TrySetException(exception);
            }
        }
    }

    private sealed class RequestSubscription(
        InMemoryRequestBroker owner,
        RequestAddress address,
        RequestFrameHandler handler
    ) : Subscription(owner)
    {
        public RequestFrameHandler Handler { get; } = handler;

        public override string Name => $"listener on '{address}'";

        protected override void Remove() => Owner._handlers.TryRemove(new(address, this));
    }

    private sealed class StreamSubscription(
        InMemoryRequestBroker owner,
        RequestAddress address,
        StreamRequestHandler handler
    ) : Subscription(owner)
    {
        public StreamRequestHandler Handler { get; } = handler;

        public override string Name => $"stream listener on '{address}'";

        protected override void Remove() => Owner._streams.TryRemove(new(address, this));
    }

    private sealed class EventSubscription(
        InMemoryRequestBroker owner,
        (RequestAddress Topic, string Name) key,
        EventFrameHandler handler
    ) : Subscription(owner)
    {
        public (RequestAddress Topic, string Name) Key { get; } = key;
        public EventFrameHandler Handler { get; } = handler;

        public override string Name => $"subscription '{Key.Name}' on '{Key.Topic}'";

        protected override void Remove() => Owner._topics.TryRemove(new(Key, this));
    }
}
