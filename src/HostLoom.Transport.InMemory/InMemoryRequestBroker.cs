using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

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
/// seeing the listener's cancellation. An exception the listener's frame handler throws, which
/// the HostLoom dispatcher does only for a malformed frame, reaches the requester unchanged.
/// Disposing the transport ends every waiting request with <see cref="ObjectDisposedException"/>.
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
public sealed class InMemoryRequestBroker : IRequestBroker, IEventBroker, IBrokerHealthProbe
{
    /// <summary>How long stopping waits for the handlers it cancelled before it gives up on them.</summary>
    internal static readonly TimeSpan HandlerDrainBound = TimeSpan.FromSeconds(5);

    private readonly ConcurrentDictionary<RequestAddress, RequestSubscription> _handlers = new();
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
    {
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
    private static async Task DeliverRequestAsync(
        RequestSubscription subscription,
        byte[] request,
        TaskCompletionSource<ReadOnlyMemory<byte>> reply
    )
    {
        try
        {
            var response = await Task.Run(async () =>
                    await subscription.Handler(request, subscription.Token).ConfigureAwait(false)
                )
                .ConfigureAwait(false);
            reply.TrySetResult(response);
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
