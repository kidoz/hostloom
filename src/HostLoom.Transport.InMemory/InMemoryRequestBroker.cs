using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.Transport.InMemory;

public sealed class InMemoryRequestBroker : IRequestBroker, IEventBroker, IBrokerHealthProbe
{
    private readonly ConcurrentDictionary<RequestAddress, RequestSubscription> _handlers = new();
    private readonly ConcurrentDictionary<
        (RequestAddress Topic, string Name),
        EventSubscription
    > _topics = new();
    private readonly ILogger<InMemoryRequestBroker> _logger;
    private volatile bool _disposed;
    private readonly Lock _lifecycleGate = new();

    public InMemoryRequestBroker()
        : this(null) { }

    public InMemoryRequestBroker(ILogger<InMemoryRequestBroker>? logger) =>
        _logger = logger ?? NullLogger<InMemoryRequestBroker>.Instance;

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
        if (!_handlers.TryGetValue(address, out var subscription))
        {
            await Task.Delay(timeout, cancellationToken).ConfigureAwait(false);
            throw new RequestTimeoutException(address, timeout);
        }
        var owned = request.ToArray();
        var delivery = Task.Run(async () =>
            await subscription.Handler(owned, subscription.Token).ConfigureAwait(false)
        );
        // Observe a receiver failure even when the caller has already stopped waiting.
        _ = delivery.ContinueWith(
            task => _ = task.Exception,
            CancellationToken.None,
            TaskContinuationOptions.OnlyOnFaulted | TaskContinuationOptions.ExecuteSynchronously,
            TaskScheduler.Default
        );
        try
        {
            return await delivery.WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new RequestTimeoutException(address, timeout, exception);
        }
    }

    public async ValueTask DisposeAsync()
    {
        lock (_lifecycleGate)
            _disposed = true;
        foreach (var subscription in _handlers.Values)
            await subscription.DisposeAsync().ConfigureAwait(false);
        foreach (var subscription in _topics.Values)
            await subscription.DisposeAsync().ConfigureAwait(false);
    }

    private abstract class Subscription : IAsyncDisposable
    {
        private readonly CancellationTokenSource _stopping = new();
        private int _disposed;
        public CancellationToken Token { get; }

        protected Subscription() => Token = _stopping.Token;

        protected abstract void Remove();

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposed, 1) != 0)
                return;
            Remove();
            try
            {
                await _stopping.CancelAsync().ConfigureAwait(false);
            }
            finally
            {
                _stopping.Dispose();
            }
        }
    }

    private sealed class RequestSubscription(
        InMemoryRequestBroker owner,
        RequestAddress address,
        RequestFrameHandler handler
    ) : Subscription
    {
        public RequestFrameHandler Handler { get; } = handler;

        protected override void Remove() => owner._handlers.TryRemove(new(address, this));
    }

    private sealed class EventSubscription(
        InMemoryRequestBroker owner,
        (RequestAddress Topic, string Name) key,
        EventFrameHandler handler
    ) : Subscription
    {
        public (RequestAddress Topic, string Name) Key { get; } = key;
        public EventFrameHandler Handler { get; } = handler;

        protected override void Remove() => owner._topics.TryRemove(new(Key, this));
    }
}
