using System.Collections.Concurrent;

namespace HostLoom.Transport.InMemory;

public sealed class InMemoryRequestBroker : IRequestBroker, IEventBroker, IBrokerHealthProbe
{
    private readonly ConcurrentDictionary<RequestAddress, RequestFrameHandler> _handlers = new();
    private readonly ConcurrentDictionary<
        RequestAddress,
        ConcurrentDictionary<string, EventFrameHandler>
    > _topics = new();

    /// <summary>Simulates an unreachable broker, so readiness behaviour is testable in process.</summary>
    public bool IsReachable { get; set; } = true;

    public ValueTask<BrokerHealth> CheckHealthAsync(CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        return ValueTask.FromResult(
            IsReachable
                ? BrokerHealth.Healthy("In-memory transport is always reachable in process.")
                : BrokerHealth.Unhealthy("In-memory transport was marked unreachable.")
        );
    }

    public ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_handlers.TryAdd(address, handler))
        {
            throw new InvalidOperationException(
                $"The in-memory endpoint '{address}' already has a listener."
            );
        }

        // CA2000: ownership of the subscription transfers to the caller, which disposes it.
#pragma warning disable CA2000
        return ValueTask.FromResult<IAsyncDisposable>(new Subscription(_handlers, address));
#pragma warning restore CA2000
    }

    public ValueTask<IAsyncDisposable> SubscribeAsync(
        RequestAddress topic,
        string subscription,
        EventFrameHandler handler,
        CancellationToken cancellationToken
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        cancellationToken.ThrowIfCancellationRequested();

        var subscriptions = _topics.GetOrAdd(
            topic,
            _ => new ConcurrentDictionary<string, EventFrameHandler>(StringComparer.Ordinal)
        );
        if (!subscriptions.TryAdd(subscription, handler))
        {
            throw new InvalidOperationException(
                $"Topic '{topic}' already has a subscription named '{subscription}'."
            );
        }

        // CA2000: ownership of the subscription transfers to the caller, which disposes it.
#pragma warning disable CA2000
        return ValueTask.FromResult<IAsyncDisposable>(
            new EventSubscription(subscriptions, subscription)
        );
#pragma warning restore CA2000
    }

    /// <summary>
    /// Delivers to every subscription in process. Each is attempted even when an earlier one throws,
    /// because subscriptions are independent; the failures are then aggregated so a test or a local
    /// run does not swallow them. A networked broker would decouple the publisher from them entirely.
    /// </summary>
    public async ValueTask PublishAsync(
        RequestAddress topic,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken
    )
    {
        if (!_topics.TryGetValue(topic, out var subscriptions))
        {
            return;
        }

        List<Exception>? failures = null;
        foreach (var handler in subscriptions.Values)
        {
            try
            {
                await handler(frame, cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                (failures ??= []).Add(exception);
            }
        }

        if (failures is not null)
        {
            throw new AggregateException(
                $"One or more subscriptions on '{topic}' failed.",
                failures
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
        if (!_handlers.TryGetValue(address, out var handler))
        {
            throw new RequestTimeoutException(address, timeout);
        }

        try
        {
            return await handler(request, cancellationToken)
                .AsTask()
                .WaitAsync(timeout, cancellationToken)
                .ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new RequestTimeoutException(address, timeout, exception);
        }
    }

    public ValueTask DisposeAsync()
    {
        _handlers.Clear();
        _topics.Clear();
        return ValueTask.CompletedTask;
    }

    private sealed class Subscription(
        ConcurrentDictionary<RequestAddress, RequestFrameHandler> handlers,
        RequestAddress address
    ) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            handlers.TryRemove(address, out _);
            return ValueTask.CompletedTask;
        }
    }

    private sealed class EventSubscription(
        ConcurrentDictionary<string, EventFrameHandler> subscriptions,
        string name
    ) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            subscriptions.TryRemove(name, out _);
            return ValueTask.CompletedTask;
        }
    }
}
