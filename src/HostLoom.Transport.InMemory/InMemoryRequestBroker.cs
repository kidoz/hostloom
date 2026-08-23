using System.Collections.Concurrent;

namespace HostLoom.Transport.InMemory;

public sealed class InMemoryRequestBroker : IRequestBroker
{
    private readonly ConcurrentDictionary<RequestAddress, RequestFrameHandler> _handlers = new();

    public ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken)
    {
        cancellationToken.ThrowIfCancellationRequested();
        if (!_handlers.TryAdd(address, handler))
        {
            throw new InvalidOperationException($"The in-memory endpoint '{address}' already has a listener.");
        }

        // CA2000: ownership of the subscription transfers to the caller, which disposes it.
#pragma warning disable CA2000
        return ValueTask.FromResult<IAsyncDisposable>(new Subscription(_handlers, address));
#pragma warning restore CA2000
    }

    public async ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        RequestAddress address,
        ReadOnlyMemory<byte> request,
        Guid requestId,
        TimeSpan timeout,
        CancellationToken cancellationToken)
    {
        if (!_handlers.TryGetValue(address, out var handler))
        {
            throw new RequestTimeoutException(address, timeout);
        }

        try
        {
            return await handler(request, cancellationToken).AsTask().WaitAsync(timeout, cancellationToken).ConfigureAwait(false);
        }
        catch (TimeoutException exception)
        {
            throw new RequestTimeoutException(address, timeout, exception);
        }
    }

    public ValueTask DisposeAsync()
    {
        _handlers.Clear();
        return ValueTask.CompletedTask;
    }

    private sealed class Subscription(
        ConcurrentDictionary<RequestAddress, RequestFrameHandler> handlers,
        RequestAddress address) : IAsyncDisposable
    {
        public ValueTask DisposeAsync()
        {
            handlers.TryRemove(address, out _);
            return ValueTask.CompletedTask;
        }
    }
}
