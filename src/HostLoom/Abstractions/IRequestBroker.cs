namespace HostLoom;

public delegate ValueTask<ReadOnlyMemory<byte>> RequestFrameHandler(
    ReadOnlyMemory<byte> request,
    CancellationToken cancellationToken);

/// <summary>
/// Minimal transport service-provider interface. Implementations own correlation, reply routing,
/// broker acknowledgements, and transport resource lifetime.
/// </summary>
public interface IRequestBroker : IAsyncDisposable
{
    ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken);

    ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        RequestAddress address,
        ReadOnlyMemory<byte> request,
        Guid requestId,
        TimeSpan timeout,
        CancellationToken cancellationToken);
}
