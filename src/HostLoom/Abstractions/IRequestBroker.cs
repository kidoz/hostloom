namespace HostLoom;

public delegate ValueTask<ReadOnlyMemory<byte>> RequestFrameHandler(
    ReadOnlyMemory<byte> request,
    CancellationToken cancellationToken
);

/// <summary>
/// Minimal transport service-provider interface. Implementations own correlation, reply routing,
/// broker acknowledgements, and transport resource lifetime.
/// </summary>
/// <remarks>
/// Disposing the transport ends its listeners, and any later call, or any call still waiting,
/// throws <see cref="ObjectDisposedException"/>.
/// </remarks>
public interface IRequestBroker : IAsyncDisposable
{
    /// <summary>
    /// Binds <paramref name="handler"/> to <paramref name="address"/> until the returned handle
    /// is disposed.
    /// </summary>
    /// <remarks>
    /// The token passed to <paramref name="handler"/> is cancelled only by disposing the returned
    /// handle or by the transport itself, for example when it is disposed or the broker withdraws
    /// the delivery; never by a requester, so a caller that stops waiting leaves accepted work
    /// running. The handler may be invoked for several requests at once. Ordering, redelivery,
    /// and how listeners bound to the same address in different processes share its requests
    /// are defined by each transport.
    /// </remarks>
    ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        RequestFrameHandler handler,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Sends <paramref name="request"/> to <paramref name="address"/> and returns the reply
    /// correlated to <paramref name="requestId"/>.
    /// </summary>
    /// <remarks>
    /// <para>
    /// <paramref name="timeout"/> bounds the whole call: whatever setup the transport needs, such
    /// as connecting or starting its reply consumer, publishing the request, and waiting for the
    /// reply. When it elapses the call ends promptly with <see cref="RequestTimeoutException"/>,
    /// although the request may still be delivered and handled afterwards. An address with no
    /// listener, or one the transport cannot route to, times out the same way rather than
    /// failing sooner.
    /// </para>
    /// <para>
    /// Cancelling <paramref name="cancellationToken"/> ends only this wait, with an
    /// <see cref="OperationCanceledException"/> carrying that token. It neither recalls the
    /// request nor cancels a handler already running it.
    /// </para>
    /// </remarks>
    /// <exception cref="RequestTimeoutException">No reply arrived within <paramref name="timeout"/>, including for an unbound or unroutable address.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <exception cref="ObjectDisposedException">The transport was disposed before or during the call.</exception>
    /// <exception cref="MessagingTransportException">Any other transport failure. Whether the request was delivered is unknown.</exception>
    ValueTask<ReadOnlyMemory<byte>> RequestAsync(
        RequestAddress address,
        ReadOnlyMemory<byte> request,
        Guid requestId,
        TimeSpan timeout,
        CancellationToken cancellationToken
    );
}
