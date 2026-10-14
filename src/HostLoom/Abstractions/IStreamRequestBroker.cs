namespace HostLoom;

/// <summary>
/// Handles one accepted stream: the request object in, the handler's items out. The token is
/// cancelled when the listener stops or the caller stops reading.
/// </summary>
public delegate IAsyncEnumerable<object?> StreamRequestHandler(
    object request,
    CancellationToken cancellationToken
);

/// <summary>
/// In-process stream delivery. A transport implements this when a caller and a handler in the
/// same process can exchange a sequence without a reply envelope. Broker transports do not.
/// </summary>
/// <remarks>
/// The request passed to <see cref="StreamAsync"/> is the caller's instance, not an encoded
/// body. Items are whatever the handler yields. Nothing on this interface is a wire contract.
/// </remarks>
public interface IStreamRequestBroker
{
    /// <summary>
    /// Binds <paramref name="handler"/> to <paramref name="address"/> until the returned handle
    /// is disposed.
    /// </summary>
    ValueTask<IAsyncDisposable> ListenAsync(
        RequestAddress address,
        StreamRequestHandler handler,
        CancellationToken cancellationToken
    );

    /// <summary>
    /// Runs the listener at <paramref name="address"/> and yields its items.
    /// </summary>
    /// <param name="address">The endpoint that accepted the stream listener.</param>
    /// <param name="request">The caller's request instance. It is not encoded.</param>
    /// <param name="timeout">
    /// Bounds the wait for a listener to accept the stream, including an address with none.
    /// It does not bound the time between items after acceptance.
    /// </param>
    /// <param name="cancellationToken">Cancels the wait and the handler's enumeration.</param>
    /// <exception cref="RequestTimeoutException">No listener accepted the stream within <paramref name="timeout"/>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <exception cref="ObjectDisposedException">The transport was disposed before or during the stream.</exception>
    IAsyncEnumerable<object?> StreamAsync(
        RequestAddress address,
        object request,
        TimeSpan timeout,
        CancellationToken cancellationToken
    );
}
