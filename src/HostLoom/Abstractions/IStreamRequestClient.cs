namespace HostLoom;

/// <summary>Sends a stream request to an in-process endpoint and reads its items.</summary>
/// <typeparam name="TRequest">The stream request.</typeparam>
/// <typeparam name="TItem">One element of the sequence.</typeparam>
public interface IStreamRequestClient<in TRequest, TItem>
    where TRequest : IStreamRequest<TItem>
{
    /// <summary>
    /// Yields the items <paramref name="destination"/> produces for <paramref name="request"/>.
    /// </summary>
    /// <param name="destination">The endpoint that registered the handler.</param>
    /// <param name="request">The request. The handler receives this instance.</param>
    /// <param name="timeout">
    /// How long to wait for a listener to accept the stream. Defaults to
    /// <see cref="HostLoomOptions.RequestTimeout"/>. It does not bound the gaps between items
    /// once the listener has accepted.
    /// </param>
    /// <param name="cancellationToken">
    /// Cancels the wait and the handler's enumeration. Stopping the loop cancels the handler.
    /// </param>
    /// <exception cref="NotSupportedException">The configured transport does not stream.</exception>
    /// <exception cref="RequestTimeoutException">No listener accepted the stream within <paramref name="timeout"/>.</exception>
    /// <exception cref="OperationCanceledException"><paramref name="cancellationToken"/> was cancelled.</exception>
    /// <exception cref="ObjectDisposedException">The transport was disposed before or during the stream.</exception>
    IAsyncEnumerable<TItem> StreamAsync(
        RequestAddress destination,
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    );
}
