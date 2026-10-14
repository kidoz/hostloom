namespace HostLoom;

/// <summary>Handles one <see cref="IStreamRequest{TItem}"/> by yielding its items.</summary>
/// <typeparam name="TRequest">The stream request.</typeparam>
/// <typeparam name="TItem">One element of the sequence.</typeparam>
public interface IStreamRequestHandler<in TRequest, TItem>
    where TRequest : IStreamRequest<TItem>
{
    /// <summary>
    /// Yields the items for <paramref name="request"/>. Enumeration pulls each item, so a
    /// caller that stops reading stops this method at its next wait on
    /// <paramref name="cancellationToken"/>.
    /// </summary>
    IAsyncEnumerable<TItem> HandleAsync(TRequest request, CancellationToken cancellationToken);
}
