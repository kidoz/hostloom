namespace HostLoom;

internal interface IStreamExecutor
{
    IAsyncEnumerable<object?> ExecuteAsync(object request, CancellationToken cancellationToken);
}

internal sealed class StreamExecutor<TRequest, TItem>(
    IStreamRequestHandler<TRequest, TItem> handler
) : IStreamExecutor
    where TRequest : class, IStreamRequest<TItem>
{
    public async IAsyncEnumerable<object?> ExecuteAsync(
        object request,
        [System.Runtime.CompilerServices.EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        var typed = (TRequest)request;
        await foreach (
            var item in handler
                .HandleAsync(typed, cancellationToken)
                .ConfigureAwait(false)
                .WithCancellation(cancellationToken)
        )
        {
            yield return item;
        }
    }
}
