namespace HostLoom;

internal interface IRequestExecutor
{
    ValueTask<object?> ExecuteAsync(object request, CancellationToken cancellationToken);
}

internal sealed class RequestExecutor<TRequest, TResponse>(
    IRequestHandler<TRequest, TResponse> handler,
    IEnumerable<IRequestBehavior<TRequest, TResponse>> behaviors
) : IRequestExecutor
    where TRequest : class, IRequest<TResponse>
{
    public async ValueTask<object?> ExecuteAsync(
        object request,
        CancellationToken cancellationToken
    )
    {
        var typedRequest = (TRequest)request;
        RequestHandlerDelegate<TResponse> pipeline = token =>
            handler.HandleAsync(typedRequest, token);

        foreach (var behavior in behaviors.Reverse())
        {
            var next = pipeline;
            pipeline = token => behavior.HandleAsync(typedRequest, next, token);
        }

        return await pipeline(cancellationToken).ConfigureAwait(false);
    }
}
