namespace HostLoom;

public interface IRequestClient<in TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> GetResponseAsync(
        RequestAddress destination,
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    );
}
