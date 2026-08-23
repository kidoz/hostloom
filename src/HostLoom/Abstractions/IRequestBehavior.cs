namespace HostLoom;

public delegate ValueTask<TResponse> RequestHandlerDelegate<TResponse>(CancellationToken cancellationToken);

/// <summary>
/// A transport-independent middleware stage. Behaviors execute in registration order and may
/// validate, observe, retry, short-circuit, or decorate request handling.
/// </summary>
public interface IRequestBehavior<in TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    ValueTask<TResponse> HandleAsync(
        TRequest request,
        RequestHandlerDelegate<TResponse> next,
        CancellationToken cancellationToken);
}
