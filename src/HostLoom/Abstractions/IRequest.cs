namespace HostLoom;

/// <summary>Marks a message as a request with a single expected response type.</summary>
/// <typeparam name="TResponse">The successful response contract.</typeparam>
public interface IRequest<out TResponse>;
