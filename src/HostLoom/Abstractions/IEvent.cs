namespace HostLoom;

/// <summary>
/// Marks a message as an event: published to a topic, delivered to every subscription on it, and
/// answered by nobody. Contrast <see cref="IRequest{TResponse}"/>, which addresses one handler and
/// expects exactly one reply.
/// </summary>
public interface IEvent;
