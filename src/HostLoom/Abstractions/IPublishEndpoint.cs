namespace HostLoom;

/// <summary>Publishes events to a topic. Every subscription on that topic receives a copy.</summary>
public interface IPublishEndpoint
{
    /// <summary>
    /// Publishes <paramref name="event"/> to <paramref name="topic"/>. The wire message type is
    /// taken from the runtime type, so publishing through a base-typed variable still reaches
    /// subscribers registered for the concrete type.
    /// </summary>
    /// <exception cref="NotSupportedException">The configured transport has no publish/subscribe support.</exception>
    ValueTask PublishAsync<TEvent>(
        RequestAddress topic,
        TEvent @event,
        CancellationToken cancellationToken = default
    )
        where TEvent : class, IEvent;
}
