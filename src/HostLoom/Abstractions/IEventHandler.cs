namespace HostLoom;

/// <summary>
/// Consumes an event within one subscription. Several handlers may consume the same event type;
/// all of them run, in registration order, inside a single dependency-injection scope.
/// </summary>
public interface IEventHandler<in TEvent>
    where TEvent : IEvent
{
    ValueTask HandleAsync(TEvent @event, CancellationToken cancellationToken);
}
