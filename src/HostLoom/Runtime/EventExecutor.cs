using Microsoft.Extensions.DependencyInjection;

namespace HostLoom;

internal interface IEventExecutor
{
    ValueTask ExecuteAsync(
        object @event,
        IReadOnlyList<Type> handlerTypes,
        CancellationToken cancellationToken
    );
}

/// <summary>
/// Runs the handlers belonging to one subscription, inside the caller's scope.
/// </summary>
/// <remarks>
/// Handler types are passed in rather than resolved as <c>IEnumerable&lt;IEventHandler&lt;TEvent&gt;&gt;</c>.
/// The container has no notion of subscriptions, so resolving the set would hand every subscription
/// every handler registered for the event type anywhere in the process.
/// </remarks>
internal sealed class EventExecutor<TEvent>(IServiceProvider provider) : IEventExecutor
    where TEvent : class, IEvent
{
    public async ValueTask ExecuteAsync(
        object @event,
        IReadOnlyList<Type> handlerTypes,
        CancellationToken cancellationToken
    )
    {
        var typed = (TEvent)@event;
        foreach (var handlerType in handlerTypes)
        {
            var handler = (IEventHandler<TEvent>)provider.GetRequiredService(handlerType);
            await handler.HandleAsync(typed, cancellationToken).ConfigureAwait(false);
        }
    }
}
