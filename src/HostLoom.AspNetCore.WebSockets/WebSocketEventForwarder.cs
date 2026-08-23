namespace HostLoom.AspNetCore.WebSockets;

internal sealed class WebSocketEventForwarder<TEvent>(
    GatewayConfiguration configuration,
    WebSocketSessionRegistry sessions,
    IMessageSerializer serializer
) : IEventHandler<TEvent>
    where TEvent : class, IEvent
{
    public ValueTask HandleAsync(TEvent @event, CancellationToken cancellationToken)
    {
        var route = configuration.GetTopic(typeof(TEvent));
        var key = route.KeySelector(@event);
        if (key is { Length: > 256 })
        {
            throw new InvalidOperationException(
                "A WebSocket event key cannot exceed 256 characters."
            );
        }

        sessions.Publish(route.Name, key, serializer.Serialize(@event));
        return ValueTask.CompletedTask;
    }
}
