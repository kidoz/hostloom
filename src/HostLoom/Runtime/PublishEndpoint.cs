namespace HostLoom;

internal sealed class PublishEndpoint(IRequestBroker broker, IMessageSerializer serializer)
    : IPublishEndpoint
{
    public ValueTask PublishAsync<TEvent>(
        RequestAddress topic,
        TEvent @event,
        CancellationToken cancellationToken = default
    )
        where TEvent : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(@event);
        if (broker is not IEventBroker events)
        {
            throw new NotSupportedException(
                $"The configured transport '{broker.GetType().Name}' supports request/response only. "
                    + $"Publishing to '{topic}' requires a transport that implements {nameof(IEventBroker)}."
            );
        }

        // Runtime type, not TEvent: publishing through a base-typed variable must still reach
        // subscribers registered for the concrete contract.
        var eventType = @event.GetType();
        var envelope = new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            Kind = MessageKind.Event,
            MessageType = MessageTypeName.For(eventType),
            ResponseType = string.Empty,
            SentAt = DateTimeOffset.UtcNow,
            Body = serializer.Serialize(@event, eventType),
        };

        return events.PublishAsync(topic, WireEnvelopeCodec.Encode(envelope), cancellationToken);
    }
}
