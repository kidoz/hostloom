namespace HostLoom;

/// <summary>
/// The <see cref="IPublishEndpoint"/> in effect when the outbox is on: encodes the same envelope a
/// direct publish would send, appends it to the store from the caller's scope, and wakes the
/// relay. Publishing completes when the append does, not when the transport has the frame.
/// </summary>
internal sealed class OutboxPublishEndpoint(
    IRequestBroker broker,
    IMessageSerializer serializer,
    IOutboxStore store,
    OutboxRelay relay,
    TimeProvider clock
) : IPublishEndpoint
{
    public async ValueTask PublishAsync<TEvent>(
        RequestAddress topic,
        TEvent @event,
        CancellationToken cancellationToken = default
    )
        where TEvent : class, IEvent
    {
        ArgumentNullException.ThrowIfNull(@event);
        if (broker is not IEventBroker)
        {
            throw new NotSupportedException(
                $"The configured transport '{broker.GetType().Name}' supports request/response only. "
                    + $"Publishing to '{topic}' requires a transport that implements {nameof(IEventBroker)}."
            );
        }

        var eventType = @event.GetType();
        var messageType = MessageTypeName.For(eventType);
        var now = clock.GetUtcNow();
        var envelope = new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            Kind = MessageKind.Event,
            MessageType = messageType,
            ResponseType = string.Empty,
            SentAt = now,
            Body = serializer.Serialize(@event, eventType),
        };

        await store
            .AppendAsync(
                new OutboxMessage
                {
                    MessageId = envelope.MessageId,
                    Topic = topic.Value,
                    MessageType = messageType,
                    Frame = WireEnvelopeCodec.Encode(envelope),
                    EnqueuedAt = now,
                },
                cancellationToken
            )
            .ConfigureAwait(false);
        relay.Wake();
    }
}
