using Microsoft.Extensions.DependencyInjection;

namespace HostLoom;

internal sealed class EventDispatcher(
    HostLoomConfiguration configuration,
    IServiceScopeFactory scopeFactory,
    IMessageSerializer serializer)
{
    public async ValueTask DispatchAsync(
        RequestAddress topic,
        string subscription,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken)
    {
        var envelope = WireEnvelopeCodec.Decode(frame.Span);
        if (envelope.Kind is not MessageKind.Event)
        {
            throw new InvalidDataException($"Expected an event envelope, received '{envelope.Kind}'.");
        }

        // A topic carries every event type published to it. A subscription that has no handler for
        // this one is not misconfigured, it is simply uninterested.
        if (!configuration.TryGetSubscriber(topic, subscription, envelope.MessageType, out var registration))
        {
            return;
        }

        using var activity = HostLoomDiagnostics.ActivitySource.StartActivity("hostloom handle event");
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.destination.name", topic.Value);
        activity?.SetTag("messaging.consumer.group.name", subscription);
        activity?.SetTag("messaging.message.type", envelope.MessageType);
        activity?.SetTag("messaging.message.id", envelope.MessageId);

        var message = serializer.Deserialize(envelope.Body, registration.EventType)
            ?? throw new InvalidDataException($"Event body for '{envelope.MessageType}' was null.");

        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            var executor = (IEventExecutor)scope.ServiceProvider.GetRequiredService(registration.ExecutorType);
            await executor.ExecuteAsync(message, registration.HandlerTypes, cancellationToken).ConfigureAwait(false);
        }
    }
}
