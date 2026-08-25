using System.Diagnostics;
using HostLoom.Pipelines;

namespace HostLoom;

internal sealed class EventDispatcher(
    HostLoomConfiguration configuration,
    IMessageSerializer serializer,
    ReceivePipeline receivePipeline
)
{
    public async ValueTask DispatchAsync(
        RequestAddress topic,
        string subscription,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken
    )
    {
        var envelope = WireEnvelopeCodec.Decode(frame.Span);
        if (envelope.Kind is not MessageKind.Event)
        {
            throw new InvalidDataException(
                $"Expected an event envelope, received '{envelope.Kind}'."
            );
        }

        // A topic carries every event type published to it. A subscription that has no handler for
        // this one is not misconfigured, it is simply uninterested.
        if (
            !configuration.TryGetSubscriber(
                topic,
                subscription,
                envelope.MessageType,
                out var registration
            )
        )
        {
            return;
        }

        using var activity = HostLoomDiagnostics.ActivitySource.StartActivity(
            "hostloom handle event"
        );
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.destination.name", topic.Value);
        activity?.SetTag("messaging.consumer.group.name", subscription);
        activity?.SetTag("messaging.message.type", envelope.MessageType);
        activity?.SetTag("messaging.message.id", envelope.MessageId);

        var tags = new TagList
        {
            { "messaging.destination.name", topic.Value },
            { "messaging.message.type", envelope.MessageType },
        };

        var message =
            serializer.Deserialize(envelope.Body, registration.EventType)
            ?? throw new InvalidDataException($"Event body for '{envelope.MessageType}' was null.");

        var context = new EventReceiveContext(
            topic,
            subscription,
            envelope.MessageId,
            envelope.MessageType,
            registration.ExecutorType,
            registration.HandlerTypes,
            message,
            cancellationToken
        );

        var start = Stopwatch.GetTimestamp();
        HostLoomDiagnostics.ActiveRequests.Add(1, tags);
        try
        {
            await receivePipeline.SendAsync(context).ConfigureAwait(false);
            if (context.TryGetPayload<RetryAttempt>(out var attempt) && attempt is not null)
            {
                HostLoomDiagnostics.Retries.Add(attempt.Number, tags);
            }
        }
        catch (Exception exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);

            // No fault envelope exists for an event: there is nobody to answer. The failure goes
            // back to the transport, which decides whether to redeliver or dead-letter.
            throw;
        }
        finally
        {
            HostLoomDiagnostics.ActiveRequests.Add(-1, tags);
            HostLoomDiagnostics.RequestDuration.Record(
                Stopwatch.GetElapsedTime(start).TotalSeconds,
                tags
            );
        }
    }
}
