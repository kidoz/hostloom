using System.Diagnostics;
using HostLoom.Pipelines;
using Microsoft.Extensions.Options;

namespace HostLoom;

internal sealed class EventDispatcher(
    HostLoomConfiguration configuration,
    IMessageSerializer serializer,
    ReceivePipeline receivePipeline,
    IOptions<HostLoomOptions> options,
    TimeProvider? timeProvider = null,
    ReceiveAdmission? admission = null
)
{
    private readonly TimeProvider _clock = timeProvider ?? TimeProvider.System;
    private readonly ReceiveAdmission _admission = admission ?? new ReceiveAdmission();
    public async ValueTask DispatchAsync(
        RequestAddress topic,
        string subscription,
        ReadOnlyMemory<byte> frame,
        CancellationToken cancellationToken
    )
    {
        var envelope = WireEnvelopeCodec.Decode(frame.Span, options.Value.MaxFrameBytes);
        if (envelope.Kind is not MessageKind.Event)
        {
            throw new MalformedEnvelopeException(
                $"Expected an event envelope, received '{envelope.Kind}'."
            );
        }

        var destination = new ReceiveDestination(topic.Value, subscription);
        if (_admission.TryGetSuspension(destination, _clock.GetUtcNow(), out var resumeAt))
        {
            throw new ReceiveSuspendedException(topic, subscription, resumeAt);
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

        using var flow = MessageFlow.Begin(envelope);
        using var activity = HostLoomDiagnostics.ActivitySource.StartActivity(
            "hostloom handle event"
        );
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.destination.name", topic.Value);
        activity?.SetTag("messaging.consumer.group.name", subscription);
        activity?.SetTag("messaging.message.type", envelope.MessageType);
        activity?.SetTag("messaging.message.id", envelope.MessageId);
        MessageFlow.Tag(activity);

        var tags = new TagList
        {
            { "messaging.destination.name", topic.Value },
            { "messaging.message.type", envelope.MessageType },
            { HostLoomDiagnostics.MessageKindTag, HostLoomDiagnostics.EventKind },
        };

        var message =
            serializer.Deserialize(envelope.Body, registration.EventType)
            ?? throw new MalformedEnvelopeException(
                $"Event body for '{envelope.MessageType}' was null."
            );

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
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // The listener is stopping; like a cancelled request, that is not a fault.
            throw;
        }
        catch (Exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error);
            HostLoomDiagnostics.Faults.Add(1, tags);

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
