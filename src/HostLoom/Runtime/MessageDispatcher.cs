using System.Diagnostics;
using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom;

internal sealed class MessageDispatcher
{
    private readonly HostLoomConfiguration _configuration;
    private readonly IMessageSerializer _serializer;
    private readonly ReceivePipeline _receivePipeline;

    public MessageDispatcher(
        HostLoomConfiguration configuration,
        IMessageSerializer serializer,
        ReceivePipeline receivePipeline
    )
    {
        _configuration = configuration;
        _serializer = serializer;
        _receivePipeline = receivePipeline;
    }

    public async ValueTask<ReadOnlyMemory<byte>> DispatchAsync(
        RequestAddress endpoint,
        ReadOnlyMemory<byte> requestFrame,
        CancellationToken cancellationToken
    )
    {
        var request = WireEnvelopeCodec.Decode(requestFrame.Span);

        if (request.Kind is not MessageKind.Request)
        {
            throw new InvalidDataException(
                $"Expected a request envelope, received '{request.Kind}'."
            );
        }

        var tags = new TagList
        {
            { "messaging.destination.name", endpoint.Value },
            { "messaging.message.type", request.MessageType },
        };

        if (!_configuration.TryGetHandler(endpoint, request.MessageType, out var registration))
        {
            return EncodeFault(
                request,
                tags,
                new InvalidOperationException(
                    $"No handler is registered for '{request.MessageType}' on endpoint '{endpoint}'."
                )
            );
        }

        var registeredResponseType = MessageTypeName.For(registration.ResponseType);
        if (!string.Equals(request.ResponseType, registeredResponseType, StringComparison.Ordinal))
        {
            return EncodeFault(
                request,
                tags,
                new InvalidDataException(
                    $"Request declares response '{request.ResponseType}', but '{request.MessageType}' returns '{registeredResponseType}'."
                )
            );
        }

        using var activity = HostLoomDiagnostics.ActivitySource.StartActivity(
            "hostloom handle request"
        );
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.destination.name", endpoint.Value);
        activity?.SetTag("messaging.message.type", request.MessageType);
        activity?.SetTag("messaging.message.id", request.MessageId);

        var start = Stopwatch.GetTimestamp();
        HostLoomDiagnostics.ActiveRequests.Add(1, tags);
        try
        {
            var message =
                _serializer.Deserialize(request.Body, registration.RequestType)
                ?? throw new InvalidDataException(
                    $"Request body for '{request.MessageType}' was null."
                );

            var receiveContext = new RequestReceiveContext(
                endpoint,
                request.MessageId,
                request.MessageType,
                registration.ExecutorType,
                message,
                cancellationToken
            );

            await _receivePipeline.SendAsync(receiveContext).ConfigureAwait(false);
            RecordRetries(receiveContext, tags);

            var envelope = new MessageEnvelope
            {
                MessageId = Guid.NewGuid(),
                CorrelationId = request.MessageId,
                Kind = MessageKind.Response,
                MessageType = request.ResponseType,
                ResponseType = request.ResponseType,
                SentAt = DateTimeOffset.UtcNow,
                Body = _serializer.Serialize(receiveContext.Response, registration.ResponseType),
            };

            return WireEnvelopeCodec.Encode(envelope);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            return EncodeFault(request, tags, exception);
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

    // The payload is absent unless a retry filter ran, so its number is the count of extra attempts.
    private static void RecordRetries(ReceiveContext context, in TagList tags)
    {
        if (context.TryGetPayload<RetryAttempt>(out var attempt) && attempt is not null)
        {
            HostLoomDiagnostics.Retries.Add(attempt.Number, tags);
        }
    }

    private static byte[] EncodeFault(MessageEnvelope request, in TagList tags, Exception exception)
    {
        HostLoomDiagnostics.Faults.Add(1, tags);
        return EncodeFaultCore(request, exception);
    }

    private static byte[] EncodeFaultCore(MessageEnvelope request, Exception exception) =>
        WireEnvelopeCodec.Encode(
            new MessageEnvelope
            {
                MessageId = Guid.NewGuid(),
                CorrelationId = request.MessageId,
                Kind = MessageKind.Fault,
                MessageType = request.ResponseType,
                ResponseType = request.ResponseType,
                SentAt = DateTimeOffset.UtcNow,
                Body = [],
                Fault = new RemoteFault(
                    exception.GetType().FullName ?? exception.GetType().Name,
                    exception.Message
                ),
            }
        );
}
