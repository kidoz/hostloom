using System.Diagnostics;
using HostLoom.Pipelines;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace HostLoom;

internal sealed class MessageDispatcher
{
    /// <summary>Fault type reported for any handler failure the caller is not allowed to read.</summary>
    internal const string HandlerFaultType = "HandlerFault";

    /// <summary>Fault message reported with <see cref="HandlerFaultType"/>.</summary>
    internal const string HandlerFaultMessage = "The request handler failed.";

    /// <summary>Fault type for a request whose message type has no handler on the endpoint.</summary>
    internal const string HandlerNotFoundFaultType = "HandlerNotFound";

    /// <summary>Fault type for a request whose declared response type does not match the registration.</summary>
    internal const string ResponseTypeMismatchFaultType = "ResponseTypeMismatch";

    /// <summary>
    /// Fault type for a request a receive filter completed without running its handler, such as
    /// <c>UseTerminal</c> or a conditional branch that ends the pipeline.
    /// </summary>
    internal const string HandlerNotRunFaultType = "HandlerNotRun";

    /// <summary>Fault message reported with <see cref="HandlerNotRunFaultType"/>.</summary>
    internal const string HandlerNotRunFaultMessage =
        "A receive filter completed the request without running its handler, so there is no response.";

    /// <summary>
    /// Metric tag used in place of a message type the endpoint does not know. The wire value is
    /// caller-controlled, so tagging it verbatim would let a caller mint unbounded time series.
    /// </summary>
    internal const string UnknownMessageTypeTag = "unknown";

    private readonly HostLoomConfiguration _configuration;
    private readonly IMessageSerializer _serializer;
    private readonly ReceivePipeline _receivePipeline;
    private readonly bool _includeFaultDetails;
    private readonly ILogger<MessageDispatcher> _logger;

    public MessageDispatcher(
        HostLoomConfiguration configuration,
        IMessageSerializer serializer,
        ReceivePipeline receivePipeline,
        IOptions<HostLoomOptions>? options = null,
        ILogger<MessageDispatcher>? logger = null
    )
    {
        _configuration = configuration;
        _serializer = serializer;
        _receivePipeline = receivePipeline;
        _includeFaultDetails = options?.Value.IncludeFaultDetails ?? false;
        _logger = logger ?? NullLogger<MessageDispatcher>.Instance;
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
            throw new MalformedEnvelopeException(
                $"Expected a request envelope, received '{request.Kind}'."
            );
        }

        if (!_configuration.TryGetHandler(endpoint, request.MessageType, out var registration))
        {
            var unknownTags = new TagList
            {
                { "messaging.destination.name", endpoint.Value },
                { "messaging.message.type", UnknownMessageTypeTag },
                { HostLoomDiagnostics.MessageKindTag, HostLoomDiagnostics.RequestKind },
            };
            _logger.LogWarning(
                "No handler is registered for '{MessageType}' on endpoint '{Endpoint}'.",
                request.MessageType,
                endpoint.Value
            );
            return EncodeFault(
                request,
                unknownTags,
                new RemoteFault(
                    HandlerNotFoundFaultType,
                    $"No handler is registered for '{request.MessageType}' on endpoint '{endpoint}'."
                )
            );
        }

        var tags = new TagList
        {
            { "messaging.destination.name", endpoint.Value },
            { "messaging.message.type", request.MessageType },
            { HostLoomDiagnostics.MessageKindTag, HostLoomDiagnostics.RequestKind },
        };

        var registeredResponseType = MessageTypeName.For(registration.ResponseType);
        if (!string.Equals(request.ResponseType, registeredResponseType, StringComparison.Ordinal))
        {
            return EncodeFault(
                request,
                tags,
                new RemoteFault(
                    ResponseTypeMismatchFaultType,
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
                ?? throw new MalformedEnvelopeException(
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

            if (!receiveContext.Handled)
            {
                // A success envelope would carry no body, and the caller would blame the wire for
                // a malformed reply. The fault names no filter, like every framework fault.
                activity?.SetStatus(ActivityStatusCode.Error, HandlerNotRunFaultMessage);
                _logger.LogWarning(
                    "A receive filter completed request '{MessageType}' on endpoint '{Endpoint}' without running its handler.",
                    request.MessageType,
                    endpoint.Value
                );
                return EncodeFault(
                    request,
                    tags,
                    new RemoteFault(HandlerNotRunFaultType, HandlerNotRunFaultMessage)
                );
            }

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
            // The full exception stays here, in the log, whatever the caller is allowed to see.
            _logger.LogError(
                exception,
                "Request '{MessageType}' on endpoint '{Endpoint}' faulted.",
                request.MessageType,
                endpoint.Value
            );
            return EncodeFault(request, tags, DescribeFault(exception, _includeFaultDetails));
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

    /// <summary>
    /// What a remote caller learns about a handler failure. A <see cref="RemoteFaultException"/>
    /// was thrown for the caller, so it travels as-is; anything else is reduced to the fixed
    /// <see cref="HandlerFaultType"/> unless the deployment opted into full details.
    /// </summary>
    internal static RemoteFault DescribeFault(Exception exception, bool includeDetails)
    {
        if (includeDetails || exception is RemoteFaultException)
        {
            return new RemoteFault(
                exception.GetType().FullName ?? exception.GetType().Name,
                exception.Message
            );
        }

        return new RemoteFault(HandlerFaultType, HandlerFaultMessage);
    }

    // The payload is absent unless a retry filter ran, so its number is the count of extra attempts.
    private static void RecordRetries(ReceiveContext context, in TagList tags)
    {
        if (context.TryGetPayload<RetryAttempt>(out var attempt) && attempt is not null)
        {
            HostLoomDiagnostics.Retries.Add(attempt.Number, tags);
        }
    }

    private static byte[] EncodeFault(MessageEnvelope request, in TagList tags, RemoteFault fault)
    {
        HostLoomDiagnostics.Faults.Add(1, tags);
        return WireEnvelopeCodec.Encode(
            new MessageEnvelope
            {
                MessageId = Guid.NewGuid(),
                CorrelationId = request.MessageId,
                Kind = MessageKind.Fault,
                MessageType = request.ResponseType,
                ResponseType = request.ResponseType,
                SentAt = DateTimeOffset.UtcNow,
                Body = [],
                Fault = fault,
            }
        );
    }
}
