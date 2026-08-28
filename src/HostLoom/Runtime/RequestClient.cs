using Microsoft.Extensions.Options;

namespace HostLoom;

internal sealed class RequestClient<TRequest, TResponse>(
    IRequestBroker broker,
    IMessageSerializer serializer,
    IOptions<HostLoomOptions> options
) : IRequestClient<TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    public async ValueTask<TResponse> GetResponseAsync(
        RequestAddress destination,
        TRequest request,
        TimeSpan? timeout = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);
        var effectiveTimeout = timeout ?? options.Value.RequestTimeout;
        if (effectiveTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout),
                "The request timeout must be positive."
            );
        }

        var requestId = Guid.NewGuid();
        var envelope = new MessageEnvelope
        {
            MessageId = requestId,
            Kind = MessageKind.Request,
            MessageType = MessageTypeName.For<TRequest>(),
            ResponseType = MessageTypeName.For<TResponse>(),
            SentAt = DateTimeOffset.UtcNow,
            Body = serializer.Serialize(request),
        };

        using var activity = HostLoomDiagnostics.ActivitySource.StartActivity("hostloom request");
        activity?.SetTag("messaging.operation.type", "send");
        activity?.SetTag("messaging.destination.name", destination.Value);
        activity?.SetTag("messaging.message.id", requestId);

        var responseFrame = await broker
            .RequestAsync(
                destination,
                WireEnvelopeCodec.Encode(envelope),
                requestId,
                effectiveTimeout,
                cancellationToken
            )
            .ConfigureAwait(false);

        var response = WireEnvelopeCodec.Decode(responseFrame.Span);
        if (response.CorrelationId != requestId)
        {
            throw new MalformedEnvelopeException(
                $"Response correlation id '{response.CorrelationId}' did not match request '{requestId}'."
            );
        }

        if (response.Kind is MessageKind.Fault)
        {
            throw new RemoteRequestException(
                response.Fault
                    ?? new RemoteFault("Unknown", "The remote endpoint returned an empty fault.")
            );
        }

        if (
            response.Kind is not MessageKind.Response
            || response.MessageType != MessageTypeName.For<TResponse>()
        )
        {
            throw new MalformedEnvelopeException(
                $"Expected response '{MessageTypeName.For<TResponse>()}', received '{response.MessageType}'."
            );
        }

        return serializer.Deserialize<TResponse>(response.Body)
            ?? throw new MalformedEnvelopeException(
                $"Response body for '{response.MessageType}' was null."
            );
    }
}
