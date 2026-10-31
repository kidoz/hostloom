using Microsoft.Extensions.Options;

namespace HostLoom;

internal sealed class RequestClient<TRequest, TResponse>(
    IRequestBroker broker,
    IMessageSerializer serializer,
    IOptions<HostLoomOptions> options,
    TimeProvider? timeProvider = null
) : IRequestClient<TRequest, TResponse>
    where TRequest : IRequest<TResponse>
{
    private readonly TimeProvider _clock = timeProvider ?? TimeProvider.System;

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
        var sentAt = _clock.GetUtcNow();
        var (conversationId, initiatorId) = MessageFlow.StampOutbound();
        var envelope = new MessageEnvelope
        {
            MessageId = requestId,
            ConversationId = conversationId,
            InitiatorId = initiatorId,
            Kind = MessageKind.Request,
            MessageType = MessageTypeName.For<TRequest>(),
            ResponseType = MessageTypeName.For<TResponse>(),
            SentAt = sentAt,
            ExpiresAt = sentAt + effectiveTimeout,
            Body = serializer.Serialize(request),
        };

        using var activity = HostLoomDiagnostics.ActivitySource.StartActivity("hostloom request");
        activity?.SetTag("messaging.operation.type", "send");
        activity?.SetTag("messaging.destination.name", destination.Value);
        activity?.SetTag("messaging.message.id", requestId);
        activity?.SetTag("hostloom.conversation.id", conversationId);
        if (initiatorId is { } initiator)
        {
            activity?.SetTag("hostloom.initiator.id", initiator);
        }

        var responseFrame = await broker
            .RequestAsync(
                destination,
                WireEnvelopeCodec.Encode(envelope, options.Value.MaxFrameBytes),
                requestId,
                effectiveTimeout,
                cancellationToken
            )
            .ConfigureAwait(false);

        var response = WireEnvelopeCodec.Decode(responseFrame.Span, options.Value.MaxFrameBytes);
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
