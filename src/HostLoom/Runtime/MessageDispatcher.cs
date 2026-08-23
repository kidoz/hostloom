using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom;

internal sealed class MessageDispatcher
{
    private readonly HostLoomConfiguration _configuration;
    private readonly IMessageSerializer _serializer;
    private readonly IPipe<ReceiveContext> _receivePipe;

    public MessageDispatcher(
        HostLoomConfiguration configuration,
        IServiceScopeFactory scopeFactory,
        IMessageSerializer serializer)
    {
        _configuration = configuration;
        _serializer = serializer;

        // Composed once, not per delivery: a circuit breaker or rate limit is only meaningful if
        // its state is shared across every request the endpoint receives.
        _receivePipe = Pipe.Create<ReceiveContext>(builder =>
        {
            configuration.ReceivePipeline?.Invoke(builder);
            builder.Use(new ExecuteRequestFilter(scopeFactory));
        });
    }

    public async ValueTask<ReadOnlyMemory<byte>> DispatchAsync(
        RequestAddress endpoint,
        ReadOnlyMemory<byte> requestFrame,
        CancellationToken cancellationToken)
    {
        var request = WireEnvelopeCodec.Decode(requestFrame.Span);

        if (request.Kind is not MessageKind.Request)
        {
            throw new InvalidDataException($"Expected a request envelope, received '{request.Kind}'.");
        }

        if (!_configuration.TryGetHandler(endpoint, request.MessageType, out var registration))
        {
            return EncodeFault(
                request,
                new InvalidOperationException($"No handler is registered for '{request.MessageType}' on endpoint '{endpoint}'."));
        }

        var registeredResponseType = MessageTypeName.For(registration.ResponseType);
        if (!string.Equals(request.ResponseType, registeredResponseType, StringComparison.Ordinal))
        {
            return EncodeFault(
                request,
                new InvalidDataException(
                    $"Request declares response '{request.ResponseType}', but '{request.MessageType}' returns '{registeredResponseType}'."));
        }

        using var activity = HostLoomDiagnostics.ActivitySource.StartActivity("hostloom handle request");
        activity?.SetTag("messaging.operation.type", "process");
        activity?.SetTag("messaging.destination.name", endpoint.Value);
        activity?.SetTag("messaging.message.type", request.MessageType);
        activity?.SetTag("messaging.message.id", request.MessageId);

        try
        {
            var message = _serializer.Deserialize(request.Body, registration.RequestType)
                ?? throw new InvalidDataException($"Request body for '{request.MessageType}' was null.");

            var receiveContext = new ReceiveContext(
                endpoint,
                request.MessageId,
                request.MessageType,
                registration.ExecutorType,
                message,
                cancellationToken);

            await _receivePipe.SendAsync(receiveContext).ConfigureAwait(false);

            var envelope = new MessageEnvelope
            {
                MessageId = Guid.NewGuid(),
                CorrelationId = request.MessageId,
                Kind = MessageKind.Response,
                MessageType = request.ResponseType,
                ResponseType = request.ResponseType,
                SentAt = DateTimeOffset.UtcNow,
                Body = _serializer.Serialize(receiveContext.Response, registration.ResponseType)
            };

            return WireEnvelopeCodec.Encode(envelope);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            activity?.SetStatus(System.Diagnostics.ActivityStatusCode.Error, exception.Message);
            return EncodeFault(request, exception);
        }
    }

    private static byte[] EncodeFault(MessageEnvelope request, Exception exception) =>
        WireEnvelopeCodec.Encode(new MessageEnvelope
        {
            MessageId = Guid.NewGuid(),
            CorrelationId = request.MessageId,
            Kind = MessageKind.Fault,
            MessageType = request.ResponseType,
            ResponseType = request.ResponseType,
            SentAt = DateTimeOffset.UtcNow,
            Body = [],
            Fault = new RemoteFault(exception.GetType().FullName ?? exception.GetType().Name, exception.Message)
        });
}
