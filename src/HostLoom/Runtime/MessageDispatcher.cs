using Microsoft.Extensions.DependencyInjection;

namespace HostLoom;

internal sealed class MessageDispatcher(
    HostLoomConfiguration configuration,
    IServiceScopeFactory scopeFactory,
    IMessageSerializer serializer)
{
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

        if (!configuration.TryGetHandler(endpoint, request.MessageType, out var registration))
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
            var message = serializer.Deserialize(request.Body, registration.RequestType)
                ?? throw new InvalidDataException($"Request body for '{request.MessageType}' was null.");

            var scope = scopeFactory.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                var executor = (IRequestExecutor)scope.ServiceProvider.GetRequiredService(registration.ExecutorType);
                var response = await executor.ExecuteAsync(message, cancellationToken).ConfigureAwait(false);

                var envelope = new MessageEnvelope
                {
                    MessageId = Guid.NewGuid(),
                    CorrelationId = request.MessageId,
                    Kind = MessageKind.Response,
                    MessageType = request.ResponseType,
                    ResponseType = request.ResponseType,
                    SentAt = DateTimeOffset.UtcNow,
                    Body = serializer.Serialize(response, registration.ResponseType)
                };

                return WireEnvelopeCodec.Encode(envelope);
            }
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
