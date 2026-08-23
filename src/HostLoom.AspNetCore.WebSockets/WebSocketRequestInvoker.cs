namespace HostLoom.AspNetCore.WebSockets;

internal interface IWebSocketRequestInvoker
{
    ValueTask<ReadOnlyMemory<byte>> InvokeAsync(
        ReadOnlyMemory<byte> payload,
        RequestAddress destination,
        TimeSpan timeout,
        CancellationToken cancellationToken
    );
}

internal sealed class WebSocketRequestInvoker<TRequest, TResponse>(
    IRequestClient<TRequest, TResponse> client,
    IMessageSerializer serializer
) : IWebSocketRequestInvoker
    where TRequest : class, IRequest<TResponse>
{
    public async ValueTask<ReadOnlyMemory<byte>> InvokeAsync(
        ReadOnlyMemory<byte> payload,
        RequestAddress destination,
        TimeSpan timeout,
        CancellationToken cancellationToken
    )
    {
        var request =
            serializer.Deserialize<TRequest>(payload.Span)
            ?? throw new InvalidDataException("The request payload was null.");
        var response = await client
            .GetResponseAsync(destination, request, timeout, cancellationToken)
            .ConfigureAwait(false);
        return serializer.Serialize(response);
    }
}
