using System.Runtime.CompilerServices;

namespace HostLoom.AspNetCore.WebSockets;

internal interface IWebSocketTopicSnapshotInvoker
{
    IAsyncEnumerable<SerializedWebSocketSnapshot> GetSnapshotAsync(
        WebSocketTopicSnapshotContext context,
        Func<object, string?> keySelector,
        CancellationToken cancellationToken
    );
}

internal sealed class WebSocketTopicSnapshotInvoker<TEvent>(
    IWebSocketTopicSnapshotProvider<TEvent> provider,
    IMessageSerializer serializer
) : IWebSocketTopicSnapshotInvoker
    where TEvent : class, IEvent
{
    public async IAsyncEnumerable<SerializedWebSocketSnapshot> GetSnapshotAsync(
        WebSocketTopicSnapshotContext context,
        Func<object, string?> keySelector,
        [EnumeratorCancellation] CancellationToken cancellationToken
    )
    {
        await foreach (
            var value in provider
                .GetSnapshotAsync(context, cancellationToken)
                .WithCancellation(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            if (value is null)
            {
                throw new InvalidDataException("A WebSocket topic snapshot contained null.");
            }

            var key = keySelector(value);
            if (key is { Length: > 256 })
            {
                throw new InvalidDataException(
                    "A WebSocket topic snapshot key exceeded 256 characters."
                );
            }

            yield return new SerializedWebSocketSnapshot(key, serializer.Serialize(value));
        }
    }
}

internal readonly record struct SerializedWebSocketSnapshot(
    string? Key,
    ReadOnlyMemory<byte> Payload
);
