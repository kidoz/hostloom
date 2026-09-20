using System.Text.Json;
using System.Text.Json.Serialization;

namespace HostLoom;

internal static class WireEnvelopeCodec
{
    private static readonly JsonSerializerOptions Options = new(JsonSerializerDefaults.Web)
    {
        Converters = { new JsonStringEnumConverter<MessageKind>() },
    };

    public static byte[] Encode(MessageEnvelope envelope) =>
        JsonSerializer.SerializeToUtf8Bytes(envelope, Options);

    public static MessageEnvelope Decode(ReadOnlySpan<byte> frame)
    {
        MessageEnvelope envelope;
        try
        {
            envelope =
                JsonSerializer.Deserialize<MessageEnvelope>(frame, Options)
                ?? throw new MalformedEnvelopeException(
                    "The broker frame did not contain a message envelope."
                );
        }
        catch (JsonException exception)
        {
            throw new MalformedEnvelopeException(
                "The broker frame did not contain a valid message envelope.",
                exception
            );
        }

        Validate(envelope);
        return envelope;
    }

    /// <summary>
    /// Rejects the identifiers a sender can leave out or zero. A missing <c>MessageId</c> reads as
    /// <see cref="Guid.Empty"/> after deserialization, and every idempotency key and correlation
    /// lookup is built on that id, so accepting it would let unrelated deliveries collide.
    /// </summary>
    internal static void Validate(MessageEnvelope envelope)
    {
        if (envelope.MessageId == Guid.Empty)
        {
            throw new MalformedEnvelopeException("The message envelope has no message id.");
        }

        if (envelope.CorrelationId == Guid.Empty)
        {
            throw new MalformedEnvelopeException(
                "The message envelope carries an empty correlation id."
            );
        }

        if (
            envelope.Kind is MessageKind.Response or MessageKind.Fault
            && envelope.CorrelationId is null
        )
        {
            throw new MalformedEnvelopeException(
                $"A '{envelope.Kind}' envelope must carry the correlation id of its request."
            );
        }
    }
}
