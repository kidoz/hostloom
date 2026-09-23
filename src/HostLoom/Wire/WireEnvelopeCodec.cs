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
    /// Rejects the identifiers and names a sender can leave out, zero, or null. A missing
    /// <c>MessageId</c> reads as <see cref="Guid.Empty"/> after deserialization, and every
    /// idempotency key and correlation lookup is built on that id, so accepting it would let
    /// unrelated deliveries collide. The web defaults do not enforce nullable annotations, so an
    /// explicit <c>null</c> reaches a non-nullable name: a null message type would fail the
    /// registration lookup as an argument error, which a transport retries as a handler failure
    /// instead of skipping the frame as poison.
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

        if (string.IsNullOrEmpty(envelope.MessageType))
        {
            throw new MalformedEnvelopeException("The message envelope has no message type.");
        }

        // Only a request reads its response type, to match the registration and to type the
        // reply. An event carries an empty one by design, and a reply's is never read.
        if (envelope.Kind is MessageKind.Request && string.IsNullOrEmpty(envelope.ResponseType))
        {
            throw new MalformedEnvelopeException(
                "A 'Request' envelope must name the response type it expects."
            );
        }

        // An absent fault is tolerated and reaches the caller as an unknown one; a present fault
        // must carry what RemoteRequestException exposes. An empty message is legitimate.
        if (
            envelope is { Kind: MessageKind.Fault, Fault: { } fault }
            && (string.IsNullOrEmpty(fault.ErrorType) || fault.Message is null)
        )
        {
            throw new MalformedEnvelopeException(
                "A 'Fault' envelope's fault must carry an error type and a message."
            );
        }
    }
}
