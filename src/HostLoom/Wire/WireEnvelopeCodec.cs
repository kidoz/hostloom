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
        try
        {
            return JsonSerializer.Deserialize<MessageEnvelope>(frame, Options)
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
    }
}
