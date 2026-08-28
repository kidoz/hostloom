using System.Text.Json;

namespace HostLoom;

public sealed class SystemTextJsonMessageSerializer(JsonSerializerOptions? options = null)
    : IMessageSerializer
{
    private readonly JsonSerializerOptions _options = options ?? new(JsonSerializerDefaults.Web);

    public byte[] Serialize(object? value, Type type) =>
        JsonSerializer.SerializeToUtf8Bytes(value, type, _options);

    public object? Deserialize(ReadOnlySpan<byte> payload, Type type)
    {
        try
        {
            return JsonSerializer.Deserialize(payload, type, _options);
        }
        catch (JsonException exception)
        {
            throw new MalformedEnvelopeException(
                $"The message payload could not be deserialized as '{type}'.",
                exception
            );
        }
    }
}
