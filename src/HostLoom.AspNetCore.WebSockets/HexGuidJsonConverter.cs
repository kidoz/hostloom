using System.Text.Json;
using System.Text.Json.Serialization;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Reads and writes a <see cref="Guid"/> as the 32 lowercase hex digits JSON v2 carries.</summary>
internal sealed class HexGuidJsonConverter : JsonConverter<Guid>
{
    public override Guid Read(
        ref Utf8JsonReader reader,
        Type typeToConvert,
        JsonSerializerOptions options
    )
    {
        // "N" is the only accepted form. Admitting the dashed or braced spellings would make
        // several encodings valid for one frame, which is what the published fixtures exist to
        // prevent.
        if (
            reader.TokenType is not JsonTokenType.String
            || !Guid.TryParseExact(reader.GetString(), "N", out var value)
        )
        {
            throw new JsonException("A frame identifier must be 32 hexadecimal digits.");
        }

        return value;
    }

    public override void Write(Utf8JsonWriter writer, Guid value, JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(writer);
        writer.WriteStringValue(value.ToString("N"));
    }
}
