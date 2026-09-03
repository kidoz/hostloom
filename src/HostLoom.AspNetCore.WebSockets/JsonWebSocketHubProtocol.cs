using System.Net.WebSockets;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace HostLoom.AspNetCore.WebSockets;

public sealed class JsonWebSocketHubProtocol : IWebSocketHubProtocol
{
    private static readonly JsonSerializerOptions SerializerOptions = new(
        JsonSerializerDefaults.Web
    )
    {
        MaxDepth = 32,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
        Converters = { new HexGuidJsonConverter() },
    };

    public const string ProtocolName = "hostloom.json.v1";

    public string SubProtocol => ProtocolName;

    public WebSocketMessageType MessageType => WebSocketMessageType.Text;

    public HubFrame Decode(ReadOnlySpan<byte> payload)
    {
        try
        {
            var frame =
                JsonSerializer.Deserialize<JsonHubFrame>(payload, SerializerOptions)
                ?? throw new InvalidDataException("The JSON frame was null.");
            return frame.ToHubFrame();
        }
        catch (JsonException exception)
        {
            throw new InvalidDataException("The JSON frame was invalid.", exception);
        }
    }

    public byte[] Encode(HubFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        return JsonSerializer.SerializeToUtf8Bytes(
            JsonHubFrame.FromHubFrame(frame, camelCaseKind: true),
            SerializerOptions
        );
    }
}
