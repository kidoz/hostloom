using System.Net.WebSockets;
using MessagePack;

namespace HostLoom.AspNetCore.WebSockets;

public sealed class MessagePackWebSocketHubProtocol : IWebSocketHubProtocol
{
    private static readonly MessagePackSerializerOptions SerializerOptions =
        MessagePackSerializerOptions.Standard.WithSecurity(MessagePackSecurity.UntrustedData);

    public const string ProtocolName = "hostloom.msgpack.v1";

    public string SubProtocol => ProtocolName;

    public WebSocketMessageType MessageType => WebSocketMessageType.Binary;

    public HubFrame Decode(ReadOnlySpan<byte> payload)
    {
        try
        {
            return MessagePackSerializer.Deserialize<HubFrame>(payload.ToArray(), SerializerOptions)
                ?? throw new InvalidDataException("The MessagePack frame was null.");
        }
        catch (MessagePackSerializationException exception)
        {
            throw new InvalidDataException("The MessagePack frame was invalid.", exception);
        }
    }

    public byte[] Encode(HubFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        return MessagePackSerializer.Serialize(frame, SerializerOptions);
    }
}
