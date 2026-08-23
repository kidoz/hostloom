using System.Net.WebSockets;

namespace HostLoom.AspNetCore.WebSockets;

/// <summary>Encodes the common hub frame for one negotiated WebSocket subprotocol.</summary>
public interface IWebSocketHubProtocol
{
    string SubProtocol { get; }

    WebSocketMessageType MessageType { get; }

    HubFrame Decode(ReadOnlySpan<byte> payload);

    byte[] Encode(HubFrame frame);
}
