using System.Buffers;
using System.Net.WebSockets;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;

namespace HostLoom.AspNetCore.WebSockets.Testing;

/// <summary>Drives HostLoom application frames through an ASP.NET Core test server.</summary>
public sealed class WebSocketTestClient : IAsyncDisposable
{
    private readonly TestServer _server;
    private WebSocket? _socket;

    /// <summary>Creates a client using JSON v1 unless another protocol is supplied.</summary>
    public WebSocketTestClient(TestServer server, IWebSocketHubProtocol? protocol = null)
    {
        ArgumentNullException.ThrowIfNull(server);
        _server = server;
        Protocol = protocol ?? new JsonWebSocketHubProtocol();
    }

    /// <summary>Gets the frame codec and negotiated subprotocol.</summary>
    public IWebSocketHubProtocol Protocol { get; }

    /// <summary>Gets or sets a callback that configures the HTTP upgrade request.</summary>
    public Action<HttpRequest>? ConfigureRequest { get; set; }

    /// <summary>Gets the connected socket.</summary>
    public WebSocket Socket =>
        _socket ?? throw new InvalidOperationException("The test client is not connected.");

    /// <summary>Connects to a gateway endpoint in the test server.</summary>
    public async Task ConnectAsync(Uri endpoint, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(endpoint);
        if (_socket is not null)
        {
            throw new InvalidOperationException("The test client is already connected.");
        }

        var client = _server.CreateWebSocketClient();
        client.SubProtocols.Add(Protocol.SubProtocol);
        client.ConfigureRequest = ConfigureRequest;
        _socket = await client.ConnectAsync(endpoint, cancellationToken).ConfigureAwait(false);
    }

    /// <summary>Encodes and sends one complete HostLoom frame.</summary>
    public ValueTask SendAsync(HubFrame frame, CancellationToken cancellationToken = default)
    {
        ArgumentNullException.ThrowIfNull(frame);
        var payload = Protocol.Encode(frame);
        return Socket.SendAsync(
            payload.AsMemory(),
            Protocol.MessageType,
            endOfMessage: true,
            cancellationToken
        );
    }

    /// <summary>Receives and decodes one complete HostLoom frame.</summary>
    public async ValueTask<HubFrame> ReceiveAsync(
        int maximumMessageSize = 1024 * 1024,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(maximumMessageSize);
        var payload = new ArrayBufferWriter<byte>();
        WebSocketMessageType? messageType = null;

        while (true)
        {
            var memory = payload.GetMemory(
                Math.Min(4096, maximumMessageSize - payload.WrittenCount)
            );
            var result = await Socket.ReceiveAsync(memory, cancellationToken).ConfigureAwait(false);
            if (result.MessageType is WebSocketMessageType.Close)
            {
                throw new EndOfStreamException(
                    $"The WebSocket closed before a frame was received: {Socket.CloseStatus}."
                );
            }

            messageType ??= result.MessageType;
            if (messageType != result.MessageType)
            {
                throw new InvalidDataException("A fragmented frame changed its message type.");
            }

            payload.Advance(result.Count);
            if (payload.WrittenCount > maximumMessageSize)
            {
                throw new InvalidDataException(
                    "The received frame exceeded the test client limit."
                );
            }

            if (result.EndOfMessage)
            {
                break;
            }

            if (payload.WrittenCount == maximumMessageSize)
            {
                throw new InvalidDataException(
                    "The received frame exceeded the test client limit."
                );
            }
        }

        if (messageType != Protocol.MessageType)
        {
            throw new InvalidDataException(
                "The received frame type does not match the negotiated subprotocol."
            );
        }

        return Protocol.Decode(payload.WrittenSpan);
    }

    /// <summary>Receives one frame and requires it to have the expected kind.</summary>
    public async ValueTask<HubFrame> ReceiveAsync(
        HubFrameKind expectedKind,
        int maximumMessageSize = 1024 * 1024,
        CancellationToken cancellationToken = default
    )
    {
        var frame = await ReceiveAsync(maximumMessageSize, cancellationToken).ConfigureAwait(false);
        if (frame.Kind != expectedKind)
        {
            throw new InvalidDataException(
                $"Expected a '{expectedKind}' frame but received '{frame.Kind}'."
            );
        }

        return frame;
    }

    /// <summary>Receives the initial welcome frame.</summary>
    public ValueTask<HubFrame> AwaitWelcomeAsync(CancellationToken cancellationToken = default) =>
        ReceiveAsync(HubFrameKind.Welcome, cancellationToken: cancellationToken);

    /// <summary>Receives a subscribed frame and validates its stream id.</summary>
    public async ValueTask<HubFrame> AwaitSubscribedAsync(
        ulong streamId,
        CancellationToken cancellationToken = default
    )
    {
        var frame = await ReceiveAsync(
                HubFrameKind.Subscribed,
                cancellationToken: cancellationToken
            )
            .ConfigureAwait(false);
        return RequireStream(frame, streamId);
    }

    /// <summary>Receives an event frame and validates its stream id.</summary>
    public async ValueTask<HubFrame> AwaitEventAsync(
        ulong streamId,
        CancellationToken cancellationToken = default
    )
    {
        var frame = await ReceiveAsync(HubFrameKind.Event, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return RequireStream(frame, streamId);
    }

    /// <summary>Receives a fault frame and validates its stream id.</summary>
    public async ValueTask<HubFrame> AwaitFaultAsync(
        ulong streamId,
        CancellationToken cancellationToken = default
    )
    {
        var frame = await ReceiveAsync(HubFrameKind.Fault, cancellationToken: cancellationToken)
            .ConfigureAwait(false);
        return RequireStream(frame, streamId);
    }

    /// <inheritdoc />
    public ValueTask DisposeAsync()
    {
        _socket?.Dispose();
        _socket = null;
        return ValueTask.CompletedTask;
    }

    private static HubFrame RequireStream(HubFrame frame, ulong streamId)
    {
        if (frame.StreamId != streamId)
        {
            throw new InvalidDataException(
                $"Expected stream '{streamId}' but received stream '{frame.StreamId}'."
            );
        }

        return frame;
    }
}
