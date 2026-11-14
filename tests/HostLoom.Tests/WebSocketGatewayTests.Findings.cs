using System.Net.WebSockets;
using System.Security.Claims;
using HostLoom.AspNetCore.WebSockets;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

public sealed partial class WebSocketGatewayTests
{
    [Fact]
    public async Task A_response_encoding_failure_returns_a_fault_and_the_session_stays_usable()
    {
        var token = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false);
        await using var provider = services.BuildServiceProvider();
        var protocol = new FailOneResponseProtocol();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());
        var running = session.RunAsync(token);
        _ = await socket.ReadSentAsync(token);
        socket.Enqueue(
            new JsonWebSocketHubProtocol().Encode(
                new HubFrame
                {
                    Kind = HubFrameKind.Request,
                    StreamId = Stream(91),
                    Operation = "missing",
                    Payload = new byte[] { 1 },
                }
            ),
            WebSocketMessageType.Text
        );
        var fault = await ReadFrameAsync(socket, protocol, token);
        Assert.Equal(HubFrameKind.Fault, fault.Kind);
        Assert.Equal(Stream(91), fault.StreamId);
        Assert.Equal(HubFaultCodes.RequestFailed, fault.Code);
        await SendUnregisteredRequestAndAwaitFaultAsync(socket, protocol, 92, token);
        socket.EnqueueClose();
        await running.WaitAsync(TimeSpan.FromSeconds(10), token);
    }

    [Fact]
    public async Task An_unexpected_writer_failure_aborts_and_ends_the_pending_receive()
    {
        var token = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false);
        await using var provider = services.BuildServiceProvider();
        using var socket = new ScriptedWebSocket { SendFailure = new IOException("send failed") };
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, new JsonWebSocketHubProtocol(), new ClaimsPrincipal());
        await session.RunAsync(token).WaitAsync(TimeSpan.FromSeconds(10), token);
        Assert.Equal(WebSocketState.Aborted, socket.State);
    }

    private sealed class FailOneResponseProtocol : IWebSocketHubProtocol
    {
        private readonly JsonWebSocketHubProtocol _inner = new();
        private bool _failed;
        public string SubProtocol => _inner.SubProtocol;
        public WebSocketMessageType MessageType => _inner.MessageType;

        public HubFrame Decode(ReadOnlySpan<byte> payload) => _inner.Decode(payload);

        public byte[] Encode(HubFrame frame)
        {
            if (!_failed && frame.Kind == HubFrameKind.Fault)
            {
                _failed = true;
                throw new InvalidOperationException("encoding failed");
            }
            return _inner.Encode(frame);
        }
    }
}
