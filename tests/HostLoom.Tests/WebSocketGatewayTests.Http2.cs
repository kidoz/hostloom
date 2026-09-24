using System.Net;
using System.Net.WebSockets;
using HostLoom.AspNetCore.WebSockets;
using HostLoom.Transport.InMemory;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Server.Kestrel.Core;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// WebSockets over HTTP/2 (RFC 8441) against a real Kestrel listener. The handshake is an extended
/// CONNECT rather than a GET upgrade, which TestServer does not exercise.
/// </summary>
public sealed partial class WebSocketGatewayTests
{
    [Fact]
    public async Task Http2_extended_connect_reaches_the_gateway_and_completes_a_session()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var protocol = new JsonWebSocketHubProtocol();
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();

        // HTTP/2 only, without TLS: the client must use prior knowledge, so the handshake cannot
        // silently fall back to an HTTP/1.1 upgrade.
        builder.WebHost.UseKestrel(options =>
            options.Listen(
                IPAddress.Loopback,
                0,
                listener => listener.Protocols = HttpProtocols.Http2
            )
        );
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false);
        await using var application = builder.Build();
        application.UseHostLoomWebSockets();
        application.MapHostLoomWebSocketHub("/hostloom");
        await application.StartAsync(cancellationToken);

        using var handler = new SocketsHttpHandler();
        using var invoker = new HttpMessageInvoker(handler);
        using var client = new ClientWebSocket();
        client.Options.HttpVersion = HttpVersion.Version20;
        client.Options.HttpVersionPolicy = HttpVersionPolicy.RequestVersionExact;
        client.Options.CollectHttpResponseDetails = true;
        client.Options.AddSubProtocol(protocol.SubProtocol);
        var endpoint = new UriBuilder(application.Urls.Single())
        {
            Scheme = "ws",
            Path = "/hostloom",
        }.Uri;
        await client
            .ConnectAsync(endpoint, invoker, cancellationToken)
            .WaitAsync(RealSocketBound, cancellationToken);

        // An HTTP/2 WebSocket is accepted with 200; an HTTP/1.1 upgrade would report 101.
        Assert.Equal(HttpStatusCode.OK, client.HttpStatusCode);
        Assert.Equal(protocol.SubProtocol, client.SubProtocol);
        var welcome = await ReceiveRealFrameAsync(client, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Welcome, welcome.Kind);

        await client
            .SendAsync(
                protocol
                    .Encode(new HubFrame { Kind = HubFrameKind.Ping, StreamId = Stream(1) })
                    .AsMemory(),
                protocol.MessageType,
                true,
                cancellationToken
            )
            .AsTask()
            .WaitAsync(RealSocketBound, cancellationToken);
        var pong = await ReceiveRealFrameAsync(client, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Pong, pong.Kind);
        Assert.Equal(Stream(1), pong.StreamId);

        await client
            .CloseAsync(WebSocketCloseStatus.NormalClosure, "done", cancellationToken)
            .WaitAsync(RealSocketBound, cancellationToken);
        Assert.Equal(WebSocketState.Closed, client.State);
        await application
            .StopAsync(cancellationToken)
            .WaitAsync(RealSocketBound, cancellationToken);
    }
}
