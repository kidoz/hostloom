using HostLoom.AspNetCore.WebSockets;
using HostLoom.AspNetCore.WebSockets.Testing;
using HostLoom.Transport.InMemory;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Controlled faults against live gateway sessions: a client that vanishes with a request in
/// flight, a client that reconnects after missing events, and a dead session sharing a topic with
/// a live one. The hypothesis is the gateway's stated lifetime contract: a dropped connection
/// ends its session's work rather than replaying it, a reconnect is a new session with no memory
/// and no backlog, and one dead session neither fails a publisher nor starves the sessions beside
/// it.
/// </summary>
public sealed class WebSocketChaosTests
{
    private static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);
    private static readonly Uri Endpoint = new("ws://localhost/hostloom");

    [Fact]
    public async Task A_client_that_drops_mid_request_is_never_answered_and_never_replayed()
    {
        var token = TestContext.Current.CancellationToken;
        var gate = new RequestGate();
        using var host = await StartGatewayAsync(gate);
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();

        var client = new WebSocketTestClient(host.GetTestServer());
        await client.ConnectAsync(Endpoint, token);
        _ = await client.AwaitWelcomeAsync(token);
        await client.SendAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = Stream(1),
                Operation = "inventory.reserve",
                Payload = serializer.Serialize(new Reserve("R-1")),
            },
            token
        );
        await gate.Entered.Task.WaitAsync(Bounded, token);

        // Fault: the client disappears without a close handshake while the handler is still
        // working. The answer it was waiting for has nowhere to go.
        client.Socket.Abort();
        await client.DisposeAsync();
        gate.Release.SetResult();

        // The dropped request is not handed to the handler a second time, and the gateway is
        // still serving: a fresh session is answered normally.
        await using var reconnected = new WebSocketTestClient(host.GetTestServer());
        await reconnected.ConnectAsync(Endpoint, token);
        _ = await reconnected.AwaitWelcomeAsync(token);
        await reconnected.SendAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = Stream(2),
                Operation = "inventory.reserve",
                Payload = serializer.Serialize(new Reserve("R-2")),
            },
            token
        );

        var response = await reconnected.ReceiveAsync(
            HubFrameKind.Response,
            cancellationToken: token
        );
        Assert.Equal(Stream(2), response.StreamId);
        Assert.Equal(
            "R-2",
            serializer.Deserialize<Reserved>(response.Payload!.Value.Span)!.Reference
        );
        Assert.Equal(2, gate.Started);
        Assert.Equal(["R-1", "R-2"], gate.Handled);
    }

    [Fact]
    public async Task A_reconnected_client_receives_nothing_that_happened_while_it_was_away()
    {
        var token = TestContext.Current.CancellationToken;
        using var host = await StartGatewayAsync();
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();
        var events = host.Services.GetRequiredService<IPublishEndpoint>();

        var client = new WebSocketTestClient(host.GetTestServer());
        await client.ConnectAsync(Endpoint, token);
        _ = await client.AwaitWelcomeAsync(token);
        await SubscribeAsync(client, Stream(3), token);
        await events.PublishAsync("orders", new OrderChanged("customer-1", "E-1"), token);
        Assert.Equal("E-1", await NextReferenceAsync(client, Stream(3), serializer, token));

        // Fault: the connection is gone, and the stream keeps moving without it.
        client.Socket.Abort();
        await client.DisposeAsync();
        await events.PublishAsync("orders", new OrderChanged("customer-1", "E-2"), token);
        await events.PublishAsync("orders", new OrderChanged("customer-1", "E-3"), token);

        // Recovery: a reconnect is a new session. It resubscribes and sees what happens from
        // then on; the two events it missed are not replayed to it, and it is not told they
        // existed. Anything that must survive a reconnect belongs in a snapshot, not a buffer.
        await using var reconnected = new WebSocketTestClient(host.GetTestServer());
        await reconnected.ConnectAsync(Endpoint, token);
        _ = await reconnected.AwaitWelcomeAsync(token);
        await SubscribeAsync(reconnected, Stream(4), token);
        await events.PublishAsync("orders", new OrderChanged("customer-1", "E-4"), token);

        Assert.Equal("E-4", await NextReferenceAsync(reconnected, Stream(4), serializer, token));
    }

    [Fact]
    public async Task A_dead_session_neither_fails_the_publisher_nor_starves_the_live_one()
    {
        var token = TestContext.Current.CancellationToken;
        using var host = await StartGatewayAsync();
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();
        var events = host.Services.GetRequiredService<IPublishEndpoint>();

        var dying = new WebSocketTestClient(host.GetTestServer());
        await dying.ConnectAsync(Endpoint, token);
        _ = await dying.AwaitWelcomeAsync(token);
        await SubscribeAsync(dying, Stream(5), token);

        await using var live = new WebSocketTestClient(host.GetTestServer());
        await live.ConnectAsync(Endpoint, token);
        _ = await live.AwaitWelcomeAsync(token);
        await SubscribeAsync(live, Stream(6), token);

        // Fault: one of the two subscribers on the topic dies mid-stream.
        dying.Socket.Abort();
        await dying.DisposeAsync();

        // The publisher is not the place a dead subscriber surfaces, and the session beside it
        // keeps its place in the stream.
        await events.PublishAsync("orders", new OrderChanged("customer-1", "E-1"), token);
        await events.PublishAsync("orders", new OrderChanged("customer-1", "E-2"), token);

        Assert.Equal("E-1", await NextReferenceAsync(live, Stream(6), serializer, token));
        Assert.Equal("E-2", await NextReferenceAsync(live, Stream(6), serializer, token));
    }

    private static async Task SubscribeAsync(
        WebSocketTestClient client,
        Guid stream,
        CancellationToken cancellationToken
    )
    {
        await client.SendAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = stream,
                Topic = "orders.changed",
                Key = "customer-1",
                Credit = 16,
            },
            cancellationToken
        );
        _ = await client.AwaitSubscribedAsync(stream, cancellationToken);
    }

    private static async Task<string?> NextReferenceAsync(
        WebSocketTestClient client,
        Guid stream,
        IMessageSerializer serializer,
        CancellationToken cancellationToken
    )
    {
        var frame = await client.AwaitEventAsync(stream, cancellationToken);
        return serializer.Deserialize<OrderChanged>(frame.Payload!.Value.Span)?.Reference;
    }

    private static Guid Stream(int value) => new(value, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    private static async Task<IHost> StartGatewayAsync(RequestGate? gate = null)
    {
        var builder = new HostBuilder().ConfigureWebHost(webHost =>
            webHost
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddRouting();
                    services.AddSingleton(gate ?? new RequestGate());
                    services
                        .AddHostLoom()
                        .UseInMemory()
                        .AddHandler<Reserve, Reserved, GatedReserveHandler>("inventory")
                        .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
                        .AddRequest<Reserve, Reserved>("inventory.reserve", "inventory")
                        .AddTopic<OrderChanged>(
                            "orders.changed",
                            "orders",
                            value => value.CustomerId
                        );
                })
                .Configure(application =>
                {
                    application.UseHostLoomWebSockets();
                    application.UseRouting();
                    application.UseEndpoints(endpoints =>
                        endpoints.MapHostLoomWebSocketHub("/hostloom")
                    );
                })
        );

        return await builder.StartAsync(TestContext.Current.CancellationToken);
    }

    public sealed record Reserve(string Reference) : IRequest<Reserved>;

    public sealed record Reserved(string Reference);

    public sealed record OrderChanged(string CustomerId, string Reference) : IEvent;

    /// <summary>Holds the first request open so a test can cut the connection under it.</summary>
    public sealed class RequestGate
    {
        private readonly Lock _gate = new();
        private readonly List<string> _handled = [];

        public TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Started
        {
            get
            {
                lock (_gate)
                {
                    return _handled.Count;
                }
            }
        }

        public IReadOnlyList<string> Handled
        {
            get
            {
                lock (_gate)
                {
                    return [.. _handled];
                }
            }
        }

        public bool Enter(string reference)
        {
            lock (_gate)
            {
                _handled.Add(reference);
                if (_handled.Count > 1)
                {
                    return false;
                }
            }

            Entered.TrySetResult();
            return true;
        }
    }

    public sealed class GatedReserveHandler(RequestGate gate) : IRequestHandler<Reserve, Reserved>
    {
        public async ValueTask<Reserved> HandleAsync(
            Reserve request,
            CancellationToken cancellationToken
        )
        {
            if (gate.Enter(request.Reference))
            {
                await gate.Release.Task.WaitAsync(cancellationToken);
            }

            return new Reserved(request.Reference);
        }
    }
}
