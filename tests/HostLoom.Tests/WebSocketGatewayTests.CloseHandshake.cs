using System.Buffers;
using System.Diagnostics;
using System.Net;
using System.Net.Sockets;
using System.Net.WebSockets;
using System.Security.Claims;
using HostLoom.AspNetCore.WebSockets;
using HostLoom.Transport.InMemory;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Server-initiated closes over the runtime's own WebSocket implementation, on both ends of a
/// loopback TCP connection or behind Kestrel. TestServer's socket and the scripted fake neither
/// abort when a pending receive is cancelled nor enforce the close handshake, so only a real
/// socket shows whether a client receives the close frame or an abnormal closure.
/// </summary>
public sealed partial class WebSocketGatewayTests
{
    private static readonly TimeSpan RealSocketBound = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Session_expiry_reaches_a_real_client_as_a_close_frame()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        await using var provider = BuildClockedGateway(
            clock,
            options => options.MaximumSessionLifetime = TimeSpan.FromMinutes(1)
        );
        await using var connection = await LoopbackSession.StartAsync(
            provider,
            new ClaimsPrincipal(),
            cancellationToken
        );

        clock.Advance(TimeSpan.FromMinutes(1));

        var close = await ReceiveCloseAsync(connection.Client, cancellationToken);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, close.Status);
        Assert.Equal("session_expired", close.Description);
        Assert.Equal(
            WebSocketState.Closed,
            await AnswerCloseAsync(connection, close, cancellationToken)
        );
        Assert.Equal(0, provider.GetRequiredService<IWebSocketSessionDirectory>().Count);
        Assert.Equal(0, clock.PendingTimers);
    }

    [Fact]
    public async Task Administrative_disconnects_reach_real_clients_as_close_frames()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var provider = BuildClockedGateway(new TestClock());
        var user = new ClaimsPrincipal(
            new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, "subject-1")], "test")
        );
        await using var first = await LoopbackSession.StartAsync(provider, user, cancellationToken);
        await using var second = await LoopbackSession.StartAsync(
            provider,
            user,
            cancellationToken
        );
        var control = provider.GetRequiredService<IWebSocketSessionControl>();

        var logout = control.DisconnectAsync(first.Session.Id, "logout", cancellationToken);
        var firstClose = await ReceiveCloseAsync(first.Client, cancellationToken);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, firstClose.Status);
        Assert.Equal("logout", firstClose.Description);
        Assert.Equal(
            WebSocketState.Closed,
            await AnswerCloseAsync(first, firstClose, cancellationToken)
        );
        Assert.True(await logout.AsTask().WaitAsync(RealSocketBound, cancellationToken));

        var revocation = control.DisconnectSubjectAsync(
            "subject-1",
            "roles_changed",
            cancellationToken
        );
        var secondClose = await ReceiveCloseAsync(second.Client, cancellationToken);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, secondClose.Status);
        Assert.Equal("roles_changed", secondClose.Description);
        Assert.Equal(
            WebSocketState.Closed,
            await AnswerCloseAsync(second, secondClose, cancellationToken)
        );
        Assert.Equal(1, await revocation.AsTask().WaitAsync(RealSocketBound, cancellationToken));
    }

    [Fact]
    public async Task Server_shutdown_reaches_a_real_client_as_a_close_frame()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var provider = BuildClockedGateway(new TestClock());
        await using var connection = await LoopbackSession.StartAsync(
            provider,
            new ClaimsPrincipal(),
            cancellationToken
        );
        var shutdown = provider
            .GetServices<IHostedService>()
            .OfType<WebSocketSessionShutdownService>()
            .Single();

        await shutdown.StoppingAsync(cancellationToken);
        var close = await ReceiveCloseAsync(connection.Client, cancellationToken);
        Assert.Equal(WebSocketCloseStatus.EndpointUnavailable, close.Status);
        Assert.Equal("server_shutdown", close.Description);

        var stop = shutdown.StopAsync(cancellationToken);
        Assert.Equal(
            WebSocketState.Closed,
            await AnswerCloseAsync(connection, close, cancellationToken)
        );
        await stop.WaitAsync(RealSocketBound, cancellationToken);
    }

    [Fact]
    public async Task Session_accepted_after_shutdown_began_is_closed_without_a_welcome()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var provider = BuildClockedGateway(new TestClock());
        var shutdown = provider
            .GetServices<IHostedService>()
            .OfType<WebSocketSessionShutdownService>()
            .Single();
        await shutdown.StoppingAsync(cancellationToken);

        // The web server keeps accepting upgrades until it stops itself, and then it waits for
        // them; a session that arrives in between must not hold that wait open.
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, new JsonWebSocketHubProtocol(), new ClaimsPrincipal());
        await session.RunAsync(cancellationToken).WaitAsync(RealSocketBound, cancellationToken);

        Assert.Equal(WebSocketCloseStatus.EndpointUnavailable, socket.CloseStatus);
        Assert.Equal("server_shutdown", socket.CloseStatusDescription);
        Assert.False(socket.TryReadSent(out _));
        await shutdown.StopAsync(cancellationToken).WaitAsync(RealSocketBound, cancellationToken);
    }

    [Fact]
    public async Task Rate_limited_session_reaches_a_real_client_as_a_close_frame()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var provider = BuildClockedGateway(
            new TestClock(),
            options => options.MaximumControlFramesPerSecond = 2
        );
        await using var connection = await LoopbackSession.StartAsync(
            provider,
            new ClaimsPrincipal(),
            cancellationToken
        );

        for (var stream = 1; stream <= 3; stream++)
        {
            await SendRealFrameAsync(
                connection,
                new HubFrame { Kind = HubFrameKind.Ping, StreamId = Stream(stream) },
                cancellationToken
            );
        }

        var close = await ReceiveCloseAsync(connection.Client, cancellationToken);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, close.Status);
        Assert.Equal("rate_limited", close.Description);
        Assert.Equal(
            WebSocketState.Closed,
            await AnswerCloseAsync(connection, close, cancellationToken)
        );
    }

    [Fact]
    public async Task Events_published_while_a_close_is_pending_do_not_abort_the_handshake()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var provider = BuildClockedGateway(
            new TestClock(),
            gateway: builder =>
                builder.AddTopic<OrderChanged>(
                    "orders.changed",
                    "orders",
                    value => value.CustomerId
                )
        );
        await using var connection = await LoopbackSession.StartAsync(
            provider,
            new ClaimsPrincipal(),
            cancellationToken
        );
        await SendRealFrameAsync(
            connection,
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(1),
                Topic = "orders.changed",
                Key = "customer-1",
                Credit = 8,
            },
            cancellationToken
        );
        var subscribed = await ReceiveRealFrameAsync(
            connection.Client,
            connection.Protocol,
            cancellationToken
        );
        Assert.Equal(HubFrameKind.Subscribed, subscribed.Kind);

        var disconnect = provider
            .GetRequiredService<IWebSocketSessionControl>()
            .DisconnectAsync(connection.Session.Id, "logout", cancellationToken)
            .AsTask();
        var close = await ReceiveCloseAsync(connection.Client, cancellationToken);

        // The subscription is still registered until teardown, and the outbound queue already
        // refuses frames. A refusal reported to the publisher would make it abort the session.
        var registry = provider.GetRequiredService<WebSocketSessionRegistry>();
        for (var sequence = 0; sequence < 4; sequence++)
        {
            registry.Publish("orders.changed", "customer-1", new byte[] { 1 });
        }

        Assert.Equal(
            WebSocketState.Closed,
            await AnswerCloseAsync(connection, close, cancellationToken)
        );
        Assert.True(await disconnect.WaitAsync(RealSocketBound, cancellationToken));
    }

    [Fact]
    public async Task Frames_sent_after_a_protocol_violation_are_discarded_until_the_client_answers()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        await using var provider = BuildClockedGateway(new TestClock());
        await using var connection = await LoopbackSession.StartAsync(
            provider,
            new ClaimsPrincipal(),
            cancellationToken
        );

        // JSON is a text subprotocol, so a binary message is a violation; the ping behind it
        // arrives after the server has started closing and must not be answered.
        await connection
            .Client.SendAsync(
                new byte[] { 1 }.AsMemory(),
                WebSocketMessageType.Binary,
                true,
                cancellationToken
            )
            .AsTask()
            .WaitAsync(RealSocketBound, cancellationToken);
        await SendRealFrameAsync(
            connection,
            new HubFrame { Kind = HubFrameKind.Ping, StreamId = Stream(1) },
            cancellationToken
        );

        var buffer = new byte[4096];
        var next = await connection
            .Client.ReceiveAsync(buffer.AsMemory(), cancellationToken)
            .AsTask()
            .WaitAsync(RealSocketBound, cancellationToken);
        Assert.Equal(WebSocketMessageType.Close, next.MessageType);
        var close = (connection.Client.CloseStatus, connection.Client.CloseStatusDescription);
        Assert.Equal(WebSocketCloseStatus.InvalidMessageType, close.CloseStatus);
        Assert.Equal(
            WebSocketState.Closed,
            await AnswerCloseAsync(connection, close, cancellationToken)
        );
    }

    [Theory]
    [InlineData(MalformedProtobuf.NegativeLengthOfAnUnknownField)]
    [InlineData(MalformedProtobuf.NegativeLengthOfAKnownField)]
    [InlineData(MalformedProtobuf.ImplausibleLengthOfAKnownField)]
    [InlineData(MalformedProtobuf.GroupsNestedBeyondTheDepthLimit)]
    public async Task Malformed_protobuf_frame_closes_a_real_client_with_invalid_payload(
        MalformedProtobuf shape
    )
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var logs = new WebSocketLogRecorder();
        await using var provider = BuildClockedGateway(new TestClock(), logs: logs);
        await using var connection = await LoopbackSession.StartAsync(
            provider,
            new ClaimsPrincipal(),
            new ProtobufWebSocketHubProtocol(),
            cancellationToken
        );

        await connection
            .Client.SendAsync(
                MalformedProtobufFrame(shape).AsMemory(),
                WebSocketMessageType.Binary,
                true,
                cancellationToken
            )
            .AsTask()
            .WaitAsync(RealSocketBound, cancellationToken);

        var close = await ReceiveCloseAsync(connection.Client, cancellationToken);
        Assert.Equal(WebSocketCloseStatus.InvalidPayloadData, close.Status);
        Assert.Equal(
            WebSocketState.Closed,
            await AnswerCloseAsync(connection, close, cancellationToken)
        );
        Assert.DoesNotContain(logs.Entries, entry => entry.Level >= LogLevel.Error);
        var closed = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SessionClosed
        );
        Assert.Equal("invalid_payload", closed.Property("CloseReason"));
    }

    [Fact]
    public async Task Unexpected_failure_closes_a_real_client_with_internal_error()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var logs = new WebSocketLogRecorder();
        await using var provider = BuildClockedGateway(new TestClock(), logs: logs);
        await using var connection = await LoopbackSession.StartAsync(
            provider,
            new ClaimsPrincipal(),
            new PingFailingProtocol(),
            cancellationToken
        );

        await SendRealFrameAsync(
            connection,
            new HubFrame { Kind = HubFrameKind.Ping, StreamId = Stream(1) },
            cancellationToken
        );

        var close = await ReceiveCloseAsync(connection.Client, cancellationToken);
        Assert.Equal(WebSocketCloseStatus.InternalServerError, close.Status);
        Assert.Equal("internal_error", close.Description);

        // The session answers through the close handshake and ends normally; the exception must
        // not escape to the server as an unhandled request failure.
        Assert.Equal(
            WebSocketState.Closed,
            await AnswerCloseAsync(connection, close, cancellationToken)
        );
        Assert.True(connection.Run.IsCompletedSuccessfully);
        var failed = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SessionFailed
        );
        Assert.Equal(LogLevel.Error, failed.Level);
        Assert.Equal(connection.Session.Id, failed.Property("SessionId"));
        var closed = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SessionClosed
        );
        Assert.Equal("internal_error", closed.Property("CloseReason"));
        Assert.Equal(WebSocketCloseStatus.InternalServerError, closed.Property("CloseStatus"));
    }

    [Fact]
    public async Task Unexpected_receive_failure_still_closes_with_internal_error()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var logs = new WebSocketLogRecorder();
        await using var provider = BuildClockedGateway(new TestClock(), logs: logs);
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, new JsonWebSocketHubProtocol(), new ClaimsPrincipal());

        // A failure outside frame processing leaves no receive loop to finish the handshake, so
        // the close frame is the last thing the session can still give the peer.
        socket.EnqueueReceiveFailure(new InvalidOperationException("The transport has a defect."));
        await session.RunAsync(cancellationToken).WaitAsync(RealSocketBound, cancellationToken);

        Assert.Equal(WebSocketCloseStatus.InternalServerError, socket.CloseStatus);
        Assert.Equal("internal_error", socket.CloseStatusDescription);
        Assert.Single(logs.Entries, entry => entry.Event == WebSocketEvents.SessionFailed);
        Assert.Equal(0, provider.GetRequiredService<IWebSocketSessionDirectory>().Count);
    }

    [Fact]
    public async Task Unanswered_close_is_aborted_when_the_close_timeout_elapses()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        using var logs = new WebSocketLogRecorder();
        await using var provider = BuildClockedGateway(
            clock,
            options => options.CloseTimeout = TimeSpan.FromSeconds(5),
            logs
        );
        await using var connection = await LoopbackSession.StartAsync(
            provider,
            new ClaimsPrincipal(),
            cancellationToken
        );

        var disconnect = provider
            .GetRequiredService<IWebSocketSessionControl>()
            .DisconnectAsync(connection.Session.Id, "logout", cancellationToken)
            .AsTask();
        var close = await ReceiveCloseAsync(connection.Client, cancellationToken);
        Assert.Equal("logout", close.Description);

        // The client has the close frame and never answers. Only the close timeout, measured by
        // the injected clock, may end the session now.
        Assert.False(connection.Run.IsCompleted);
        Assert.False(disconnect.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(5));

        Assert.True(await disconnect.WaitAsync(RealSocketBound, cancellationToken));
        await connection.Run.WaitAsync(RealSocketBound, cancellationToken);
        Assert.Equal(WebSocketState.Aborted, connection.Server.State);
        Assert.Equal(0, clock.PendingTimers);
        var timedOut = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.CloseTimedOut
        );
        Assert.Equal(LogLevel.Warning, timedOut.Level);
        Assert.Equal(connection.Session.Id, timedOut.Property("SessionId"));
        Assert.Equal(5000d, timedOut.Property("TimeoutMilliseconds"));
        var closed = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SessionClosed
        );
        Assert.Equal("aborted", closed.Property("CloseReason"));
    }

    [Fact]
    public async Task Stopping_a_web_application_closes_sessions_before_the_server_drains()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var shutdownTimeout = TimeSpan.FromSeconds(30);
        var protocol = new JsonWebSocketHubProtocol();
        var builder = WebApplication.CreateSlimBuilder();
        builder.Logging.ClearProviders();
        builder.WebHost.UseKestrel(options => options.Listen(IPAddress.Loopback, 0));
        builder.Services.Configure<HostOptions>(options =>
            options.ShutdownTimeout = shutdownTimeout
        );
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false);
        await using var application = builder.Build();
        application.UseHostLoomWebSockets();
        application.MapHostLoomWebSocketHub("/hostloom");
        await application.StartAsync(cancellationToken);

        using var client = new ClientWebSocket();
        client.Options.AddSubProtocol(protocol.SubProtocol);
        var endpoint = new UriBuilder(application.Urls.Single())
        {
            Scheme = "ws",
            Path = "/hostloom",
        }.Uri;
        await client
            .ConnectAsync(endpoint, cancellationToken)
            .WaitAsync(RealSocketBound, cancellationToken);
        var welcome = await ReceiveRealFrameAsync(client, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Welcome, welcome.Kind);

        // A WebApplication registers its web server last, so the server stops first and waits
        // for every upgraded request. The client must hear 1001 before that wait, not after it.
        var stopwatch = Stopwatch.StartNew();
        var stop = application.StopAsync(cancellationToken);
        var close = await ReceiveCloseAsync(client, cancellationToken);
        Assert.Equal(WebSocketCloseStatus.EndpointUnavailable, close.Status);
        Assert.Equal("server_shutdown", close.Description);
        await client
            .CloseOutputAsync(close.Status!.Value, close.Description, cancellationToken)
            .WaitAsync(RealSocketBound, cancellationToken);
        await stop.WaitAsync(RealSocketBound, cancellationToken);
        stopwatch.Stop();

        Assert.True(
            stopwatch.Elapsed < shutdownTimeout / 3,
            $"Stopping took {stopwatch.Elapsed} against a {shutdownTimeout} shutdown timeout."
        );
    }

    private static ServiceProvider BuildClockedGateway(
        TestClock clock,
        Action<HostLoomWebSocketOptions>? configure = null,
        WebSocketLogRecorder? logs = null,
        Action<HostLoomWebSocketBuilder>? gateway = null
    )
    {
        var services = new ServiceCollection();
        if (logs is not null)
        {
            services.AddLogging(builder => builder.AddProvider(logs));
        }

        services.AddSingleton<TimeProvider>(clock);
        var builder = services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                configure?.Invoke(options);
            });
        gateway?.Invoke(builder);
        return services.BuildServiceProvider();
    }

    private static Task SendRealFrameAsync(
        LoopbackSession connection,
        HubFrame frame,
        CancellationToken cancellationToken
    ) =>
        connection
            .Client.SendAsync(
                connection.Protocol.Encode(frame).AsMemory(),
                connection.Protocol.MessageType,
                true,
                cancellationToken
            )
            .AsTask()
            .WaitAsync(RealSocketBound, cancellationToken);

    private static async Task<HubFrame> ReceiveRealFrameAsync(
        WebSocket socket,
        IWebSocketHubProtocol protocol,
        CancellationToken cancellationToken
    )
    {
        var payload = new ArrayBufferWriter<byte>();
        while (true)
        {
            var result = await socket
                .ReceiveAsync(payload.GetMemory(4096), cancellationToken)
                .AsTask()
                .WaitAsync(RealSocketBound, cancellationToken);
            Assert.NotEqual(WebSocketMessageType.Close, result.MessageType);
            payload.Advance(result.Count);
            if (result.EndOfMessage)
            {
                return protocol.Decode(payload.WrittenSpan);
            }
        }
    }

    /// <summary>
    /// Reads past any application frames to the close frame. A socket the server aborted fails
    /// here with the runtime's premature-close exception instead.
    /// </summary>
    private static async Task<(
        WebSocketCloseStatus? Status,
        string? Description
    )> ReceiveCloseAsync(WebSocket socket, CancellationToken cancellationToken)
    {
        var buffer = new byte[4096];
        while (true)
        {
            var result = await socket
                .ReceiveAsync(buffer.AsMemory(), cancellationToken)
                .AsTask()
                .WaitAsync(RealSocketBound, cancellationToken);
            if (result.MessageType is WebSocketMessageType.Close)
            {
                return (socket.CloseStatus, socket.CloseStatusDescription);
            }
        }
    }

    public enum MalformedProtobuf
    {
        NegativeLengthOfAnUnknownField,
        NegativeLengthOfAKnownField,
        ImplausibleLengthOfAKnownField,
        GroupsNestedBeyondTheDepthLimit,
    }

    /// <summary>
    /// Frames protobuf-net rejects with <see cref="InvalidOperationException"/> rather than a
    /// <c>ProtoException</c>. Field 20 is not part of the frame contract, so the reader skips it;
    /// field 4 is <c>operation</c> and field 13 is <c>payload</c>.
    /// </summary>
    private static byte[] MalformedProtobufFrame(MalformedProtobuf shape) =>
        shape switch
        {
            // Field 20, length-delimited, with -1 as a ten-byte varint.
            MalformedProtobuf.NegativeLengthOfAnUnknownField => Convert.FromHexString(
                "A201" + "FFFFFFFFFFFFFFFFFF01"
            ),

            // Field 4, length-delimited, with -1 as a five-byte varint.
            MalformedProtobuf.NegativeLengthOfAKnownField => Convert.FromHexString(
                "22" + "FFFFFFFF0F"
            ),

            // Field 13 claims int.MaxValue bytes and supplies none.
            MalformedProtobuf.ImplausibleLengthOfAKnownField => Convert.FromHexString(
                "6A" + "FFFFFFFF07"
            ),

            // 600 start-group tags for field 20, past protobuf-net's nesting limit of 512.
            MalformedProtobuf.GroupsNestedBeyondTheDepthLimit => Convert.FromHexString(
                string.Concat(Enumerable.Repeat("A301", 600))
            ),
            _ => throw new ArgumentOutOfRangeException(nameof(shape)),
        };

    /// <summary>
    /// JSON with a decoder that fails on <c>ping</c> the way a defect would, with an exception
    /// other than <see cref="InvalidDataException"/>. Server frames still decode, so the test
    /// client can read the welcome.
    /// </summary>
    private sealed class PingFailingProtocol : IWebSocketHubProtocol
    {
        private readonly JsonWebSocketHubProtocol _json = new();

        public string SubProtocol => _json.SubProtocol;

        public WebSocketMessageType MessageType => _json.MessageType;

        public HubFrame Decode(ReadOnlySpan<byte> payload)
        {
            var frame = _json.Decode(payload);
            return frame.Kind is HubFrameKind.Ping
                ? throw new InvalidOperationException("The codec has a defect.")
                : frame;
        }

        public byte[] Encode(HubFrame frame) => _json.Encode(frame);
    }

    /// <summary>
    /// Answers the server's close frame as a compliant client does and returns the server socket's
    /// state when the session ended: <c>Closed</c> after a completed handshake, <c>Aborted</c>
    /// otherwise.
    /// </summary>
    private static async Task<WebSocketState> AnswerCloseAsync(
        LoopbackSession connection,
        (WebSocketCloseStatus? Status, string? Description) close,
        CancellationToken cancellationToken
    )
    {
        // A client socket waits for the server to drop the connection after the handshake, which
        // the endpoint does by disposing the server socket once the session ends.
        var answer = connection.Client.CloseOutputAsync(
            close.Status!.Value,
            close.Description,
            cancellationToken
        );
        await connection.Run.WaitAsync(RealSocketBound, cancellationToken);
        var serverState = connection.Server.State;
        connection.ReleaseServer();
        await answer.WaitAsync(RealSocketBound, cancellationToken);
        return serverState;
    }

    /// <summary>
    /// One gateway session whose server and client are both the runtime's WebSocket over a
    /// loopback TCP connection, with keep-alive disabled so every frame on the wire is the test's.
    /// </summary>
    private sealed class LoopbackSession : IAsyncDisposable
    {
        private LoopbackSession(
            WebSocket server,
            WebSocket client,
            IWebSocketHubProtocol protocol,
            WebSocketSession session,
            Task run
        )
        {
            Server = server;
            Client = client;
            Protocol = protocol;
            Session = session;
            Run = run;
        }

        public WebSocket Server { get; }

        public WebSocket Client { get; }

        public IWebSocketHubProtocol Protocol { get; }

        public WebSocketSession Session { get; }

        public Task Run { get; }

        public static Task<LoopbackSession> StartAsync(
            IServiceProvider services,
            ClaimsPrincipal user,
            CancellationToken cancellationToken
        ) => StartAsync(services, user, new JsonWebSocketHubProtocol(), cancellationToken);

        public static async Task<LoopbackSession> StartAsync(
            IServiceProvider services,
            ClaimsPrincipal user,
            IWebSocketHubProtocol protocol,
            CancellationToken cancellationToken
        )
        {
            using var listener = new TcpListener(IPAddress.Loopback, 0);
            listener.Start();
            var accepted = listener.AcceptSocketAsync(cancellationToken).AsTask();
            // CA2000: each socket moves to a stream that owns it, and the returned session disposes
            // both streams through their WebSockets.
#pragma warning disable CA2000
            var clientSocket = new Socket(
                AddressFamily.InterNetwork,
                SocketType.Stream,
                ProtocolType.Tcp
            );
#pragma warning restore CA2000
            await clientSocket
                .ConnectAsync((IPEndPoint)listener.LocalEndpoint, cancellationToken)
                .AsTask()
                .WaitAsync(RealSocketBound, cancellationToken);
            var serverSocket = await accepted.WaitAsync(RealSocketBound, cancellationToken);

            var server = WebSocket.CreateFromStream(
                new NetworkStream(serverSocket, ownsSocket: true),
                new WebSocketCreationOptions
                {
                    IsServer = true,
                    SubProtocol = protocol.SubProtocol,
                    KeepAliveInterval = TimeSpan.Zero,
                }
            );
            var client = WebSocket.CreateFromStream(
                new NetworkStream(clientSocket, ownsSocket: true),
                new WebSocketCreationOptions
                {
                    IsServer = false,
                    SubProtocol = protocol.SubProtocol,
                    KeepAliveInterval = TimeSpan.Zero,
                }
            );
            var session = services
                .GetRequiredService<WebSocketSessionFactory>()
                .Create(server, protocol, user);
            var connection = new LoopbackSession(
                server,
                client,
                protocol,
                session,
                session.RunAsync(cancellationToken)
            );

            var welcome = await ReceiveRealFrameAsync(client, protocol, cancellationToken);
            Assert.Equal(HubFrameKind.Welcome, welcome.Kind);
            return connection;
        }

        /// <summary>Disposes the server socket, as the endpoint does when the session returns.</summary>
        public void ReleaseServer() => Server.Dispose();

        public async ValueTask DisposeAsync()
        {
            Client.Dispose();
            Server.Dispose();
            await Run.WaitAsync(RealSocketBound)
                .ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
        }
    }
}
