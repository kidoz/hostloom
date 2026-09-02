using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using HostLoom.AspNetCore.WebSockets;
using HostLoom.Transport.InMemory;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class WebSocketGatewayTests
{
    [Fact]
    public void Json_protocol_round_trips_the_common_frame()
    {
        var protocol = new JsonWebSocketHubProtocol();
        AssertRoundTrip(protocol);
        Assert.Equal(WebSocketMessageType.Text, protocol.MessageType);
    }

    [Theory]
    [InlineData("welcome")]
    [InlineData("subscribed")]
    [InlineData("event")]
    [InlineData("fault")]
    public void Json_v1_protocol_matches_published_fixtures(string fixture)
    {
        AssertFixture(new JsonWebSocketHubProtocol(), "json-v1", fixture);
    }

    [Theory]
    [InlineData("hostloom-websocket-json-v1.schema.json")]
    public void Published_json_schema_is_well_formed(string schema)
    {
        using var document = JsonDocument.Parse(File.ReadAllBytes(ProtocolFile(schema)));
        Assert.Equal(JsonValueKind.Object, document.RootElement.ValueKind);
    }

    [Theory]
    [InlineData("{\"kind\":1,\"streamId\":1}")]
    [InlineData("{\"kind\":\"Unknown\",\"streamId\":1}")]
    [InlineData("{\"kind\":\"None\",\"streamId\":1}")]
    public void Json_protocols_reject_non_contract_frame_kinds(string json)
    {
        var payload = Encoding.UTF8.GetBytes(json);
        _ = Assert.Throws<InvalidDataException>(() =>
            new JsonWebSocketHubProtocol().Decode(payload)
        );
    }

    [Fact]
    public void Json_protocol_reads_legacy_kind_casing_case_insensitively()
    {
        var frame = new JsonWebSocketHubProtocol().Decode(
            "{\"kind\":\"Welcome\",\"streamId\":0}"u8
        );

        Assert.Equal(HubFrameKind.Welcome, frame.Kind);
    }

    [Fact]
    public void MessagePack_protocol_round_trips_the_common_frame()
    {
        var protocol = new MessagePackWebSocketHubProtocol();
        AssertRoundTrip(protocol);
        Assert.Equal(WebSocketMessageType.Binary, protocol.MessageType);
    }

    [Fact]
    public void Protobuf_protocol_round_trips_the_common_frame()
    {
        var protocol = new ProtobufWebSocketHubProtocol();
        AssertRoundTrip(protocol);
        Assert.Equal(WebSocketMessageType.Binary, protocol.MessageType);
    }

    [Fact]
    public void Gateway_registers_all_builtin_protocols()
    {
        var services = new ServiceCollection();
        services.AddHostLoom().UseInMemory().AddWebSocketGateway();
        using var provider = services.BuildServiceProvider();

        var protocols = provider
            .GetServices<IWebSocketHubProtocol>()
            .Select(static protocol => protocol.SubProtocol)
            .Order(StringComparer.Ordinal)
            .ToArray();

        Assert.Equal(
            [
                JsonWebSocketHubProtocol.ProtocolName,
                MessagePackWebSocketHubProtocol.ProtocolName,
                ProtobufWebSocketHubProtocol.ProtocolName,
            ],
            protocols
        );
    }

    [Fact]
    public void Protobuf_protocol_rejects_a_truncated_frame()
    {
        var protocol = new ProtobufWebSocketHubProtocol();

        _ = Assert.Throws<InvalidDataException>(() => protocol.Decode(new byte[] { 0x80 }));
    }

    [Fact]
    public async Task Registered_request_is_dispatched_through_HostLoom()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddRequest<Greet, Greeting>("greet", "greeter");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();
        var router = host.Services.GetRequiredService<WebSocketRequestRouter>();

        var response = await router.RouteAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = 17,
                Operation = "greet",
                Payload = serializer.Serialize(new Greet("Ada")),
            },
            new ClaimsPrincipal(),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(HubFrameKind.Response, response.Kind);
        Assert.Equal((ulong)17, response.StreamId);
        var greeting = serializer.Deserialize<Greeting>(response.Payload!.Value.Span);
        Assert.Equal("Hello, Ada!", greeting?.Text);
    }

    [Fact]
    public async Task Malformed_request_payload_returns_an_invalid_payload_fault()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddRequest<Greet, Greeting>("greet", "greeter");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var router = host.Services.GetRequiredService<WebSocketRequestRouter>();

        var response = await router.RouteAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = 18,
                Operation = "greet",
                Payload = "not-json"u8.ToArray(),
            },
            new ClaimsPrincipal(),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(HubFrameKind.Fault, response.Kind);
        Assert.Equal(HubFaultCodes.InvalidPayload, response.Code);
    }

    [Fact]
    public async Task Operation_policy_is_checked_for_each_request()
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddAuthorization(options =>
            options.AddPolicy("operators", policy => policy.RequireClaim("role", "operator"))
        );
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddRequest<Greet, Greeting>("greet", "greeter", "operators");

        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();
        var router = host.Services.GetRequiredService<WebSocketRequestRouter>();

        var response = await router.RouteAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = 3,
                Operation = "greet",
                Payload = serializer.Serialize(new Greet("Ada")),
            },
            new ClaimsPrincipal(new ClaimsIdentity()),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(HubFrameKind.Fault, response.Kind);
        Assert.Equal(HubFaultCodes.Forbidden, response.Code);
    }

    [Fact]
    public void Registry_routes_keyed_events_to_wildcard_and_matching_subscribers()
    {
        var registry = new WebSocketSessionRegistry();
        var wildcard = new RecordingSink("wildcard");
        var matching = new RecordingSink("matching");
        var other = new RecordingSink("other");
        registry.Subscribe(wildcard, "orders", null);
        registry.Subscribe(matching, "orders", "customer-1");
        registry.Subscribe(other, "orders", "customer-2");

        registry.Publish("orders", "customer-1", new byte[] { 7 });

        Assert.Single(wildcard.Events);
        Assert.Single(matching.Events);
        Assert.Empty(other.Events);
    }

    [Fact]
    public void Outbound_queue_enforces_its_byte_budget()
    {
        var queue = new ByteBoundedOutboundQueue(maximumBytes: 4, maximumFrames: 4);

        Assert.True(queue.TryWrite(new byte[] { 1, 2, 3 }, WebSocketMessageType.Binary));
        Assert.False(queue.TryWrite(new byte[] { 4, 5 }, WebSocketMessageType.Binary));
        Assert.True(queue.Reader.TryRead(out var first));
        queue.Release(first);
        Assert.True(queue.TryWrite(new byte[] { 4, 5 }, WebSocketMessageType.Binary));
    }

    [Fact]
    public void Subscription_credit_is_atomic_and_bounded()
    {
        var subscription = new SubscriptionState(1, "orders", null, initialCredit: 1);

        Assert.True(subscription.TryConsumeCredit());
        Assert.False(subscription.TryConsumeCredit());
        Assert.True(subscription.TryAddCredit(2, maximum: 2));
        Assert.False(subscription.TryAddCredit(1, maximum: 2));
        Assert.True(subscription.TryConsumeCredit());
        Assert.True(subscription.TryConsumeCredit());
    }

    [Fact]
    public async Task Published_HostLoom_event_reaches_the_gateway_registry()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<OrderChanged>("orders.changed", "orders", value => value.CustomerId);
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var sink = new RecordingSink("browser");
        host.Services.GetRequiredService<WebSocketSessionRegistry>()
            .Subscribe(sink, "orders.changed", "customer-1");

        await host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync(
                "orders",
                new OrderChanged("customer-1"),
                TestContext.Current.CancellationToken
            );

        var delivered = Assert.Single(sink.Payloads);
        var decoded = host
            .Services.GetRequiredService<IMessageSerializer>()
            .Deserialize<OrderChanged>(delivered.Span);
        Assert.Equal("customer-1", decoded?.CustomerId);
    }

    [Fact]
    public async Task Raw_session_subscribes_and_forwards_an_event_with_credit()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<OrderChanged>("orders.changed", "orders", value => value.CustomerId);
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        socket.Enqueue(
            protocol.Encode(
                new HubFrame
                {
                    Kind = HubFrameKind.Subscribe,
                    StreamId = 41,
                    Topic = "orders.changed",
                    Key = "customer-1",
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);

        var welcome = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        var subscribed = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(HubFrameKind.Welcome, welcome.Kind);
        Assert.Equal(HubFrameKind.Subscribed, subscribed.Kind);
        Assert.Equal(
            1,
            Assert
                .Single(provider.GetRequiredService<IWebSocketSessionDirectory>().GetSessions())
                .SubscriptionCount
        );

        provider
            .GetRequiredService<WebSocketSessionRegistry>()
            .Publish("orders.changed", "customer-1", new byte[] { 9, 8 });
        var delivered = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(HubFrameKind.Event, delivered.Kind);
        Assert.Equal((ulong)41, delivered.StreamId);
        Assert.Equal("customer-1", delivered.Key);
        Assert.Equal(new byte[] { 9, 8 }, delivered.Payload!.Value.ToArray());

        socket.EnqueueClose();
        await run;
        Assert.Equal(WebSocketCloseStatus.NormalClosure, socket.CloseStatus);
        Assert.Equal(0, provider.GetRequiredService<IWebSocketSessionDirectory>().Count);
    }

    [Fact]
    public async Task Session_expiry_is_capped_and_closes_with_policy_violation()
    {
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.MaximumSessionLifetime = TimeSpan.FromMinutes(1);
            });
        await using var provider = services.BuildServiceProvider();
        using var socket = new ScriptedWebSocket();
        var protocol = new JsonWebSocketHubProtocol();
        var user = new ClaimsPrincipal(
            new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, "subject-1")], "test")
        );
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, user, clock.GetUtcNow() + TimeSpan.FromHours(1));

        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        var directory = provider.GetRequiredService<IWebSocketSessionDirectory>();
        var info = Assert.Single(directory.GetSessionsBySubject("subject-1"));
        Assert.Equal(session.Id, info.SessionId);
        Assert.Equal(protocol.SubProtocol, info.Protocol);
        Assert.Equal(DateTimeOffset.UnixEpoch, info.ConnectedAt);
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromMinutes(1), info.ExpiresAt);

        clock.Advance(TimeSpan.FromMinutes(1));
        await run.WaitAsync(TestContext.Current.CancellationToken);

        Assert.Equal(WebSocketCloseStatus.PolicyViolation, socket.CloseStatus);
        Assert.Equal("session_expired", socket.CloseStatusDescription);
        Assert.Equal(0, directory.Count);
    }

    [Fact]
    public async Task Session_control_disconnects_one_session_or_every_session_for_a_subject()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false);
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        var user = new ClaimsPrincipal(
            new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, "subject-1")], "test")
        );
        using var firstSocket = new ScriptedWebSocket();
        using var secondSocket = new ScriptedWebSocket();
        var factory = provider.GetRequiredService<WebSocketSessionFactory>();
        var first = factory.Create(firstSocket, protocol, user);
        var second = factory.Create(secondSocket, protocol, user);
        var firstRun = first.RunAsync(TestContext.Current.CancellationToken);
        var secondRun = second.RunAsync(TestContext.Current.CancellationToken);
        _ = await firstSocket.ReadSentAsync(TestContext.Current.CancellationToken);
        _ = await secondSocket.ReadSentAsync(TestContext.Current.CancellationToken);
        var control = provider.GetRequiredService<IWebSocketSessionControl>();

        Assert.False(
            await control.DisconnectAsync(
                "missing",
                "logout",
                TestContext.Current.CancellationToken
            )
        );
        Assert.True(
            await control.DisconnectAsync(first.Id, "logout", TestContext.Current.CancellationToken)
        );
        Assert.Equal(
            1,
            await control.DisconnectSubjectAsync(
                "subject-1",
                "roles_changed",
                TestContext.Current.CancellationToken
            )
        );
        await Task.WhenAll(firstRun, secondRun);

        Assert.Equal(WebSocketCloseStatus.PolicyViolation, firstSocket.CloseStatus);
        Assert.Equal("logout", firstSocket.CloseStatusDescription);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, secondSocket.CloseStatus);
        Assert.Equal("roles_changed", secondSocket.CloseStatusDescription);
    }

    [Fact]
    public async Task Default_lifetime_resolver_prefers_ticket_expiry_and_falls_back_to_exp_claim()
    {
        var resolver = new DefaultWebSocketSessionLifetimeResolver();
        var ticketExpiry = DateTimeOffset.UnixEpoch + TimeSpan.FromHours(2);
        var context = new DefaultHttpContext
        {
            User = new ClaimsPrincipal(new ClaimsIdentity([new Claim("exp", "3600")], "test")),
        };
        context.Features.Set<IAuthenticateResultFeature>(
            new TestAuthenticateResultFeature
            {
                AuthenticateResult = AuthenticateResult.Success(
                    new AuthenticationTicket(
                        context.User,
                        new AuthenticationProperties { ExpiresUtc = ticketExpiry },
                        "test"
                    )
                ),
            }
        );

        Assert.Equal(
            ticketExpiry,
            await resolver.ResolveExpirationAsync(context, TestContext.Current.CancellationToken)
        );
        context.Features.Set<IAuthenticateResultFeature>(null);
        Assert.Equal(
            DateTimeOffset.UnixEpoch + TimeSpan.FromHours(1),
            await resolver.ResolveExpirationAsync(context, TestContext.Current.CancellationToken)
        );
    }

    private static void AssertRoundTrip(IWebSocketHubProtocol protocol)
    {
        var encoded = protocol.Encode(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = 12,
                SessionId = "session-1",
                Operation = "orders.get",
                Topic = "orders.changed",
                Key = "customer-1",
                TimeoutMilliseconds = 5000,
                Credit = 32,
                Sequence = 123456789,
                EventId = "event-1",
                Code = "test_code",
                Message = "test message",
                Payload = new byte[] { 1, 2, 3 },
                MaximumMessageSize = 65536,
                MaximumConcurrentRequests = 8,
            }
        );
        var decoded = protocol.Decode(encoded);

        Assert.Equal(HubFrameKind.Request, decoded.Kind);
        Assert.Equal((ulong)12, decoded.StreamId);
        Assert.Equal("session-1", decoded.SessionId);
        Assert.Equal("orders.get", decoded.Operation);
        Assert.Equal("orders.changed", decoded.Topic);
        Assert.Equal("customer-1", decoded.Key);
        Assert.Equal(5000, decoded.TimeoutMilliseconds);
        Assert.Equal(32, decoded.Credit);
        Assert.Equal(123456789, decoded.Sequence);
        Assert.Equal("event-1", decoded.EventId);
        Assert.Equal("test_code", decoded.Code);
        Assert.Equal("test message", decoded.Message);
        Assert.Equal(new byte[] { 1, 2, 3 }, decoded.Payload!.Value.ToArray());
        Assert.Equal(65536, decoded.MaximumMessageSize);
        Assert.Equal(8, decoded.MaximumConcurrentRequests);
    }

    private static void AssertFixture(
        JsonWebSocketHubProtocol protocol,
        string fixtureDirectory,
        string fixture
    )
    {
        var expected = File.ReadAllText(
                ProtocolFile("fixtures", fixtureDirectory, $"{fixture}.json")
            )
            .TrimEnd();
        var frame = CreateFixtureFrame(fixture);
        var encoded = Encoding.UTF8.GetString(protocol.Encode(frame));

        Assert.Equal(expected, encoded);
        var decoded = protocol.Decode(Encoding.UTF8.GetBytes(expected));
        Assert.Equal(frame.Kind, decoded.Kind);
        Assert.Equal(frame.StreamId, decoded.StreamId);
    }

    private static HubFrame CreateFixtureFrame(string fixture) =>
        fixture switch
        {
            "welcome" => new HubFrame
            {
                Kind = HubFrameKind.Welcome,
                SessionId = "session-1",
                Credit = 1024,
                MaximumMessageSize = 65536,
                MaximumConcurrentRequests = 8,
            },
            "subscribed" => new HubFrame
            {
                Kind = HubFrameKind.Subscribed,
                StreamId = 41,
                Topic = "orders.changed",
                Key = "customer-1",
                Credit = 32,
            },
            "event" => new HubFrame
            {
                Kind = HubFrameKind.Event,
                StreamId = 41,
                Topic = "orders.changed",
                Key = "customer-1",
                Sequence = 7,
                EventId = "event-1",
                Payload = new byte[] { 1, 2, 3 },
            },
            "fault" => new HubFrame
            {
                Kind = HubFrameKind.Fault,
                StreamId = 12,
                Code = "forbidden",
                Message = "The caller is not authorized.",
            },
            _ => throw new ArgumentOutOfRangeException(nameof(fixture), fixture, null),
        };

    private static string ProtocolFile(params string[] segments) =>
        Path.Combine([AppContext.BaseDirectory, "protocol", .. segments]);

    public sealed record Greet(string Name) : IRequest<Greeting>;

    public sealed record Greeting(string Text);

    public sealed record OrderChanged(string CustomerId) : IEvent;

    public sealed class GreetHandler : IRequestHandler<Greet, Greeting>
    {
        public ValueTask<Greeting> HandleAsync(
            Greet request,
            CancellationToken cancellationToken
        ) => ValueTask.FromResult(new Greeting($"Hello, {request.Name}!"));
    }

    private sealed class RecordingSink(string id) : IWebSocketEventSink
    {
        public string Id { get; } = id;

        public ConcurrentQueue<string?> Events { get; } = new();

        public ConcurrentQueue<ReadOnlyMemory<byte>> Payloads { get; } = new();

        public bool TryQueueEvent(
            string topic,
            string? subscriptionKey,
            string? eventKey,
            ReadOnlyMemory<byte> payload,
            string eventId,
            long sequence
        )
        {
            Events.Enqueue(eventKey);
            Payloads.Enqueue(payload);
            return true;
        }

        public void Abort() =>
            throw new InvalidOperationException("The recording sink should not be aborted.");
    }

    private sealed class ScriptedWebSocket : WebSocket
    {
        private readonly Channel<Inbound> _inbound = Channel.CreateUnbounded<Inbound>();
        private readonly Channel<ReadOnlyMemory<byte>> _sent = Channel.CreateUnbounded<
            ReadOnlyMemory<byte>
        >();
        private WebSocketState _state = WebSocketState.Open;
        private WebSocketCloseStatus? _closeStatus;
        private string? _closeStatusDescription;

        public override WebSocketCloseStatus? CloseStatus => _closeStatus;

        public override string? CloseStatusDescription => _closeStatusDescription;

        public override WebSocketState State => _state;

        public override string? SubProtocol => JsonWebSocketHubProtocol.ProtocolName;

        public void Enqueue(byte[] payload, WebSocketMessageType messageType) =>
            _inbound.Writer.TryWrite(new Inbound(payload, messageType, null));

        public void EnqueueClose() =>
            _inbound.Writer.TryWrite(
                new Inbound([], WebSocketMessageType.Close, WebSocketCloseStatus.NormalClosure)
            );

        public ValueTask<ReadOnlyMemory<byte>> ReadSentAsync(CancellationToken cancellationToken) =>
            _sent.Reader.ReadAsync(cancellationToken);

        public override void Abort() => _state = WebSocketState.Aborted;

        public override Task CloseAsync(
            WebSocketCloseStatus closeStatus,
            string? statusDescription,
            CancellationToken cancellationToken
        ) => CloseOutputAsync(closeStatus, statusDescription, cancellationToken);

        public override Task CloseOutputAsync(
            WebSocketCloseStatus closeStatus,
            string? statusDescription,
            CancellationToken cancellationToken
        )
        {
            _closeStatus = closeStatus;
            _closeStatusDescription = statusDescription;
            _state = WebSocketState.Closed;
            return Task.CompletedTask;
        }

        public override void Dispose() => _state = WebSocketState.Closed;

        public override async Task<WebSocketReceiveResult> ReceiveAsync(
            ArraySegment<byte> buffer,
            CancellationToken cancellationToken
        )
        {
            var inbound = await _inbound.Reader.ReadAsync(cancellationToken);
            if (inbound.MessageType is WebSocketMessageType.Close)
            {
                _state = WebSocketState.CloseReceived;
                return new WebSocketReceiveResult(
                    0,
                    WebSocketMessageType.Close,
                    true,
                    inbound.CloseStatus,
                    "test close"
                );
            }

            inbound.Payload.AsSpan().CopyTo(buffer.AsSpan());
            return new WebSocketReceiveResult(inbound.Payload.Length, inbound.MessageType, true);
        }

        public override Task SendAsync(
            ArraySegment<byte> buffer,
            WebSocketMessageType messageType,
            bool endOfMessage,
            CancellationToken cancellationToken
        )
        {
            _sent.Writer.TryWrite(buffer.ToArray());
            return Task.CompletedTask;
        }

        private readonly record struct Inbound(
            byte[] Payload,
            WebSocketMessageType MessageType,
            WebSocketCloseStatus? CloseStatus
        );
    }

    private sealed class TestAuthenticateResultFeature : IAuthenticateResultFeature
    {
        public AuthenticateResult? AuthenticateResult { get; set; }
    }
}
