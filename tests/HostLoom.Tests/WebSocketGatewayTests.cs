using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using System.Text;
using System.Text.Json;
using System.Threading.Channels;
using HostLoom.AspNetCore.WebSockets;
using HostLoom.AspNetCore.WebSockets.Testing;
using HostLoom.Diagnostics;
using HostLoom.Transport.InMemory;
using Microsoft.AspNetCore.Authentication;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
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
    [InlineData("snapshot-event")]
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
    public void WebSocket_log_events_have_stable_ids_and_names()
    {
        Assert.Equal(new EventId(4100, "WebSocketSessionOpened"), WebSocketEvents.SessionOpened);
        Assert.Equal(new EventId(4101, "WebSocketSessionClosed"), WebSocketEvents.SessionClosed);
        Assert.Equal(
            new EventId(4102, "WebSocketSubscriptionDenied"),
            WebSocketEvents.SubscriptionDenied
        );
        Assert.Equal(
            new EventId(4103, "WebSocketSlowClientAborted"),
            WebSocketEvents.SlowClientAborted
        );
        Assert.Equal(
            new EventId(4104, "WebSocketHandshakeRejected"),
            WebSocketEvents.HandshakeRejected
        );
        Assert.Equal(
            new EventId(4105, "WebSocketOperationFailed"),
            WebSocketEvents.OperationFailed
        );
        Assert.Equal(new EventId(4106, "WebSocketSnapshotFailed"), WebSocketEvents.SnapshotFailed);
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
        using var activities = new WebSocketActivityRecorder();
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
        using var traceRoot = new Activity("websocket trace test");
        traceRoot.SetIdFormat(ActivityIdFormat.W3C);
        traceRoot.Start();
        var traceId = traceRoot.TraceId;

        var response = await router.RouteAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = 17,
                Operation = "greet",
                Payload = serializer.Serialize(new Greet("Ada")),
            },
            new ClaimsPrincipal(),
            TestContext.Current.CancellationToken,
            JsonWebSocketHubProtocol.ProtocolName
        );

        Assert.Equal(HubFrameKind.Response, response.Kind);
        Assert.Equal((ulong)17, response.StreamId);
        var greeting = serializer.Deserialize<Greeting>(response.Payload!.Value.Span);
        Assert.Equal("Hello, Ada!", greeting?.Text);

        var gateway = Assert.Single(
            activities.Activities,
            activity =>
                activity.Source == WebSocketDiagnostics.ActivitySourceName
                && activity.TraceId == traceId
                && activity.Tag(WebSocketDiagnostics.OperationTag) is "greet"
        );
        Assert.Equal(WebSocketDiagnostics.RequestActivityName, gateway.Name);
        Assert.Equal(ActivityKind.Server, gateway.Kind);
        Assert.Equal(ActivityStatusCode.Ok, gateway.Status);
        Assert.Equal("success", gateway.Tag(WebSocketDiagnostics.OutcomeTag));
        Assert.Equal(
            JsonWebSocketHubProtocol.ProtocolName,
            gateway.Tag(WebSocketDiagnostics.ProtocolTag)
        );

        var brokerSend = Assert.Single(
            activities.Activities,
            activity =>
                activity.TraceId == traceId
                && activity.Source == HostLoomDiagnostics.ActivitySourceName
                && activity.Name == "hostloom request"
        );
        Assert.Equal(gateway.TraceId, brokerSend.TraceId);
        Assert.Equal(gateway.SpanId, brokerSend.ParentSpanId);

        var dispatch = Assert.Single(
            activities.Activities,
            activity =>
                activity.TraceId == traceId
                && activity.Source == HostLoomDiagnostics.ActivitySourceName
                && activity.Name == "hostloom handle request"
        );
        Assert.Equal(brokerSend.TraceId, dispatch.TraceId);
        Assert.Equal(brokerSend.SpanId, dispatch.ParentSpanId);
    }

    [Fact]
    public async Task Malformed_request_payload_returns_an_invalid_payload_fault()
    {
        using var activities = new WebSocketActivityRecorder();
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
        using var traceRoot = new Activity("websocket fault trace test");
        traceRoot.SetIdFormat(ActivityIdFormat.W3C);
        traceRoot.Start();
        var traceId = traceRoot.TraceId;

        var response = await router.RouteAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = 18,
                Operation = "greet",
                Payload = "not-json"u8.ToArray(),
            },
            new ClaimsPrincipal(),
            TestContext.Current.CancellationToken,
            JsonWebSocketHubProtocol.ProtocolName
        );

        Assert.Equal(HubFrameKind.Fault, response.Kind);
        Assert.Equal(HubFaultCodes.InvalidPayload, response.Code);
        var gateway = Assert.Single(
            activities.Activities,
            activity =>
                activity.Source == WebSocketDiagnostics.ActivitySourceName
                && activity.TraceId == traceId
                && activity.Tag(WebSocketDiagnostics.OperationTag) is "greet"
        );
        Assert.Equal(ActivityStatusCode.Error, gateway.Status);
        Assert.Equal("fault", gateway.Tag(WebSocketDiagnostics.OutcomeTag));
        Assert.Equal(HubFaultCodes.InvalidPayload, gateway.Tag(WebSocketDiagnostics.FaultCodeTag));
    }

    [Fact]
    public async Task Unregistered_operation_does_not_become_trace_identity()
    {
        const string untrustedOperation = "private.top-secret-token";
        using var activities = new WebSocketActivityRecorder();
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false);
        await using var provider = services.BuildServiceProvider();
        using var traceRoot = new Activity("unregistered websocket trace test");
        traceRoot.SetIdFormat(ActivityIdFormat.W3C);
        traceRoot.Start();
        var traceId = traceRoot.TraceId;

        var response = await provider
            .GetRequiredService<WebSocketRequestRouter>()
            .RouteAsync(
                new HubFrame
                {
                    Kind = HubFrameKind.Request,
                    StreamId = 19,
                    Operation = untrustedOperation,
                    Payload = ReadOnlyMemory<byte>.Empty,
                },
                new ClaimsPrincipal(),
                TestContext.Current.CancellationToken,
                JsonWebSocketHubProtocol.ProtocolName
            );

        Assert.Equal(HubFaultCodes.OperationNotFound, response.Code);
        Assert.DoesNotContain(
            activities.Activities,
            activity =>
                activity.TraceId == traceId
                && activity.Source == WebSocketDiagnostics.ActivitySourceName
        );
        Assert.DoesNotContain(
            activities.Activities,
            activity =>
                activity.TraceId == traceId
                && (
                    activity.Name.Contains(untrustedOperation, StringComparison.Ordinal)
                    || activity.Tags.Values.OfType<string>().Contains(untrustedOperation)
                )
        );
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
    public async Task Topic_policy_receives_the_client_selected_subscription_key()
    {
        var services = new ServiceCollection();
        services.AddAuthorization(options =>
            options.AddPolicy(
                "customer-key",
                policy =>
                    policy.RequireAssertion(context =>
                        context.Resource
                            is WebSocketTopicResource { Topic: "orders.changed", Key: "customer-1" }
                    )
            )
        );
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<OrderChanged>(
                "orders.changed",
                "orders",
                value => value.CustomerId,
                authorizationPolicy: "customer-key"
            );
        await using var provider = services.BuildServiceProvider();
        var configuration = provider.GetRequiredService<GatewayConfiguration>();
        Assert.True(configuration.TryGetTopic("orders.changed", out var topic));
        var router = provider.GetRequiredService<WebSocketRequestRouter>();

        Assert.True(await router.AuthorizeTopicAsync(topic, "customer-1", new ClaimsPrincipal()));
        Assert.False(await router.AuthorizeTopicAsync(topic, "customer-2", new ClaimsPrincipal()));
    }

    [Fact]
    public async Task Subject_only_topic_policy_accepts_own_key_and_rejects_other_keys()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.SubjectClaimType = "tenant_id";
            })
            .AddTopic<OrderChanged>(
                "orders.changed",
                "orders",
                value => value.CustomerId,
                authorizationPolicy: TopicKeyPolicy.SubjectOnly
            );
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        var user = new ClaimsPrincipal(
            new ClaimsIdentity([new Claim("tenant_id", "tenant-1")], "test")
        );
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, user);
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);

        socket.Enqueue(
            protocol.Encode(
                new HubFrame
                {
                    Kind = HubFrameKind.Subscribe,
                    StreamId = 1,
                    Topic = "orders.changed",
                    Key = "TENANT-1",
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var forbidden = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(HubFrameKind.Fault, forbidden.Kind);
        Assert.Equal(HubFaultCodes.Forbidden, forbidden.Code);
        Assert.Equal(
            0,
            Assert
                .Single(provider.GetRequiredService<IWebSocketSessionDirectory>().GetSessions())
                .SubscriptionCount
        );

        socket.Enqueue(
            protocol.Encode(
                new HubFrame
                {
                    Kind = HubFrameKind.Subscribe,
                    StreamId = 2,
                    Topic = "orders.changed",
                    Key = "tenant-1",
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var subscribed = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(HubFrameKind.Subscribed, subscribed.Kind);
        Assert.Equal((ulong)2, subscribed.StreamId);

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public async Task Subject_only_topic_policy_rejects_missing_key_or_subject()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<OrderChanged>(
                "orders.changed",
                "orders",
                value => value.CustomerId,
                authorizationPolicy: TopicKeyPolicy.SubjectOnly
            );
        await using var provider = services.BuildServiceProvider();
        var configuration = provider.GetRequiredService<GatewayConfiguration>();
        Assert.True(configuration.TryGetTopic("orders.changed", out var topic));
        var router = provider.GetRequiredService<WebSocketRequestRouter>();
        var user = new ClaimsPrincipal(
            new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, "subject-1")], "test")
        );

        Assert.False(await router.AuthorizeTopicAsync(topic, null, user));
        Assert.False(
            await router.AuthorizeTopicAsync(
                topic,
                "subject-1",
                new ClaimsPrincipal(new ClaimsIdentity())
            )
        );
        Assert.False(
            await router.AuthorizeTopicAsync(
                topic,
                "subject-1",
                new ClaimsPrincipal(
                    new ClaimsIdentity([new Claim(ClaimTypes.NameIdentifier, "subject-1")])
                )
            )
        );
        Assert.False(
            await router.AuthorizeTopicAsync(
                topic,
                "subject-2",
                new ClaimsPrincipal(
                    new ClaimsIdentity(
                        [
                            new Claim(ClaimTypes.NameIdentifier, "subject-1"),
                            new Claim(ClaimTypes.NameIdentifier, "subject-2"),
                        ],
                        "test"
                    )
                )
            )
        );
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
    public void Outbound_queue_counts_reserved_frames_against_the_connection_budget()
    {
        var queue = new ByteBoundedOutboundQueue(maximumBytes: 16, maximumFrames: 1);

        Assert.True(
            queue.TryReserve(new byte[] { 1 }, WebSocketMessageType.Binary, out var reservation)
        );
        Assert.False(queue.TryWrite(new byte[] { 2 }, WebSocketMessageType.Binary));
        queue.Release(reservation);
        Assert.True(queue.TryWrite(new byte[] { 2 }, WebSocketMessageType.Binary));
    }

    [Fact]
    public void Active_subscription_drops_no_credit_event_without_reserving_queue_capacity()
    {
        var queue = new ByteBoundedOutboundQueue(maximumBytes: 16, maximumFrames: 1);
        var state = new SubscriptionState(1, "orders.changed", null, initialCredit: 0);
        Assert.True(state.CompleteInitialization(queue.TryWriteReserved, queue.Release));
        Assert.True(queue.TryWrite(new byte[] { 1 }, WebSocketMessageType.Binary));

        var disposition = state.AcceptLiveEvent(
            queue,
            new byte[] { 2 },
            WebSocketMessageType.Binary,
            out _
        );

        Assert.Equal(LiveEventDisposition.Dropped, disposition);
        Assert.True(queue.Reader.TryRead(out var queued));
        queue.Release(queued);
        Assert.True(queue.TryWrite(new byte[] { 2 }, WebSocketMessageType.Binary));
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
    public async Task Session_lifecycle_and_delivery_emit_bounded_metrics()
    {
        const string topic = "metrics.orders.changed";
        using var metrics = new WebSocketMetricRecorder();
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<OrderChanged>(topic, "orders", value => value.CustomerId);
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
                    StreamId = 51,
                    Topic = topic,
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);

        var registry = provider.GetRequiredService<WebSocketSessionRegistry>();
        registry.Publish(topic, key: null, new byte[] { 9, 8 });
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        registry.Publish(topic, key: null, new byte[] { 7, 6 });

        socket.Enqueue(
            protocol.Encode(new HubFrame { Kind = HubFrameKind.Response, StreamId = 52 }),
            protocol.MessageType
        );
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        socket.Enqueue(
            protocol.Encode(new HubFrame { Kind = HubFrameKind.Unsubscribe, StreamId = 51 }),
            protocol.MessageType
        );
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        socket.EnqueueClose();
        await run;

        Assert.Contains(
            metrics.Measurements("hostloom.websocket.sessions"),
            measurement =>
                measurement.Value == 1
                && measurement.Tag(WebSocketDiagnostics.ProtocolTag)
                    is JsonWebSocketHubProtocol.ProtocolName
        );
        Assert.Contains(
            metrics.Measurements("hostloom.websocket.sessions"),
            measurement =>
                measurement.Value == -1
                && measurement.Tag(WebSocketDiagnostics.ProtocolTag)
                    is JsonWebSocketHubProtocol.ProtocolName
        );
        Assert.Equal(
            [1d, -1d],
            metrics
                .Measurements("hostloom.websocket.subscriptions")
                .Where(measurement => measurement.Tag(WebSocketDiagnostics.TopicTag) is topic)
                .Select(static measurement => measurement.Value)
                .ToArray()
        );
        Assert.Single(
            metrics.Measurements("hostloom.websocket.events.sent"),
            measurement => measurement.Tag(WebSocketDiagnostics.TopicTag) is topic
        );
        Assert.Contains(
            metrics.Measurements("hostloom.websocket.events.dropped"),
            measurement =>
                measurement.Tag(WebSocketDiagnostics.TopicTag) is topic
                && measurement.Tag(WebSocketDiagnostics.ReasonTag) is "no_credit"
        );
        Assert.Contains(
            metrics.Measurements("hostloom.websocket.queue.bytes"),
            measurement =>
                measurement.Value > 0 && measurement.Tag(WebSocketDiagnostics.TopicTag) is topic
        );
        Assert.Contains(
            metrics.Measurements("hostloom.websocket.faults"),
            measurement =>
                measurement.Tag(WebSocketDiagnostics.FaultCodeTag) is HubFaultCodes.InvalidFrame
        );
        Assert.Contains(
            metrics.Measurements("hostloom.websocket.session.duration"),
            measurement =>
                measurement.Value >= 0
                && measurement.Tag(WebSocketDiagnostics.CloseReasonTag) is "peer_closed"
        );
    }

    [Fact]
    public async Task Session_lifecycle_and_subscription_denial_emit_safe_structured_logs()
    {
        const string secretToken = "top-secret-token";
        const string secretKey = "top-secret-key";
        using var logs = new WebSocketLogRecorder();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(logs));
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false);
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var principal = new ClaimsPrincipal(
            new ClaimsIdentity(
                [
                    new Claim(ClaimTypes.NameIdentifier, "customer-1"),
                    new Claim("access_token", secretToken),
                ],
                "test"
            )
        );
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, principal);

        socket.Enqueue(
            protocol.Encode(
                new HubFrame
                {
                    Kind = HubFrameKind.Subscribe,
                    StreamId = 61,
                    Topic = $"private.{secretToken}",
                    Key = secretKey,
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        socket.EnqueueClose();
        await run;

        var opened = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SessionOpened
        );
        Assert.Equal(LogLevel.Information, opened.Level);
        Assert.Equal(session.Id, opened.Property("SessionId"));
        Assert.Equal("customer-1", opened.Property("Subject"));
        Assert.Equal(JsonWebSocketHubProtocol.ProtocolName, opened.Property("Protocol"));

        var denied = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SubscriptionDenied
        );
        Assert.Equal(LogLevel.Warning, denied.Level);
        Assert.Null(denied.Property("Topic"));
        Assert.Equal("topic_not_found", denied.Property("Reason"));

        var closed = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SessionClosed
        );
        Assert.Equal("peer_closed", closed.Property("CloseReason"));
        Assert.Equal(WebSocketCloseStatus.NormalClosure, closed.Property("CloseStatus"));
        Assert.DoesNotContain(logs.Entries, entry => entry.Contains(secretToken));
        Assert.DoesNotContain(logs.Entries, entry => entry.Contains(secretKey));
    }

    [Fact]
    public async Task Outbound_capacity_aborts_once_with_a_stable_slow_client_event()
    {
        const string topic = "logs.orders.changed";
        using var logs = new WebSocketLogRecorder();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(logs));
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.MaximumQueuedFramesPerConnection = 2;
            })
            .AddTopic<OrderChanged>(topic, "orders", value => value.CustomerId);
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
                    StreamId = 62,
                    Topic = topic,
                    Credit = 3,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);

        socket.BlockSends();
        var registry = provider.GetRequiredService<WebSocketSessionRegistry>();
        registry.Publish(topic, key: null, new byte[] { 1 });
        await socket.WaitForBlockedSendAsync(TestContext.Current.CancellationToken);
        registry.Publish(topic, key: null, new byte[] { 2 });
        registry.Publish(topic, key: null, new byte[] { 3 });

        var aborted = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SlowClientAborted
        );
        Assert.Equal(LogLevel.Warning, aborted.Level);
        Assert.Equal(session.Id, aborted.Property("SessionId"));
        Assert.Equal(HubFrameKind.Event, aborted.Property("FrameKind"));
        Assert.Equal(topic, aborted.Property("Topic"));
        Assert.Equal(2, aborted.Property("MaximumQueuedFrames"));

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public async Task Snapshot_is_queued_before_live_events_that_arrive_during_initialization()
    {
        var snapshots = new BlockingStatusSnapshotProvider(new StatusChanged("customer-1", 1));
        var services = new ServiceCollection();
        services.AddSingleton<IWebSocketTopicSnapshotProvider<StatusChanged>>(snapshots);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<StatusChanged>("status.changed", "status", value => value.Key)
            .AddTopicSnapshot<StatusChanged, BlockingStatusSnapshotProvider>("status.changed");
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
                    StreamId = 51,
                    Topic = "status.changed",
                    Key = "customer-1",
                    Credit = 2,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        var subscribed = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(HubFrameKind.Subscribed, subscribed.Kind);
        await snapshots.Started.WaitAsync(TestContext.Current.CancellationToken);

        var serializer = provider.GetRequiredService<IMessageSerializer>();
        provider
            .GetRequiredService<WebSocketSessionRegistry>()
            .Publish(
                "status.changed",
                "customer-1",
                serializer.Serialize(new StatusChanged("customer-1", 2))
            );
        snapshots.Release();

        var snapshot = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        var live = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(0, snapshot.Sequence);
        Assert.Equal(
            1,
            serializer.Deserialize<StatusChanged>(snapshot.Payload!.Value.Span)?.Version
        );
        Assert.True(live.Sequence > 0);
        Assert.Equal(2, serializer.Deserialize<StatusChanged>(live.Payload!.Value.Span)?.Version);
        Assert.Equal("customer-1", snapshots.Context?.Key);

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public async Task Snapshot_waits_for_additional_subscription_credit()
    {
        var snapshots = new ListStatusSnapshotProvider(
            new StatusChanged("customer-1", 1),
            new StatusChanged("customer-1", 2)
        );
        var services = new ServiceCollection();
        services.AddSingleton<IWebSocketTopicSnapshotProvider<StatusChanged>>(snapshots);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<StatusChanged>("status.changed", "status", value => value.Key)
            .AddTopicSnapshot<StatusChanged, ListStatusSnapshotProvider>("status.changed");
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
                    StreamId = 52,
                    Topic = "status.changed",
                    Key = "customer-1",
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        var first = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(0, first.Sequence);

        socket.Enqueue(
            protocol.Encode(
                new HubFrame
                {
                    Kind = HubFrameKind.Credit,
                    StreamId = 52,
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var second = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(0, second.Sequence);
        Assert.Equal(
            2,
            provider
                .GetRequiredService<IMessageSerializer>()
                .Deserialize<StatusChanged>(second.Payload!.Value.Span)
                ?.Version
        );

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public async Task Keyless_subscription_receives_every_snapshot_value()
    {
        var snapshots = new ListStatusSnapshotProvider(
            new StatusChanged("customer-1", 1),
            new StatusChanged("customer-2", 2)
        );
        var services = new ServiceCollection();
        services.AddSingleton<IWebSocketTopicSnapshotProvider<StatusChanged>>(snapshots);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<StatusChanged>("status.changed", "status", value => value.Key)
            .AddTopicSnapshot<StatusChanged, ListStatusSnapshotProvider>("status.changed");
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
                    StreamId = 53,
                    Topic = "status.changed",
                    Credit = 2,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);

        var first = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        var second = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(["customer-1", "customer-2"], new[] { first.Key, second.Key });
        Assert.Equal(0, first.Sequence);
        Assert.Equal(0, second.Sequence);

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public async Task Keyed_subscription_ignores_snapshot_values_for_other_keys()
    {
        var snapshots = new ListStatusSnapshotProvider(
            new StatusChanged("customer-1", 1),
            new StatusChanged("customer-2", 2)
        );
        var services = new ServiceCollection();
        services.AddSingleton<IWebSocketTopicSnapshotProvider<StatusChanged>>(snapshots);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<StatusChanged>("status.changed", "status", value => value.Key)
            .AddTopicSnapshot<StatusChanged, ListStatusSnapshotProvider>("status.changed");
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
                    StreamId = 56,
                    Topic = "status.changed",
                    Key = "customer-1",
                    Credit = 2,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        var snapshot = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );

        provider
            .GetRequiredService<WebSocketSessionRegistry>()
            .Publish(
                "status.changed",
                "customer-1",
                provider
                    .GetRequiredService<IMessageSerializer>()
                    .Serialize(new StatusChanged("customer-1", 3))
            );
        var live = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );

        Assert.Equal("customer-1", snapshot.Key);
        Assert.Equal(0, snapshot.Sequence);
        Assert.True(live.Sequence > 0);

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public async Task Snapshot_failure_faults_only_the_subscription()
    {
        using var logs = new WebSocketLogRecorder();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(logs));
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<StatusChanged>("status.changed", "status", value => value.Key)
            .AddTopicSnapshot<StatusChanged, FailingStatusSnapshotProvider>("status.changed");
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
                    StreamId = 54,
                    Topic = "status.changed",
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        var fault = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );

        Assert.Equal(HubFrameKind.Fault, fault.Kind);
        Assert.Equal(HubFaultCodes.SnapshotFailed, fault.Code);
        Assert.Equal(
            0,
            Assert
                .Single(provider.GetRequiredService<IWebSocketSessionDirectory>().GetSessions())
                .SubscriptionCount
        );
        var failed = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SnapshotFailed
        );
        Assert.Equal(LogLevel.Error, failed.Level);
        Assert.Equal("status.changed", failed.Property("Topic"));
        Assert.Equal(session.Id, failed.Property("SessionId"));

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public async Task Unsubscribe_cancels_snapshot_initialization_before_completing_the_stream()
    {
        var snapshots = new BlockingStatusSnapshotProvider(new StatusChanged("customer-1", 1));
        var services = new ServiceCollection();
        services.AddSingleton<IWebSocketTopicSnapshotProvider<StatusChanged>>(snapshots);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<StatusChanged>("status.changed", "status", value => value.Key)
            .AddTopicSnapshot<StatusChanged, BlockingStatusSnapshotProvider>("status.changed");
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
                    StreamId = 55,
                    Topic = "status.changed",
                    Key = "customer-1",
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        await snapshots.Started.WaitAsync(TestContext.Current.CancellationToken);

        socket.Enqueue(
            protocol.Encode(new HubFrame { Kind = HubFrameKind.Unsubscribe, StreamId = 55 }),
            protocol.MessageType
        );
        var complete = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        await snapshots.Completed.WaitAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HubFrameKind.Complete, complete.Kind);
        Assert.True(snapshots.WasCanceled);
        Assert.Equal(
            0,
            Assert
                .Single(provider.GetRequiredService<IWebSocketSessionDirectory>().GetSessions())
                .SubscriptionCount
        );

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public void Snapshot_provider_requires_a_matching_registered_topic()
    {
        var services = new ServiceCollection();
        var gateway = services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false);

        _ = Assert.Throws<InvalidOperationException>(() =>
            gateway.AddTopicSnapshot<StatusChanged, ListStatusSnapshotProvider>("missing")
        );
        gateway
            .AddTopic<StatusChanged>("status.changed", "status", value => value.Key)
            .AddTopicSnapshot<StatusChanged, ListStatusSnapshotProvider>("status.changed");
        _ = Assert.Throws<InvalidOperationException>(() =>
            gateway.AddTopicSnapshot<StatusChanged, ListStatusSnapshotProvider>("status.changed")
        );
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
    public async Task Control_frame_rate_limit_uses_a_fake_time_window()
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
                options.MaximumControlFramesPerSecond = 2;
            });
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);

        await SendInvalidCreditAndAwaitFaultAsync(socket, protocol);
        await SendInvalidCreditAndAwaitFaultAsync(socket, protocol);
        clock.Advance(TimeSpan.FromSeconds(1));
        await SendInvalidCreditAndAwaitFaultAsync(socket, protocol);
        await SendInvalidCreditAndAwaitFaultAsync(socket, protocol);
        socket.Enqueue(
            protocol.Encode(
                new HubFrame
                {
                    Kind = HubFrameKind.Credit,
                    StreamId = 5,
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        await run.WaitAsync(TestContext.Current.CancellationToken);

        Assert.Equal(WebSocketCloseStatus.PolicyViolation, socket.CloseStatus);
        Assert.Equal("rate_limited", socket.CloseStatusDescription);
    }

    [Fact]
    public async Task Shutdown_service_closes_sessions_before_it_completes()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false);
        await using var provider = services.BuildServiceProvider();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, new JsonWebSocketHubProtocol(), new ClaimsPrincipal());
        var run = session.RunAsync(TestContext.Current.CancellationToken);
        _ = await socket.ReadSentAsync(TestContext.Current.CancellationToken);
        var shutdown = provider
            .GetServices<IHostedService>()
            .OfType<WebSocketSessionShutdownService>()
            .Single();

        await shutdown.StopAsync(TestContext.Current.CancellationToken);

        Assert.True(run.IsCompletedSuccessfully);
        Assert.Equal(WebSocketCloseStatus.EndpointUnavailable, socket.CloseStatus);
        Assert.Equal("server_shutdown", socket.CloseStatusDescription);
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

    [Fact]
    public async Task Test_client_opens_an_in_process_gateway_session()
    {
        using var host = await CreateTestHostAsync();
        await using var client = new WebSocketTestClient(host.GetTestServer());

        await client.ConnectAsync(
            new Uri("ws://localhost/hostloom"),
            TestContext.Current.CancellationToken
        );
        var welcome = await client.AwaitWelcomeAsync(TestContext.Current.CancellationToken);

        Assert.NotNull(welcome.SessionId);
        Assert.Equal(JsonWebSocketHubProtocol.ProtocolName, client.Socket.SubProtocol);
    }

    [Fact]
    public async Task Test_server_session_uses_the_registered_lifetime_resolver()
    {
        var clock = new TestClock();
        using var host = await CreateTestHostAsync(configureServices: services =>
        {
            services.AddSingleton<TimeProvider>(clock);
            services.AddSingleton<IWebSocketSessionLifetimeResolver>(
                new FixedLifetimeResolver(clock.GetUtcNow() + TimeSpan.FromMinutes(1))
            );
        });
        await using var client = new WebSocketTestClient(host.GetTestServer());
        await client.ConnectAsync(
            new Uri("ws://localhost/hostloom"),
            TestContext.Current.CancellationToken
        );
        _ = await client.AwaitWelcomeAsync(TestContext.Current.CancellationToken);

        clock.Advance(TimeSpan.FromMinutes(1));
        var buffer = new byte[1];
        var close = await client.Socket.ReceiveAsync(buffer, TestContext.Current.CancellationToken);

        Assert.Equal(WebSocketMessageType.Close, close.MessageType);
        Assert.Equal(WebSocketCloseStatus.PolicyViolation, close.CloseStatus);
        Assert.Equal("session_expired", close.CloseStatusDescription);
    }

    [Fact]
    public async Task Test_client_subscribes_and_receives_an_in_process_event()
    {
        using var host = await CreateTestHostAsync();
        await using var client = new WebSocketTestClient(host.GetTestServer());
        var cancellationToken = TestContext.Current.CancellationToken;

        await client.ConnectAsync(new Uri("ws://localhost/hostloom"), cancellationToken);
        _ = await client.AwaitWelcomeAsync(cancellationToken);
        await client.SendAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = 7,
                Topic = "orders.changed",
                Key = "customer-1",
                Credit = 1,
            },
            cancellationToken
        );
        _ = await client.AwaitSubscribedAsync(7, cancellationToken);

        await host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", new OrderChanged("customer-1"), cancellationToken);
        var delivered = await client.AwaitEventAsync(7, cancellationToken);
        var value = host
            .Services.GetRequiredService<IMessageSerializer>()
            .Deserialize<OrderChanged>(delivered.Payload!.Value.Span);

        Assert.Equal("customer-1", value?.CustomerId);
    }

    [Fact]
    public async Task Same_origin_policy_accepts_a_matching_browser_origin()
    {
        using var host = await CreateTestHostAsync();
        await using var client = new WebSocketTestClient(host.GetTestServer())
        {
            ConfigureRequest = request => request.Headers.Origin = "http://localhost",
        };

        await client.ConnectAsync(
            new Uri("ws://localhost/hostloom"),
            TestContext.Current.CancellationToken
        );

        _ = await client.AwaitWelcomeAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Same_origin_policy_rejects_a_foreign_browser_origin()
    {
        using var metrics = new WebSocketMetricRecorder();
        using var logs = new WebSocketLogRecorder();
        using var host = await CreateTestHostAsync(configureServices: services =>
            services.AddLogging(builder => builder.AddProvider(logs))
        );
        await using var client = new WebSocketTestClient(host.GetTestServer())
        {
            ConfigureRequest = request => request.Headers.Origin = "https://foreign.example",
        };

        var exception = await Assert.ThrowsAnyAsync<Exception>(() =>
            client.ConnectAsync(
                new Uri("ws://localhost/hostloom"),
                TestContext.Current.CancellationToken
            )
        );
        Assert.Contains("403", exception.Message, StringComparison.Ordinal);
        Assert.Contains(
            metrics.Measurements("hostloom.websocket.handshake.rejected"),
            measurement => measurement.Tag(WebSocketDiagnostics.ReasonTag) is "origin"
        );
        var rejected = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.HandshakeRejected
        );
        Assert.Equal(LogLevel.Warning, rejected.Level);
        Assert.Equal("origin", rejected.Property("Reason"));
        Assert.DoesNotContain("foreign.example", rejected.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task Origin_allowlist_accepts_an_exact_origin()
    {
        using var host = await CreateTestHostAsync(options =>
        {
            options.OriginMode = WebSocketOriginMode.AllowList;
            options.AllowedOrigins.Add("https://app.example");
        });
        await using var client = new WebSocketTestClient(host.GetTestServer())
        {
            ConfigureRequest = request => request.Headers.Origin = "https://APP.example:443",
        };

        await client.ConnectAsync(
            new Uri("ws://localhost/hostloom"),
            TestContext.Current.CancellationToken
        );

        _ = await client.AwaitWelcomeAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task Origin_policy_can_require_the_header()
    {
        using var host = await CreateTestHostAsync(options => options.AllowMissingOrigin = false);
        await using var client = new WebSocketTestClient(host.GetTestServer());

        var exception = await Assert.ThrowsAnyAsync<Exception>(() =>
            client.ConnectAsync(
                new Uri("ws://localhost/hostloom"),
                TestContext.Current.CancellationToken
            )
        );
        Assert.Contains("403", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Origin_allowlist_requires_at_least_one_valid_origin()
    {
        var services = new ServiceCollection();

        _ = Assert.Throws<InvalidOperationException>(() =>
            services
                .AddHostLoom()
                .UseInMemory()
                .AddWebSocketGateway(options => options.OriginMode = WebSocketOriginMode.AllowList)
        );
    }

    [Theory]
    [InlineData("")]
    [InlineData("null")]
    [InlineData("https://app.example/path")]
    [InlineData("https://user@app.example")]
    public void Origin_allowlist_rejects_malformed_origins(string origin)
    {
        var services = new ServiceCollection();

        _ = Assert.Throws<InvalidOperationException>(() =>
            services
                .AddHostLoom()
                .UseInMemory()
                .AddWebSocketGateway(options =>
                {
                    options.OriginMode = WebSocketOriginMode.AllowList;
                    options.AllowedOrigins.Add(origin);
                })
        );
    }

    private static async Task SendInvalidCreditAndAwaitFaultAsync(
        ScriptedWebSocket socket,
        JsonWebSocketHubProtocol protocol
    )
    {
        socket.Enqueue(
            protocol.Encode(
                new HubFrame
                {
                    Kind = HubFrameKind.Credit,
                    StreamId = 5,
                    Credit = 1,
                }
            ),
            protocol.MessageType
        );
        var response = protocol.Decode(
            (await socket.ReadSentAsync(TestContext.Current.CancellationToken)).Span
        );
        Assert.Equal(HubFrameKind.Fault, response.Kind);
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
            "snapshot-event" => new HubFrame
            {
                Kind = HubFrameKind.Event,
                StreamId = 41,
                Topic = "orders.changed",
                Key = "customer-1",
                Sequence = 0,
                EventId = "snapshot-1",
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

    private static async Task<IHost> CreateTestHostAsync(
        Action<HostLoomWebSocketOptions>? configure = null,
        Action<IServiceCollection>? configureServices = null
    )
    {
        var builder = new HostBuilder().ConfigureWebHost(webHost =>
            webHost
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddRouting();
                    configureServices?.Invoke(services);
                    services
                        .AddHostLoom()
                        .UseInMemory()
                        .AddWebSocketGateway(options =>
                        {
                            options.RequireAuthenticatedUser = false;
                            configure?.Invoke(options);
                        })
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
                    {
                        endpoints.MapHostLoomWebSocketHub("/hostloom");
                    });
                })
        );

        return await builder.StartAsync(TestContext.Current.CancellationToken);
    }

    public sealed record Greet(string Name) : IRequest<Greeting>;

    public sealed record Greeting(string Text);

    public sealed record OrderChanged(string CustomerId) : IEvent;

    public sealed record StatusChanged(string Key, int Version) : IEvent;

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

    private sealed class WebSocketMetricRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly List<MetricMeasurement> _measurements = [];
        private readonly Lock _gate = new();

        public WebSocketMetricRecorder()
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == WebSocketDiagnostics.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.SetMeasurementEventCallback<double>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.Start();
        }

        public MetricMeasurement[] Measurements(string instrumentName)
        {
            lock (_gate)
            {
                return _measurements
                    .Where(measurement => measurement.Name == instrumentName)
                    .ToArray();
            }
        }

        public void Dispose() => _listener.Dispose();

        private void Record(
            Instrument instrument,
            double value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags
        )
        {
            var captured = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var tag in tags)
            {
                captured[tag.Key] = tag.Value;
            }

            lock (_gate)
            {
                _measurements.Add(new MetricMeasurement(instrument.Name, value, captured));
            }
        }
    }

    private sealed record MetricMeasurement(
        string Name,
        double Value,
        IReadOnlyDictionary<string, object?> Tags
    )
    {
        public object? Tag(string name) => Tags.GetValueOrDefault(name);
    }

    private sealed class WebSocketActivityRecorder : IDisposable
    {
        private readonly List<ActivityRecord> _activities = [];
        private readonly Lock _gate = new();
        private readonly ActivityListener _listener;

        public WebSocketActivityRecorder()
        {
            _listener = new ActivityListener
            {
                ShouldListenTo = source =>
                    source.Name == WebSocketDiagnostics.ActivitySourceName
                    || source.Name == HostLoomDiagnostics.ActivitySourceName,
                Sample = (ref ActivityCreationOptions<ActivityContext> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                SampleUsingParentId = (ref ActivityCreationOptions<string> _) =>
                    ActivitySamplingResult.AllDataAndRecorded,
                ActivityStopped = activity =>
                {
                    var tags = activity.TagObjects.ToDictionary(
                        static tag => tag.Key,
                        static tag => tag.Value,
                        StringComparer.Ordinal
                    );
                    lock (_gate)
                    {
                        _activities.Add(
                            new ActivityRecord(
                                activity.Source.Name,
                                activity.DisplayName,
                                activity.Kind,
                                activity.Status,
                                activity.TraceId,
                                activity.SpanId,
                                activity.ParentSpanId,
                                tags
                            )
                        );
                    }
                },
            };
            ActivitySource.AddActivityListener(_listener);
        }

        public IReadOnlyList<ActivityRecord> Activities
        {
            get
            {
                lock (_gate)
                {
                    return [.. _activities];
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }

    private sealed record ActivityRecord(
        string Source,
        string Name,
        ActivityKind Kind,
        ActivityStatusCode Status,
        ActivityTraceId TraceId,
        ActivitySpanId SpanId,
        ActivitySpanId ParentSpanId,
        IReadOnlyDictionary<string, object?> Tags
    )
    {
        public object? Tag(string name) => Tags.GetValueOrDefault(name);
    }

    private sealed class WebSocketLogRecorder : ILoggerProvider
    {
        private readonly List<LogEntry> _entries = [];
        private readonly Lock _gate = new();

        public IReadOnlyList<LogEntry> Entries
        {
            get
            {
                lock (_gate)
                {
                    return [.. _entries];
                }
            }
        }

        public ILogger CreateLogger(string categoryName) => new Recorder(this, categoryName);

        public void Dispose() { }

        private void Add(LogEntry entry)
        {
            lock (_gate)
            {
                _entries.Add(entry);
            }
        }

        private sealed class Recorder(WebSocketLogRecorder owner, string category) : ILogger
        {
            public IDisposable? BeginScope<TState>(TState state)
                where TState : notnull => null;

            public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

            public void Log<TState>(
                LogLevel logLevel,
                EventId eventId,
                TState state,
                Exception? exception,
                Func<TState, Exception?, string> formatter
            )
            {
                var properties = new Dictionary<string, object?>(StringComparer.Ordinal);
                if (state is IEnumerable<KeyValuePair<string, object?>> values)
                {
                    foreach (var value in values)
                    {
                        properties[value.Key] = value.Value;
                    }
                }

                owner.Add(
                    new LogEntry(
                        category,
                        logLevel,
                        eventId,
                        formatter(state, exception),
                        properties
                    )
                );
            }
        }
    }

    private sealed record LogEntry(
        string Category,
        LogLevel Level,
        EventId Event,
        string Message,
        IReadOnlyDictionary<string, object?> Properties
    )
    {
        public object? Property(string name) => Properties.GetValueOrDefault(name);

        public bool Contains(string value) =>
            Message.Contains(value, StringComparison.Ordinal)
            || Properties
                .Values.OfType<string>()
                .Any(property => property.Contains(value, StringComparison.Ordinal));
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
        private readonly TaskCompletionSource _sendBlocked = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource _releaseSends = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int _blockSends;

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

        public void BlockSends() => Volatile.Write(ref _blockSends, 1);

        public Task WaitForBlockedSendAsync(CancellationToken cancellationToken) =>
            _sendBlocked.Task.WaitAsync(cancellationToken);

        public ValueTask<ReadOnlyMemory<byte>> ReadSentAsync(CancellationToken cancellationToken) =>
            _sent.Reader.ReadAsync(cancellationToken);

        public override void Abort()
        {
            _state = WebSocketState.Aborted;
            _releaseSends.TrySetResult();
        }

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

        public override void Dispose()
        {
            _state = WebSocketState.Closed;
            _releaseSends.TrySetResult();
        }

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

        public override async Task SendAsync(
            ArraySegment<byte> buffer,
            WebSocketMessageType messageType,
            bool endOfMessage,
            CancellationToken cancellationToken
        )
        {
            if (Volatile.Read(ref _blockSends) != 0)
            {
                _sendBlocked.TrySetResult();
                await _releaseSends.Task.WaitAsync(cancellationToken);
            }

            _sent.Writer.TryWrite(buffer.ToArray());
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

    private sealed class FixedLifetimeResolver(DateTimeOffset expiration)
        : IWebSocketSessionLifetimeResolver
    {
        public ValueTask<DateTimeOffset?> ResolveExpirationAsync(
            HttpContext context,
            CancellationToken cancellationToken = default
        ) => ValueTask.FromResult<DateTimeOffset?>(expiration);
    }

    private sealed class BlockingStatusSnapshotProvider(StatusChanged snapshot)
        : IWebSocketTopicSnapshotProvider<StatusChanged>
    {
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource _started = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource _completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        public Task Started => _started.Task;

        public Task Completed => _completed.Task;

        public bool WasCanceled { get; private set; }

        public WebSocketTopicSnapshotContext? Context { get; private set; }

        public async IAsyncEnumerable<StatusChanged> GetSnapshotAsync(
            WebSocketTopicSnapshotContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            Context = context;
            _started.TrySetResult();
            try
            {
                await _release.Task.WaitAsync(cancellationToken).ConfigureAwait(false);
                yield return snapshot;
            }
            finally
            {
                WasCanceled = cancellationToken.IsCancellationRequested;
                _completed.TrySetResult();
            }
        }

        public void Release() => _release.TrySetResult();
    }

    private sealed class ListStatusSnapshotProvider(params StatusChanged[] snapshots)
        : IWebSocketTopicSnapshotProvider<StatusChanged>
    {
        public async IAsyncEnumerable<StatusChanged> GetSnapshotAsync(
            WebSocketTopicSnapshotContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            await Task.CompletedTask.ConfigureAwait(false);
            foreach (var snapshot in snapshots)
            {
                cancellationToken.ThrowIfCancellationRequested();
                yield return snapshot;
            }
        }
    }

    private sealed class FailingStatusSnapshotProvider
        : IWebSocketTopicSnapshotProvider<StatusChanged>
    {
        public async IAsyncEnumerable<StatusChanged> GetSnapshotAsync(
            WebSocketTopicSnapshotContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            await Task.FromException(new InvalidOperationException("snapshot failed"))
                .ConfigureAwait(false);
            yield break;
        }
    }
}
