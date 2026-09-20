using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using System.Security.Claims;
using HostLoom.AspNetCore.WebSockets;
using HostLoom.Transport.InMemory;
using Microsoft.AspNetCore.Authorization;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Regression coverage for the gateway's isolation boundaries: keyed-topic wildcards, oversized
/// frames, stalled snapshots, long session lifetimes, per-kind rate budgets, and authorization
/// policy failures. Shares the fakes and fixtures of the main partial.
/// </summary>
public sealed partial class WebSocketGatewayTests
{
    [Fact]
    public async Task Keyed_topic_denies_a_keyless_subscription_by_default()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var logs = new WebSocketLogRecorder();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(logs));
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

        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(71),
                Topic = "orders.changed",
                Credit = 1,
            }
        );
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        var fault = await ReadFrameAsync(socket, protocol, cancellationToken);

        Assert.Equal(HubFrameKind.Fault, fault.Kind);
        Assert.Equal(Stream(71), fault.StreamId);
        Assert.Equal(HubFaultCodes.Forbidden, fault.Code);
        Assert.Equal(
            0,
            Assert
                .Single(provider.GetRequiredService<IWebSocketSessionDirectory>().GetSessions())
                .SubscriptionCount
        );

        // No wildcard membership was created: a keyed event reaches nothing on this session, and
        // the next frame the client sees is its own pong.
        provider
            .GetRequiredService<WebSocketSessionRegistry>()
            .Publish("orders.changed", "customer-1", new byte[] { 1 });
        Send(socket, protocol, new HubFrame { Kind = HubFrameKind.Ping, StreamId = Stream(72) });
        var pong = await ReadFrameAsync(socket, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Pong, pong.Kind);
        Assert.Equal(Stream(72), pong.StreamId);

        var denied = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SubscriptionDenied
        );
        Assert.Equal("key_required", denied.Property("Reason"));
        Assert.Equal("orders.changed", denied.Property("Topic"));

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public async Task Keyed_topic_allows_a_keyless_subscription_when_the_registration_opts_in()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<OrderChanged>(
                "orders.changed",
                "orders",
                value => value.CustomerId,
                allowTopicWideSubscription: true
            );
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(73),
                Topic = "orders.changed",
                Credit = 1,
            }
        );
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        var subscribed = await ReadFrameAsync(socket, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Subscribed, subscribed.Kind);
        Assert.Null(subscribed.Key);

        provider
            .GetRequiredService<WebSocketSessionRegistry>()
            .Publish("orders.changed", "customer-1", new byte[] { 5 });
        var delivered = await ReadFrameAsync(socket, protocol, cancellationToken);

        Assert.Equal(HubFrameKind.Event, delivered.Kind);
        Assert.Equal(Stream(73), delivered.StreamId);
        Assert.Equal("customer-1", delivered.Key);

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public void Probe_reports_whether_keyed_topics_allow_topic_wide_subscriptions()
    {
        var gateway = new ServiceCollection()
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<OrderChanged>("orders.changed", "orders", value => value.CustomerId)
            .AddTopic<StatusChanged>(
                "status.changed",
                "status",
                value => value.Key,
                allowTopicWideSubscription: true
            )
            .AddTopic<CatalogChanged>("catalog.changed", "catalog");

        var description = gateway.Probe();

        Assert.Collection(
            description.Topics,
            topic =>
            {
                Assert.Equal("catalog.changed", topic.Topic);
                Assert.False(topic.Keyed);
                Assert.True(topic.AllowTopicWideSubscription);
            },
            topic =>
            {
                Assert.Equal("orders.changed", topic.Topic);
                Assert.True(topic.Keyed);
                Assert.False(topic.AllowTopicWideSubscription);
            },
            topic =>
            {
                Assert.Equal("status.changed", topic.Topic);
                Assert.True(topic.Keyed);
                Assert.True(topic.AllowTopicWideSubscription);
            }
        );
        Assert.Contains(
            description.Decisions,
            decision =>
                decision.Component == "WebSockets:Topic:orders.changed"
                && decision.Reason.Contains("topicWide=False", StringComparison.Ordinal)
        );
        Assert.Contains(
            description.Decisions,
            decision =>
                decision.Component == "WebSockets:Topic:status.changed"
                && decision.Reason.Contains("topicWide=True", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Oversized_live_event_is_dropped_without_aborting_the_session()
    {
        const string topic = "oversize.orders.changed";
        var cancellationToken = TestContext.Current.CancellationToken;
        using var metrics = new WebSocketMetricRecorder();
        var services = new ServiceCollection();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.MaximumMessageSize = 512;
            })
            .AddTopic<OrderChanged>(topic, "orders", value => value.CustomerId);
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(74),
                Topic = topic,
                Key = "customer-1",
                Credit = 2,
            }
        );
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);

        var registry = provider.GetRequiredService<WebSocketSessionRegistry>();
        registry.Publish(topic, "customer-1", new byte[600]);
        registry.Publish(topic, "customer-1", new byte[] { 9 });
        var delivered = await ReadFrameAsync(socket, protocol, cancellationToken);

        Assert.Equal(HubFrameKind.Event, delivered.Kind);
        Assert.Equal(new byte[] { 9 }, delivered.Payload!.Value.ToArray());
        Assert.Equal(WebSocketState.Open, socket.State);
        Assert.Contains(
            metrics.Measurements("hostloom.websocket.events.dropped"),
            measurement =>
                measurement.Tag(WebSocketDiagnostics.TopicTag) is topic
                && measurement.Tag(WebSocketDiagnostics.ReasonTag) is "message_too_large"
        );

        socket.EnqueueClose();
        await run;
        Assert.Equal(WebSocketCloseStatus.NormalClosure, socket.CloseStatus);
    }

    [Fact]
    public async Task Oversized_snapshot_value_faults_only_its_subscription()
    {
        const string topic = "oversize.catalog.changed";
        var cancellationToken = TestContext.Current.CancellationToken;
        using var logs = new WebSocketLogRecorder();
        using var metrics = new WebSocketMetricRecorder();
        var snapshots = new ListCatalogSnapshotProvider(
            new CatalogChanged("eu", new string('x', 2048))
        );
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(logs));
        services.AddSingleton<IWebSocketTopicSnapshotProvider<CatalogChanged>>(snapshots);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.MaximumMessageSize = 1024;
            })
            .AddTopic<OrderChanged>("orders.changed", "orders", value => value.CustomerId)
            .AddTopic<CatalogChanged>(topic, "catalog", value => value.Region)
            .AddTopicSnapshot<CatalogChanged, ListCatalogSnapshotProvider>(topic);
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(75),
                Topic = "orders.changed",
                Key = "customer-1",
                Credit = 1,
            }
        );
        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(76),
                Topic = topic,
                Key = "eu",
                Credit = 1,
            }
        );
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        var fault = await ReadFrameAsync(socket, protocol, cancellationToken);

        Assert.Equal(HubFrameKind.Fault, fault.Kind);
        Assert.Equal(Stream(76), fault.StreamId);
        Assert.Equal(HubFaultCodes.SnapshotFailed, fault.Code);
        Assert.Equal(
            1,
            Assert
                .Single(provider.GetRequiredService<IWebSocketSessionDirectory>().GetSessions())
                .SubscriptionCount
        );

        // The sibling stream is untouched and the connection still delivers.
        provider
            .GetRequiredService<WebSocketSessionRegistry>()
            .Publish("orders.changed", "customer-1", new byte[] { 3 });
        var delivered = await ReadFrameAsync(socket, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Event, delivered.Kind);
        Assert.Equal(Stream(75), delivered.StreamId);
        Assert.Equal(WebSocketState.Open, socket.State);

        Assert.Contains(
            metrics.Measurements("hostloom.websocket.events.dropped"),
            measurement =>
                measurement.Tag(WebSocketDiagnostics.TopicTag) is topic
                && measurement.Tag(WebSocketDiagnostics.ReasonTag) is "message_too_large"
        );
        var failed = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SnapshotFailed
        );
        Assert.Equal(topic, failed.Property("Topic"));

        socket.EnqueueClose();
        await run;
        Assert.Equal(WebSocketCloseStatus.NormalClosure, socket.CloseStatus);
    }

    [Fact]
    public async Task Oversized_response_returns_a_fault_and_keeps_the_session_open()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var logs = new WebSocketLogRecorder();
        var builder = Host.CreateApplicationBuilder();
        builder.Logging.AddProvider(logs);
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, VerboseGreetHandler>("greeter")
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.MaximumMessageSize = 1024;
            })
            .AddRequest<Greet, Greeting>("greet", "greeter");
        using var host = builder.Build();
        await host.StartAsync(cancellationToken);
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var session = host
            .Services.GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = Stream(77),
                Operation = "greet",
                Payload = serializer.Serialize(new Greet("Ada")),
            }
        );
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        var fault = await ReadFrameAsync(socket, protocol, cancellationToken);

        Assert.Equal(HubFrameKind.Fault, fault.Kind);
        Assert.Equal(Stream(77), fault.StreamId);
        Assert.Equal(HubFaultCodes.MessageTooLarge, fault.Code);
        Assert.Equal(WebSocketState.Open, socket.State);

        Send(socket, protocol, new HubFrame { Kind = HubFrameKind.Ping, StreamId = Stream(78) });
        var pong = await ReadFrameAsync(socket, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Pong, pong.Kind);

        var logged = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.ResponseTooLarge
        );
        Assert.Equal(LogLevel.Warning, logged.Level);
        Assert.Equal("greet", logged.Property("Operation"));
        Assert.Equal(1024, logged.Property("MaximumMessageSize"));

        socket.EnqueueClose();
        await run;
        Assert.Equal(WebSocketCloseStatus.NormalClosure, socket.CloseStatus);
        await host.StopAsync(cancellationToken);
    }

    [Fact]
    public async Task Snapshot_initialization_faults_when_credit_is_withheld_past_the_timeout()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        using var logs = new WebSocketLogRecorder();
        var snapshots = new TrackingStatusSnapshotProvider(
            new StatusChanged("customer-1", 1),
            new StatusChanged("customer-1", 2)
        );
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(logs));
        services.AddSingleton<TimeProvider>(clock);
        services.AddSingleton<IWebSocketTopicSnapshotProvider<StatusChanged>>(snapshots);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.SnapshotInitializationTimeout = TimeSpan.FromSeconds(5);
            })
            .AddTopic<StatusChanged>("status.changed", "status", value => value.Key)
            .AddTopicSnapshot<StatusChanged, TrackingStatusSnapshotProvider>("status.changed");
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(79),
                Topic = "status.changed",
                Key = "customer-1",
                Credit = 1,
            }
        );
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        var first = await ReadFrameAsync(socket, protocol, cancellationToken);
        Assert.Equal(0, first.Sequence);

        // The client never adds credit. Two timers are armed: session expiry and the
        // initialization timeout that now bounds the wait for credit.
        Assert.Equal(2, clock.PendingTimers);
        clock.Advance(TimeSpan.FromSeconds(5));
        var fault = await ReadFrameAsync(socket, protocol, cancellationToken);
        await snapshots.Completed.WaitAsync(cancellationToken);

        Assert.Equal(HubFrameKind.Fault, fault.Kind);
        Assert.Equal(Stream(79), fault.StreamId);
        Assert.Equal(HubFaultCodes.SnapshotStalled, fault.Code);
        Assert.True(snapshots.WasCanceled);
        // The timeout timer was disposed with the initialization; only session expiry remains.
        Assert.Equal(1, clock.PendingTimers);
        Assert.Equal(
            0,
            Assert
                .Single(provider.GetRequiredService<IWebSocketSessionDirectory>().GetSessions())
                .SubscriptionCount
        );
        var stalled = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SnapshotStalled
        );
        Assert.Equal(LogLevel.Warning, stalled.Level);
        Assert.Equal("status.changed", stalled.Property("Topic"));
        Assert.Equal(session.Id, stalled.Property("SessionId"));

        // Late credit cannot resume the ended stream; it is an invalid frame like any other.
        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Credit,
                StreamId = Stream(79),
                Credit = 1,
            }
        );
        var late = await ReadFrameAsync(socket, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Fault, late.Kind);
        Assert.Equal(HubFaultCodes.InvalidFrame, late.Code);

        socket.EnqueueClose();
        await run;
    }

    [Fact]
    public async Task Session_lifetime_beyond_the_timer_limit_expires_through_chunked_delays()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.MaximumSessionLifetime = TimeSpan.FromDays(400);
            });
        await using var provider = services.BuildServiceProvider();
        using var socket = new ScriptedWebSocket();
        var protocol = new JsonWebSocketHubProtocol();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        var directory = provider.GetRequiredService<IWebSocketSessionDirectory>();
        var info = Assert.Single(directory.GetSessions());
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromDays(400), info.ExpiresAt);
        Assert.Equal(1, clock.PendingTimers);

        clock.Advance(TimeSpan.FromDays(400));
        await run.WaitAsync(cancellationToken);

        Assert.Equal(WebSocketCloseStatus.PolicyViolation, socket.CloseStatus);
        Assert.Equal("session_expired", socket.CloseStatusDescription);
        Assert.Equal(0, directory.Count);
        Assert.Equal(0, clock.PendingTimers);
    }

    [Fact]
    public async Task Unbounded_session_lifetime_arms_no_timer_and_still_closes_cleanly()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.MaximumSessionLifetime = TimeSpan.MaxValue;
            });
        await using var provider = services.BuildServiceProvider();
        using var socket = new ScriptedWebSocket();
        var protocol = new JsonWebSocketHubProtocol();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        var directory = provider.GetRequiredService<IWebSocketSessionDirectory>();
        Assert.Equal(DateTimeOffset.MaxValue, Assert.Single(directory.GetSessions()).ExpiresAt);
        Assert.Equal(0, clock.PendingTimers);

        socket.EnqueueClose();
        await run.WaitAsync(cancellationToken);

        Assert.Equal(WebSocketCloseStatus.NormalClosure, socket.CloseStatus);
        Assert.Equal(0, directory.Count);
    }

    [Fact]
    public async Task Expiry_timer_failure_still_runs_session_cleanup()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var logs = new WebSocketLogRecorder();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(logs));
        services.AddSingleton<TimeProvider>(new BrokenTimerClock());
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddTopic<OrderChanged>("orders.changed", "orders", value => value.CustomerId);
        await using var provider = services.BuildServiceProvider();
        using var socket = new ScriptedWebSocket();
        var protocol = new JsonWebSocketHubProtocol();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(80),
                Topic = "orders.changed",
                Key = "customer-1",
                Credit = 1,
            }
        );
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        var subscribed = await ReadFrameAsync(socket, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Subscribed, subscribed.Kind);

        socket.EnqueueClose();
        await run.WaitAsync(cancellationToken);

        // The close handshake and unregistration prove the ordinary teardown ran even though the
        // expiry task faulted before the first frame.
        Assert.Equal(WebSocketCloseStatus.NormalClosure, socket.CloseStatus);
        Assert.Equal(0, provider.GetRequiredService<IWebSocketSessionDirectory>().Count);
        var failed = Assert.Single(
            logs.Entries,
            entry => entry.Event == WebSocketEvents.SessionExpiryFailed
        );
        Assert.Equal(LogLevel.Error, failed.Level);
        Assert.Equal(session.Id, failed.Property("SessionId"));

        // Nothing is left in the registry for the closed session.
        provider
            .GetRequiredService<WebSocketSessionRegistry>()
            .Publish("orders.changed", "customer-1", new byte[] { 1 });
        Assert.False(socket.TryReadSent(out _));
    }

    [Fact]
    public async Task Request_frames_have_their_own_rate_budget()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway(options =>
            {
                options.RequireAuthenticatedUser = false;
                options.MaximumRequestsPerSecond = 2;
                options.MaximumControlFramesPerSecond = 1;
            });
        await using var provider = services.BuildServiceProvider();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);

        // Unregistered operations count against the request budget; they never reach a scope.
        await SendUnregisteredRequestAndAwaitFaultAsync(socket, protocol, 81, cancellationToken);
        await SendUnregisteredRequestAndAwaitFaultAsync(socket, protocol, 82, cancellationToken);

        // The control budget is separate: with requests exhausted, a ping is still answered.
        Send(socket, protocol, new HubFrame { Kind = HubFrameKind.Ping, StreamId = Stream(83) });
        var pong = await ReadFrameAsync(socket, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Pong, pong.Kind);

        clock.Advance(TimeSpan.FromSeconds(1));
        await SendUnregisteredRequestAndAwaitFaultAsync(socket, protocol, 84, cancellationToken);
        await SendUnregisteredRequestAndAwaitFaultAsync(socket, protocol, 85, cancellationToken);
        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = Stream(86),
                Operation = "missing",
                Payload = new byte[] { 1 },
            }
        );
        await run.WaitAsync(cancellationToken);

        Assert.Equal(WebSocketCloseStatus.PolicyViolation, socket.CloseStatus);
        Assert.Equal("rate_limited", socket.CloseStatusDescription);
    }

    [Fact]
    public async Task Client_invalid_frame_kinds_count_toward_the_control_frame_budget()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var services = new ServiceCollection();
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
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);

        for (var stream = 87; stream <= 88; stream++)
        {
            Send(
                socket,
                protocol,
                new HubFrame { Kind = HubFrameKind.Pong, StreamId = Stream(stream) }
            );
            var fault = await ReadFrameAsync(socket, protocol, cancellationToken);
            Assert.Equal(HubFrameKind.Fault, fault.Kind);
            Assert.Equal(HubFaultCodes.InvalidFrame, fault.Code);
        }

        Send(socket, protocol, new HubFrame { Kind = HubFrameKind.Pong, StreamId = Stream(89) });
        await run.WaitAsync(cancellationToken);

        Assert.Equal(WebSocketCloseStatus.PolicyViolation, socket.CloseStatus);
        Assert.Equal("rate_limited", socket.CloseStatusDescription);
    }

    [Fact]
    public void Gateway_options_validate_the_new_budgets()
    {
        var services = new ServiceCollection();
        var hostLoom = services.AddHostLoom().UseInMemory();

        _ = Assert.Throws<ArgumentOutOfRangeException>(() =>
            hostLoom.AddWebSocketGateway(options => options.MaximumRequestsPerSecond = 0)
        );
        _ = Assert.Throws<InvalidOperationException>(() =>
            hostLoom.AddWebSocketGateway(options =>
                options.SnapshotInitializationTimeout = TimeSpan.Zero
            )
        );
        _ = Assert.Throws<InvalidOperationException>(() =>
            hostLoom.AddWebSocketGateway(options =>
                options.SnapshotInitializationTimeout = TimeSpan.FromDays(60)
            )
        );
    }

    [Fact]
    public async Task Unregistered_authorization_policy_fails_host_startup()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddRequest<Greet, Greeting>("greet", "greeter", "greetings.read")
            .AddTopic<OrderChanged>(
                "orders.changed",
                "orders",
                value => value.CustomerId,
                authorizationPolicy: "orders.missing"
            )
            .AddTopic<StatusChanged>(
                "status.changed",
                "status",
                value => value.Key,
                authorizationPolicy: TopicKeyPolicy.SubjectOnly
            );
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            host.StartAsync(cancellationToken)
        );

        Assert.Contains(
            "'greetings.read' (used by request 'greet')",
            exception.Message,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "'orders.missing' (used by topic 'orders.changed')",
            exception.Message,
            StringComparison.Ordinal
        );
        Assert.DoesNotContain(
            TopicKeyPolicy.SubjectOnly,
            exception.Message,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task Authorization_handler_failure_maps_to_a_forbidden_fault()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var logs = new WebSocketLogRecorder();
        var services = new ServiceCollection();
        services.AddLogging(builder => builder.AddProvider(logs));
        services.AddAuthorization(options =>
            options.AddPolicy(
                "orders.audited",
                policy => policy.AddRequirements(new AuditedRequirement())
            )
        );
        services.AddSingleton<IAuthorizationHandler, FailingAuditHandler>();
        services
            .AddHostLoom()
            .UseInMemory()
            .AddHandler<Greet, Greeting, GreetHandler>("greeter")
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddRequest<Greet, Greeting>("greet", "greeter", "orders.audited")
            .AddTopic<OrderChanged>(
                "orders.changed",
                "orders",
                value => value.CustomerId,
                authorizationPolicy: "orders.audited"
            );
        await using var provider = services.BuildServiceProvider();
        var serializer = provider.GetRequiredService<IMessageSerializer>();
        var protocol = new JsonWebSocketHubProtocol();
        using var socket = new ScriptedWebSocket();
        var session = provider
            .GetRequiredService<WebSocketSessionFactory>()
            .Create(socket, protocol, new ClaimsPrincipal());

        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(90),
                Topic = "orders.changed",
                Key = "customer-1",
                Credit = 1,
            }
        );
        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = Stream(91),
                Operation = "greet",
                Payload = serializer.Serialize(new Greet("Ada")),
            }
        );
        var run = session.RunAsync(cancellationToken);
        _ = await socket.ReadSentAsync(cancellationToken);
        var subscribeFault = await ReadFrameAsync(socket, protocol, cancellationToken);
        var requestFault = await ReadFrameAsync(socket, protocol, cancellationToken);

        Assert.Equal(HubFrameKind.Fault, subscribeFault.Kind);
        Assert.Equal(Stream(90), subscribeFault.StreamId);
        Assert.Equal(HubFaultCodes.Forbidden, subscribeFault.Code);
        Assert.Equal(HubFrameKind.Fault, requestFault.Kind);
        Assert.Equal(Stream(91), requestFault.StreamId);
        Assert.Equal(HubFaultCodes.Forbidden, requestFault.Code);
        Assert.Equal(WebSocketState.Open, socket.State);

        var failures = logs
            .Entries.Where(entry => entry.Event == WebSocketEvents.AuthorizationFailed)
            .ToArray();
        Assert.Equal(2, failures.Length);
        Assert.All(failures, entry => Assert.Equal("orders.audited", entry.Property("Policy")));
        Assert.All(failures, entry => Assert.Equal(LogLevel.Error, entry.Level));

        socket.EnqueueClose();
        await run;
    }

    private static void Send(
        ScriptedWebSocket socket,
        IWebSocketHubProtocol protocol,
        HubFrame frame
    ) => socket.Enqueue(protocol.Encode(frame), protocol.MessageType);

    private static async ValueTask<HubFrame> ReadFrameAsync(
        ScriptedWebSocket socket,
        IWebSocketHubProtocol protocol,
        CancellationToken cancellationToken
    ) => protocol.Decode((await socket.ReadSentAsync(cancellationToken)).Span);

    private static async Task SendUnregisteredRequestAndAwaitFaultAsync(
        ScriptedWebSocket socket,
        IWebSocketHubProtocol protocol,
        int stream,
        CancellationToken cancellationToken
    )
    {
        Send(
            socket,
            protocol,
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = Stream(stream),
                Operation = "missing",
                Payload = new byte[] { 1 },
            }
        );
        var fault = await ReadFrameAsync(socket, protocol, cancellationToken);
        Assert.Equal(HubFrameKind.Fault, fault.Kind);
        Assert.Equal(Stream(stream), fault.StreamId);
        Assert.Equal(HubFaultCodes.OperationNotFound, fault.Code);
    }

    public sealed record CatalogChanged(string Region, string Description) : IEvent;

    public sealed class VerboseGreetHandler : IRequestHandler<Greet, Greeting>
    {
        public ValueTask<Greeting> HandleAsync(
            Greet request,
            CancellationToken cancellationToken
        ) => ValueTask.FromResult(new Greeting(new string('!', 4096)));
    }

    private sealed class ListCatalogSnapshotProvider(params CatalogChanged[] snapshots)
        : IWebSocketTopicSnapshotProvider<CatalogChanged>
    {
        public async IAsyncEnumerable<CatalogChanged> GetSnapshotAsync(
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

    /// <summary>Yields values while recording whether enumeration ended by cancellation.</summary>
    private sealed class TrackingStatusSnapshotProvider(params StatusChanged[] snapshots)
        : IWebSocketTopicSnapshotProvider<StatusChanged>
    {
        private readonly TaskCompletionSource _completed = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        public Task Completed => _completed.Task;

        public bool WasCanceled { get; private set; }

        public async IAsyncEnumerable<StatusChanged> GetSnapshotAsync(
            WebSocketTopicSnapshotContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            try
            {
                await Task.CompletedTask.ConfigureAwait(false);
                foreach (var snapshot in snapshots)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    yield return snapshot;
                }
            }
            finally
            {
                WasCanceled = cancellationToken.IsCancellationRequested;
                _completed.TrySetResult();
            }
        }
    }

    /// <summary>A clock whose timers cannot be created, so any timer-based wait faults.</summary>
    private sealed class BrokenTimerClock : TimeProvider
    {
        public override DateTimeOffset GetUtcNow() => DateTimeOffset.UnixEpoch;

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period
        ) => throw new InvalidOperationException("The timer could not be created.");
    }

    private sealed class AuditedRequirement : IAuthorizationRequirement;

    private sealed class FailingAuditHandler : AuthorizationHandler<AuditedRequirement>
    {
        protected override Task HandleRequirementAsync(
            AuthorizationHandlerContext context,
            AuditedRequirement requirement
        ) => throw new InvalidOperationException("The audit store is unavailable.");
    }
}
