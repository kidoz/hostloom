using System.Net.WebSockets;
using System.Runtime.CompilerServices;
using HostLoom.AspNetCore.WebSockets;
using HostLoom.AspNetCore.WebSockets.Testing;
using HostLoom.Transport.InMemory;
using Microsoft.AspNetCore.Authorization;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.AspNetCore.TestHost;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class WebSocketHubTests
{
    [Fact]
    public void Hub_composes_existing_request_channel_and_snapshot_registrations()
    {
        var snapshotProviderResolved = false;
        var services = new ServiceCollection();
        services.AddScoped<IWebSocketTopicSnapshotProvider<OrderChanged>>(_ =>
        {
            snapshotProviderResolved = true;
            return new OrderSnapshotProvider();
        });

        var description = services
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway()
            .AddHub(
                "orders",
                hub =>
                {
                    hub.AddRequest<GetOrder, OrderView>(
                        "get",
                        "orders-api",
                        authorizationPolicy: "orders.read"
                    );
                    hub.AddChannel<OrderChanged>(
                            "changed",
                            "orders",
                            changed => changed.CustomerId,
                            subscription: "realtime-node-a",
                            authorizationPolicy: TopicKeyPolicy.SubjectOnly
                        )
                        .AddChannelSnapshot<OrderChanged, OrderSnapshotProvider>("changed");
                }
            )
            .Probe();

        var request = Assert.Single(description.Requests);
        Assert.Equal("orders.get", request.Operation);
        Assert.Equal("orders-api", request.Destination);
        Assert.Equal(typeof(GetOrder).FullName, request.RequestType);
        Assert.Equal(typeof(OrderView).FullName, request.ResponseType);
        Assert.Equal("orders.read", request.AuthorizationPolicy);

        var channel = Assert.Single(description.Topics);
        Assert.Equal("orders.changed", channel.Topic);
        Assert.Equal("orders", channel.Source);
        Assert.Equal("realtime-node-a", channel.Subscription);
        Assert.Equal(typeof(OrderChanged).FullName, channel.EventType);
        Assert.True(channel.Keyed);
        Assert.False(channel.AllowTopicWideSubscription);
        Assert.Equal(TopicKeyPolicy.SubjectOnly, channel.AuthorizationPolicy);
        Assert.Equal(typeof(OrderSnapshotProvider).FullName, channel.SnapshotProvider);
        Assert.False(snapshotProviderResolved);
    }

    [Fact]
    public void Hub_and_direct_registrations_coexist()
    {
        var description = new ServiceCollection()
            .AddHostLoom()
            .UseInMemory()
            .AddWebSocketGateway()
            .AddHub(
                "orders",
                hub =>
                {
                    hub.AddRequest<GetOrder, OrderView>("get", "orders-api");
                    hub.AddChannel<OrderChanged>(
                        "changed",
                        "orders",
                        changed => changed.CustomerId
                    );
                }
            )
            .AddRequest<GetCatalog, CatalogView>("catalog.get", "catalog-api")
            .AddTopic<CatalogChanged>("catalog.changed", "catalog")
            .AddTopicSnapshot<CatalogChanged, CatalogSnapshotProvider>("catalog.changed")
            .Probe();

        Assert.Equal(
            ["catalog.get", "orders.get"],
            description.Requests.Select(request => request.Operation)
        );
        Assert.Equal(
            ["catalog.changed", "orders.changed"],
            description.Topics.Select(topic => topic.Topic)
        );
    }

    [Fact]
    public void Hub_requires_a_configuration_callback()
    {
        var gateway = CreateGateway();

        Assert.Throws<ArgumentNullException>(() => gateway.AddHub("orders", null!));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(".orders")]
    [InlineData("orders.")]
    [InlineData("orders/read")]
    public void Hub_rejects_invalid_names_even_when_empty(string? name)
    {
        var gateway = CreateGateway();

        Assert.ThrowsAny<ArgumentException>(() => gateway.AddHub(name!, _ => { }));
    }

    [Theory]
    [InlineData(null)]
    [InlineData("")]
    [InlineData(" ")]
    [InlineData(".get")]
    [InlineData("get.")]
    [InlineData("get/read")]
    public void Hub_rejects_invalid_relative_route_names(string? operation)
    {
        var gateway = CreateGateway();

        Assert.ThrowsAny<ArgumentException>(() =>
            gateway.AddHub(
                "orders",
                hub => hub.AddRequest<GetOrder, OrderView>(operation!, "orders-api")
            )
        );
    }

    [Fact]
    public void Hub_rejects_an_overlength_composed_route_name()
    {
        var gateway = CreateGateway();
        var hubName = new string('a', 120);

        var exception = Assert.ThrowsAny<ArgumentException>(() =>
            gateway.AddHub(
                hubName,
                hub => hub.AddRequest<GetOrder, OrderView>("get-order", "orders-api")
            )
        );

        Assert.Contains("at most 128", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Hub_and_direct_registrations_reject_duplicate_public_names()
    {
        var gateway = CreateGateway()
            .AddHub("orders", hub => hub.AddRequest<GetOrder, OrderView>("get", "orders-api"));

        Assert.Throws<InvalidOperationException>(() =>
            gateway.AddRequest<GetOrder, OrderView>("orders.get", "orders-api")
        );
        Assert.Throws<InvalidOperationException>(() =>
            gateway.AddHub(
                "orders",
                hub => hub.AddRequest<GetOrder, OrderView>("get", "orders-api")
            )
        );
    }

    [Fact]
    public void Hub_channels_reject_a_second_public_topic_for_the_same_event_type()
    {
        var gateway = CreateGateway()
            .AddHub(
                "orders",
                hub =>
                    hub.AddChannel<OrderChanged>("changed", "orders", changed => changed.CustomerId)
            );

        Assert.Throws<InvalidOperationException>(() =>
            gateway.AddTopic<OrderChanged>(
                "orders.audited",
                "orders",
                changed => changed.CustomerId
            )
        );
    }

    [Fact]
    public void Hub_snapshots_require_one_matching_registered_channel()
    {
        var gateway = CreateGateway();

        Assert.Throws<InvalidOperationException>(() =>
            gateway.AddHub(
                "orders",
                hub => hub.AddChannelSnapshot<OrderChanged, OrderSnapshotProvider>("changed")
            )
        );

        gateway.AddHub(
            "orders",
            hub => hub.AddChannel<OrderChanged>("changed", "orders", changed => changed.CustomerId)
        );
        Assert.Throws<InvalidOperationException>(() =>
            gateway.AddHub(
                "orders",
                hub => hub.AddChannelSnapshot<CatalogChanged, CatalogSnapshotProvider>("changed")
            )
        );

        gateway.AddHub(
            "orders",
            hub => hub.AddChannelSnapshot<OrderChanged, OrderSnapshotProvider>("changed")
        );
        Assert.Throws<InvalidOperationException>(() =>
            gateway.AddHub(
                "orders",
                hub => hub.AddChannelSnapshot<OrderChanged, OrderSnapshotProvider>("changed")
            )
        );
    }

    [Fact(Timeout = 10_000)]
    public async Task Hub_authorization_policies_participate_in_startup_validation()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .AddHandler<GetOrder, OrderView, GetOrderHandler>("orders-api")
            .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
            .AddHub(
                "orders",
                hub =>
                {
                    hub.AddRequest<GetOrder, OrderView>(
                        "get",
                        "orders-api",
                        authorizationPolicy: "orders.read"
                    );
                    hub.AddChannel<OrderChanged>(
                        "changed",
                        "orders",
                        changed => changed.CustomerId,
                        authorizationPolicy: "orders.missing"
                    );
                }
            );
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<InvalidOperationException>(() =>
            host.StartAsync(cancellationToken)
        );

        Assert.Contains(
            "'orders.read' (used by request 'orders.get')",
            exception.Message,
            StringComparison.Ordinal
        );
        Assert.Contains(
            "'orders.missing' (used by topic 'orders.changed')",
            exception.Message,
            StringComparison.Ordinal
        );
    }

    [Fact(Timeout = 10_000)]
    public async Task Hub_request_round_trips_and_an_unknown_operation_faults()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var host = await StartHubHostAsync();
        await using var client = await ConnectAsync(host, cancellationToken);
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();

        await client.SendAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = Stream(1),
                Operation = "orders.get",
                Payload = serializer.Serialize(new GetOrder("A-1")),
            },
            cancellationToken
        );
        var response = await client.ReceiveAsync(
            HubFrameKind.Response,
            cancellationToken: cancellationToken
        );

        Assert.Equal(Stream(1), response.StreamId);
        Assert.Equal(
            "A-1",
            serializer.Deserialize<OrderView>(response.Payload!.Value.Span)?.Reference
        );

        await client.SendAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Request,
                StreamId = Stream(2),
                Operation = "orders.missing",
                Payload = serializer.Serialize(new GetOrder("A-2")),
            },
            cancellationToken
        );
        var fault = await client.AwaitFaultAsync(Stream(2), cancellationToken);

        Assert.Equal(HubFaultCodes.OperationNotFound, fault.Code);
    }

    [Fact(Timeout = 10_000)]
    public async Task Hub_channel_delivers_only_to_subscribers_for_the_matching_key()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var host = await StartHubHostAsync();
        await using var first = await ConnectAsync(host, cancellationToken);
        await using var second = await ConnectAsync(host, cancellationToken);
        await using var otherKey = await ConnectAsync(host, cancellationToken);
        await using var nonSubscriber = await ConnectAsync(host, cancellationToken);
        await SubscribeAsync(first, Stream(1), "customer-1", cancellationToken);
        await SubscribeAsync(second, Stream(1), "customer-1", cancellationToken);
        await SubscribeAsync(otherKey, Stream(1), "customer-2", cancellationToken);

        await host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", new OrderChanged("customer-1", 1), cancellationToken);

        Assert.Equal(1, await ReadVersionAsync(first, Stream(1), host.Services, cancellationToken));
        Assert.Equal(
            1,
            await ReadVersionAsync(second, Stream(1), host.Services, cancellationToken)
        );
        await PingAsync(otherKey, Stream(2), cancellationToken);
        await PingAsync(nonSubscriber, Stream(1), cancellationToken);

        await host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", new OrderChanged("customer-2", 2), cancellationToken);

        Assert.Equal(
            2,
            await ReadVersionAsync(otherKey, Stream(1), host.Services, cancellationToken)
        );
        await PingAsync(first, Stream(2), cancellationToken);
        await PingAsync(second, Stream(2), cancellationToken);
        await PingAsync(nonSubscriber, Stream(2), cancellationToken);
    }

    [Fact(Timeout = 10_000)]
    public async Task Hub_channel_snapshot_precedes_live_events_buffered_during_initialization()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var snapshots = new BlockingOrderSnapshotProvider();
        using var host = await StartHubHostAsync(
            services =>
                services.AddSingleton<IWebSocketTopicSnapshotProvider<OrderChanged>>(snapshots),
            hub => hub.AddChannelSnapshot<OrderChanged, BlockingOrderSnapshotProvider>("changed")
        );
        await using var client = await ConnectAsync(host, cancellationToken);
        await SubscribeAsync(client, Stream(1), "customer-1", cancellationToken);
        await snapshots.Started.Task.WaitAsync(cancellationToken);

        await host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", new OrderChanged("customer-1", 2), cancellationToken);
        snapshots.Release.TrySetResult();

        var snapshot = await client.AwaitEventAsync(Stream(1), cancellationToken);
        var live = await client.AwaitEventAsync(Stream(1), cancellationToken);
        var serializer = host.Services.GetRequiredService<IMessageSerializer>();

        Assert.Equal(0, snapshot.Sequence);
        Assert.Equal(
            1,
            serializer.Deserialize<OrderChanged>(snapshot.Payload!.Value.Span)?.Version
        );
        Assert.True(live.Sequence > 0);
        Assert.Equal(2, serializer.Deserialize<OrderChanged>(live.Payload!.Value.Span)?.Version);
        Assert.Equal("customer-1", snapshots.Context?.Key);
    }

    [Fact(Timeout = 10_000)]
    public async Task Hub_channel_denies_a_forbidden_key_before_snapshot_or_membership_work()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var snapshots = new CountingOrderSnapshotProvider();
        using var host = await StartHubHostAsync(
            services =>
                services.AddSingleton<IWebSocketTopicSnapshotProvider<OrderChanged>>(snapshots),
            hub => hub.AddChannelSnapshot<OrderChanged, CountingOrderSnapshotProvider>("changed")
        );
        await using var client = await ConnectAsync(host, cancellationToken);

        await client.SendAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = Stream(1),
                Topic = "orders.changed",
                Key = "customer-3",
                Credit = 4,
            },
            cancellationToken
        );
        var fault = await client.AwaitFaultAsync(Stream(1), cancellationToken);

        Assert.Equal(HubFaultCodes.Forbidden, fault.Code);
        Assert.Equal(0, snapshots.Calls);
        Assert.Equal(
            0,
            Assert
                .Single(
                    host.Services.GetRequiredService<IWebSocketSessionDirectory>().GetSessions()
                )
                .SubscriptionCount
        );
    }

    [Fact(Timeout = 10_000)]
    public async Task Hub_channel_unsubscribes_one_stream_then_cleans_up_the_last_membership()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var host = await StartHubHostAsync();
        await using var client = await ConnectAsync(host, cancellationToken);
        await SubscribeAsync(client, Stream(1), "customer-1", cancellationToken);
        await SubscribeAsync(client, Stream(2), "customer-1", cancellationToken);
        var publisher = host.Services.GetRequiredService<IPublishEndpoint>();

        await publisher.PublishAsync(
            "orders",
            new OrderChanged("customer-1", 1),
            cancellationToken
        );
        Assert.Equal(
            1,
            await ReadVersionAsync(client, Stream(1), host.Services, cancellationToken)
        );
        Assert.Equal(
            1,
            await ReadVersionAsync(client, Stream(2), host.Services, cancellationToken)
        );

        await UnsubscribeAsync(client, Stream(1), cancellationToken);
        Assert.Equal(1, Assert.Single(GetSessions(host)).SubscriptionCount);
        await publisher.PublishAsync(
            "orders",
            new OrderChanged("customer-1", 2),
            cancellationToken
        );
        Assert.Equal(
            2,
            await ReadVersionAsync(client, Stream(2), host.Services, cancellationToken)
        );
        await PingAsync(client, Stream(3), cancellationToken);

        await UnsubscribeAsync(client, Stream(2), cancellationToken);
        Assert.Equal(0, Assert.Single(GetSessions(host)).SubscriptionCount);
        await publisher.PublishAsync(
            "orders",
            new OrderChanged("customer-1", 3),
            cancellationToken
        );
        await PingAsync(client, Stream(4), cancellationToken);
    }

    [Fact(Timeout = 10_000)]
    public async Task Hub_channel_unsubscribe_cancels_snapshot_initialization()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var snapshots = new BlockingOrderSnapshotProvider();
        using var host = await StartHubHostAsync(
            services =>
                services.AddSingleton<IWebSocketTopicSnapshotProvider<OrderChanged>>(snapshots),
            hub => hub.AddChannelSnapshot<OrderChanged, BlockingOrderSnapshotProvider>("changed")
        );
        await using var client = await ConnectAsync(host, cancellationToken);
        await SubscribeAsync(client, Stream(1), "customer-1", cancellationToken);
        await snapshots.Started.Task.WaitAsync(cancellationToken);

        await UnsubscribeAsync(client, Stream(1), cancellationToken);
        await snapshots.Completed.Task.WaitAsync(cancellationToken);

        Assert.True(snapshots.WasCanceled);
        Assert.Equal(0, Assert.Single(GetSessions(host)).SubscriptionCount);
    }

    [Fact(Timeout = 10_000)]
    public async Task Hub_channel_disconnect_cleans_up_session_membership()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        using var host = await StartHubHostAsync();
        await using var client = await ConnectAsync(host, cancellationToken);
        await SubscribeAsync(client, Stream(1), "customer-1", cancellationToken);
        Assert.Equal(1, Assert.Single(GetSessions(host)).SubscriptionCount);

        await client.Socket.CloseAsync(
            WebSocketCloseStatus.NormalClosure,
            "done",
            cancellationToken
        );
        var directory = host.Services.GetRequiredService<IWebSocketSessionDirectory>();
        while (directory.Count != 0)
        {
            cancellationToken.ThrowIfCancellationRequested();
            await Task.Yield();
        }

        Assert.Equal(0, directory.Count);
    }

    private static HostLoomWebSocketBuilder CreateGateway() =>
        new ServiceCollection().AddHostLoom().UseInMemory().AddWebSocketGateway();

    private static async Task<IHost> StartHubHostAsync(
        Action<IServiceCollection>? configureServices = null,
        Action<HostLoomWebSocketHubBuilder>? configureHub = null
    )
    {
        var builder = new HostBuilder().ConfigureWebHost(webHost =>
            webHost
                .UseTestServer()
                .ConfigureServices(services =>
                {
                    services.AddRouting();
                    services.AddAuthorization(options =>
                        options.AddPolicy(
                            "orders.customer",
                            policy => policy.AddRequirements(new CustomerKeyRequirement())
                        )
                    );
                    services.AddSingleton<IAuthorizationHandler, CustomerKeyAuthorizationHandler>();
                    configureServices?.Invoke(services);
                    services
                        .AddHostLoom()
                        .UseInMemory()
                        .AddHandler<GetOrder, OrderView, GetOrderHandler>("orders-api")
                        .AddWebSocketGateway(options => options.RequireAuthenticatedUser = false)
                        .AddHub(
                            "orders",
                            hub =>
                            {
                                hub.AddRequest<GetOrder, OrderView>("get", "orders-api");
                                hub.AddChannel<OrderChanged>(
                                    "changed",
                                    "orders",
                                    changed => changed.CustomerId,
                                    authorizationPolicy: "orders.customer"
                                );
                                configureHub?.Invoke(hub);
                            }
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

    private static async Task<WebSocketTestClient> ConnectAsync(
        IHost host,
        CancellationToken cancellationToken
    )
    {
        var client = new WebSocketTestClient(host.GetTestServer());
        await client.ConnectAsync(new Uri("ws://localhost/hostloom"), cancellationToken);
        await client.AwaitWelcomeAsync(cancellationToken);
        return client;
    }

    private static async Task SubscribeAsync(
        WebSocketTestClient client,
        Guid stream,
        string key,
        CancellationToken cancellationToken
    )
    {
        await client.SendAsync(
            new HubFrame
            {
                Kind = HubFrameKind.Subscribe,
                StreamId = stream,
                Topic = "orders.changed",
                Key = key,
                Credit = 16,
            },
            cancellationToken
        );
        await client.AwaitSubscribedAsync(stream, cancellationToken);
    }

    private static async Task UnsubscribeAsync(
        WebSocketTestClient client,
        Guid stream,
        CancellationToken cancellationToken
    )
    {
        await client.SendAsync(
            new HubFrame { Kind = HubFrameKind.Unsubscribe, StreamId = stream },
            cancellationToken
        );
        var complete = await client.ReceiveAsync(
            HubFrameKind.Complete,
            cancellationToken: cancellationToken
        );
        Assert.Equal(stream, complete.StreamId);
    }

    private static async Task PingAsync(
        WebSocketTestClient client,
        Guid stream,
        CancellationToken cancellationToken
    )
    {
        await client.SendAsync(
            new HubFrame { Kind = HubFrameKind.Ping, StreamId = stream },
            cancellationToken
        );
        var pong = await client.ReceiveAsync(
            HubFrameKind.Pong,
            cancellationToken: cancellationToken
        );
        Assert.Equal(stream, pong.StreamId);
    }

    private static async Task<int> ReadVersionAsync(
        WebSocketTestClient client,
        Guid stream,
        IServiceProvider services,
        CancellationToken cancellationToken
    )
    {
        var frame = await client.AwaitEventAsync(stream, cancellationToken);
        var serializer = services.GetRequiredService<IMessageSerializer>();
        return serializer.Deserialize<OrderChanged>(frame.Payload!.Value.Span)?.Version ?? -1;
    }

    private static IReadOnlyList<WebSocketSessionInfo> GetSessions(IHost host) =>
        host.Services.GetRequiredService<IWebSocketSessionDirectory>().GetSessions();

    private static Guid Stream(int value) => new(value, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);

    private sealed record GetOrder(string Reference) : IRequest<OrderView>;

    private sealed record OrderView(string Reference);

    private sealed record GetCatalog(string Reference) : IRequest<CatalogView>;

    private sealed record CatalogView(string Reference);

    private sealed record OrderChanged(string CustomerId, int Version = 0) : IEvent;

    private sealed record CatalogChanged(string Reference) : IEvent;

    private sealed class GetOrderHandler : IRequestHandler<GetOrder, OrderView>
    {
        public ValueTask<OrderView> HandleAsync(
            GetOrder request,
            CancellationToken cancellationToken
        ) => ValueTask.FromResult(new OrderView(request.Reference));
    }

    private sealed class CustomerKeyRequirement : IAuthorizationRequirement;

    private sealed class CustomerKeyAuthorizationHandler
        : AuthorizationHandler<CustomerKeyRequirement>
    {
        protected override Task HandleRequirementAsync(
            AuthorizationHandlerContext context,
            CustomerKeyRequirement requirement
        )
        {
            if (context.Resource is WebSocketTopicResource { Key: "customer-1" or "customer-2" })
            {
                context.Succeed(requirement);
            }

            return Task.CompletedTask;
        }
    }

    private class OrderSnapshotProvider : IWebSocketTopicSnapshotProvider<OrderChanged>
    {
        public virtual async IAsyncEnumerable<OrderChanged> GetSnapshotAsync(
            WebSocketTopicSnapshotContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            await Task.CompletedTask;
            yield break;
        }
    }

    private sealed class CountingOrderSnapshotProvider : OrderSnapshotProvider
    {
        public int Calls { get; private set; }

        public override async IAsyncEnumerable<OrderChanged> GetSnapshotAsync(
            WebSocketTopicSnapshotContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            Calls++;
            await Task.CompletedTask;
            yield break;
        }
    }

    private sealed class BlockingOrderSnapshotProvider
        : IWebSocketTopicSnapshotProvider<OrderChanged>
    {
        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Completed { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public WebSocketTopicSnapshotContext? Context { get; private set; }

        public bool WasCanceled { get; private set; }

        public async IAsyncEnumerable<OrderChanged> GetSnapshotAsync(
            WebSocketTopicSnapshotContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            Context = context;
            Started.TrySetResult();
            try
            {
                await Release.Task.WaitAsync(cancellationToken);
            }
            catch (OperationCanceledException)
            {
                WasCanceled = true;
                throw;
            }
            finally
            {
                Completed.TrySetResult();
            }

            yield return new OrderChanged(context.Key!, 1);
        }
    }

    private sealed class CatalogSnapshotProvider : IWebSocketTopicSnapshotProvider<CatalogChanged>
    {
        public async IAsyncEnumerable<CatalogChanged> GetSnapshotAsync(
            WebSocketTopicSnapshotContext context,
            [EnumeratorCancellation] CancellationToken cancellationToken = default
        )
        {
            await Task.CompletedTask;
            yield break;
        }
    }
}
