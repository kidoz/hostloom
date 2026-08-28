using HostLoom.Transport.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Drives the Kafka transport against a real broker from <c>docker-compose.yml</c>. Every test
/// uses fresh topic and consumer-group names, because a group that has already committed offsets
/// behaves differently from one joining for the first time and would make results order-dependent.
/// Bounds are generous: a fresh group must be assigned partitions before anything is delivered.
/// </summary>
[Collection(nameof(KafkaTransportTests))]
[CollectionDefinition(nameof(KafkaTransportTests), DisableParallelization = true)]
public sealed class KafkaTransportTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    public static bool Available => BrokerAvailability.Kafka;

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task Request_and_response_round_trip_over_a_real_broker()
    {
        var address = Unique("greeter");
        using var host = await StartAsync(hostLoom =>
            hostLoom.AddHandler<Greet, Greeting, GreetHandler>(address)
        );

        var response = await ClientOf<Greet, Greeting>(host)
            .GetResponseAsync(address, new Greet("Ada"), cancellationToken: Token);

        Assert.Equal("Hello, Ada!", response.Text);
    }

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task Concurrent_requests_correlate_through_the_shared_response_topic()
    {
        var address = Unique("concurrent");
        using var host = await StartAsync(hostLoom =>
            hostLoom.AddHandler<Greet, Greeting, GreetHandler>(address)
        );
        var client = ClientOf<Greet, Greeting>(host);

        // Every reply lands on one response topic and is filtered by correlation id, so this is
        // the case where a header-correlation bug surfaces.
        var responses = await Task.WhenAll(
            Enumerable
                .Range(0, 10)
                .Select(i =>
                    client
                        .GetResponseAsync(address, new Greet($"n{i}"), cancellationToken: Token)
                        .AsTask()
                )
        );

        Assert.Equal(
            [.. Enumerable.Range(0, 10).Select(i => $"Hello, n{i}!").Order(StringComparer.Ordinal)],
            [.. responses.Select(r => r.Text).Order(StringComparer.Ordinal)]
        );
    }

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task A_handler_fault_returns_as_a_remote_fault_without_a_stack_trace()
    {
        var address = Unique("failures");
        using var host = await StartAsync(hostLoom =>
            hostLoom.AddHandler<Fail, Never, FailingHandler>(address)
        );

        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await ClientOf<Fail, Never>(host)
                .GetResponseAsync(address, new Fail("broker fault"), cancellationToken: Token)
        );

        Assert.Contains("broker fault", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(nameof(FailingHandler), exception.Message, StringComparison.Ordinal);
    }

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task A_request_nobody_consumes_times_out_instead_of_hanging()
    {
        using var host = await StartAsync(hostLoom => hostLoom.AddRequestClient<Greet, Greeting>());

        await Assert.ThrowsAsync<RequestTimeoutException>(async () =>
            await ClientOf<Greet, Greeting>(host)
                .GetResponseAsync(
                    Unique("nobody-home"),
                    new Greet("Ada"),
                    timeout: TimeSpan.FromSeconds(5),
                    cancellationToken: Token
                )
        );
    }

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task Each_subscription_is_its_own_consumer_group_and_sees_every_event()
    {
        var topic = Unique("orders");
        var received = new Received();
        received.Expect(2);
        using var host = await StartAsync(
            hostLoom =>
                hostLoom
                    .AddSubscriber<OrderPlaced, AuditHandler>(topic, "audit")
                    .AddSubscriber<OrderPlaced, ShippingHandler>(topic, "shipping"),
            received
        );

        // Both groups start from Earliest, so the publish may precede assignment without loss.
        await PublisherOf(host).PublishAsync(topic, new OrderPlaced("A-1"), Token);

        Assert.Equal(["audit:A-1", "shipping:A-1"], await received.WaitAsync(Bound));
    }

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task Handlers_sharing_one_subscription_share_a_single_delivery()
    {
        var topic = Unique("orders-shared");
        var received = new Received();
        received.Expect(2);
        using var host = await StartAsync(
            hostLoom =>
                hostLoom
                    .AddSubscriber<OrderPlaced, AuditHandler>(topic, "combined")
                    .AddSubscriber<OrderPlaced, ShippingHandler>(topic, "combined"),
            received
        );

        await PublisherOf(host).PublishAsync(topic, new OrderPlaced("A-2"), Token);

        Assert.Equal(["audit:A-2", "shipping:A-2"], await received.WaitAsync(Bound));
    }

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task Committed_offsets_survive_a_restart_so_events_are_not_redelivered()
    {
        var topic = Unique("offsets");
        var group = Unique("group");
        var first = new Received();
        first.Expect(1);
        using (
            var host = await StartAsync(
                hostLoom => hostLoom.AddSubscriber<OrderPlaced, AuditHandler>(topic, "audit"),
                first,
                group
            )
        )
        {
            await PublisherOf(host).PublishAsync(topic, new OrderPlaced("A-4"), Token);
            Assert.Equal(["audit:A-4"], await first.WaitAsync(Bound));
            // Stopping the host must commit, or the same group replays the event on restart.
            await host.StopAsync(Token);
        }

        var second = new Received();
        second.Expect(1);
        using var restarted = await StartAsync(
            hostLoom => hostLoom.AddSubscriber<OrderPlaced, AuditHandler>(topic, "audit"),
            second,
            group
        );
        await PublisherOf(restarted).PublishAsync(topic, new OrderPlaced("A-5"), Token);

        // Only the new event: A-4 was committed by the previous run of the same group.
        Assert.Equal(["audit:A-5"], await second.WaitAsync(Bound));
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static string Unique(string prefix) => $"it-{prefix}-{Guid.NewGuid():N}";

    private static IRequestClient<TRequest, TResponse> ClientOf<TRequest, TResponse>(IHost host)
        where TRequest : class, IRequest<TResponse>
        where TResponse : class =>
        host.Services.GetRequiredService<IRequestClient<TRequest, TResponse>>();

    private static IPublishEndpoint PublisherOf(IHost host) =>
        host.Services.GetRequiredService<IPublishEndpoint>();

    private static async Task<IHost> StartAsync(
        Action<HostLoomBuilder> configure,
        Received? received = null,
        string? consumerGroup = null
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received ?? new Received());
        configure(
            builder
                .Services.AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(45))
                .UseKafka(options =>
                {
                    options.BootstrapServers = "localhost:9092";
                    options.ConsumerGroup = consumerGroup ?? Unique("group");
                    options.ResponseTopic = Unique("responses");
                })
        );

        var host = builder.Build();
        await host.StartAsync(Token);
        return host;
    }
}
