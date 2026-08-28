using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Drives the RabbitMQ transport against a real broker from <c>docker-compose.yml</c>. The unit
/// suite covers correlation against fake channels; only these tests prove the adapter against
/// actual AMQP delivery, acknowledgement, and exchange topology.
/// </summary>
[Collection(nameof(RabbitMqTransportTests))]
[CollectionDefinition(nameof(RabbitMqTransportTests), DisableParallelization = true)]
public sealed class RabbitMqTransportTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    public static bool Available => BrokerAvailability.RabbitMq;

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
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

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task Concurrent_requests_each_receive_their_own_reply()
    {
        var address = Unique("concurrent");
        using var host = await StartAsync(hostLoom =>
            hostLoom.AddHandler<Greet, Greeting, GreetHandler>(address)
        );
        var client = ClientOf<Greet, Greeting>(host);

        // One exclusive reply queue carries every reply, so correlation is the only thing keeping
        // these apart. A correlation bug shows up here and nowhere in a serial round trip.
        var responses = await Task.WhenAll(
            Enumerable
                .Range(0, 25)
                .Select(i =>
                    client
                        .GetResponseAsync(address, new Greet($"n{i}"), cancellationToken: Token)
                        .AsTask()
                )
        );

        Assert.Equal(
            [.. Enumerable.Range(0, 25).Select(i => $"Hello, n{i}!").Order(StringComparer.Ordinal)],
            [.. responses.Select(r => r.Text).Order(StringComparer.Ordinal)]
        );
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
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

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task A_request_nobody_listens_to_times_out_instead_of_hanging()
    {
        using var host = await StartAsync(hostLoom => hostLoom.AddRequestClient<Greet, Greeting>());

        await Assert.ThrowsAsync<RequestTimeoutException>(async () =>
            await ClientOf<Greet, Greeting>(host)
                .GetResponseAsync(
                    Unique("nobody-home"),
                    new Greet("Ada"),
                    timeout: TimeSpan.FromSeconds(2),
                    cancellationToken: Token
                )
        );
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task Every_subscription_on_a_topic_receives_the_event()
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

        await PublisherOf(host).PublishAsync(topic, new OrderPlaced("A-1"), Token);

        // A fanout exchange with one durable queue per subscription: both must see the event.
        Assert.Equal(["audit:A-1", "shipping:A-1"], await received.WaitAsync(Bound));
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
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

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task An_event_nobody_subscribes_to_is_dropped_rather_than_failing_the_publish()
    {
        using var host = await StartAsync(hostLoom => hostLoom.AddRequestClient<Greet, Greeting>());

        // Published without `mandatory`, so an unroutable event must not fault the publisher.
        await PublisherOf(host).PublishAsync(Unique("unheard"), new OrderPlaced("A-3"), Token);
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
        Received? received = null
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received ?? new Received());
        configure(
            builder
                .Services.AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(20))
                .UseRabbitMq(options => options.Uri = new Uri("amqp://guest:guest@localhost:5672/"))
        );

        var host = builder.Build();
        await host.StartAsync(Token);
        return host;
    }
}
