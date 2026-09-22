using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Drives the RabbitMQ transport against a real broker from <c>docker-compose.yml</c>. The unit
/// suite covers correlation against fake channels; only these tests prove the adapter against
/// actual AMQP delivery, acknowledgement, and exchange topology. Every name a test mints is
/// recorded in a topology scope, so the durable queues and exchanges it declares are removed
/// from the shared broker after the host that used them is gone.
/// </summary>
[Collection(nameof(RabbitMqTransportTests))]
[CollectionDefinition(nameof(RabbitMqTransportTests), DisableParallelization = true)]
public sealed class RabbitMqTransportTests : IAsyncLifetime
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly RabbitMqTopologyScope _scope = new();

    public static bool Available => BrokerAvailability.RabbitMq;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => _scope.DisposeAsync();

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
    public async Task A_listener_handles_a_full_prefetch_window_of_requests_at_once()
    {
        var address = Unique("held");
        var gate = new Gate(16);
        using var host = await StartAsync(
            hostLoom => hostLoom.AddHandler<Hold, Held, HoldingHandler>(address),
            services => services.AddSingleton(gate)
        );
        var client = ClientOf<Hold, Held>(host);

        // The default dispatch concurrency equals the prefetch window, so every one of these
        // must be inside the handler before any is released; a serial dispatch would park the
        // first and never let the sixteenth arrive.
        var pending = Enumerable
            .Range(0, 16)
            .Select(i =>
                client.GetResponseAsync(address, new Hold(i), cancellationToken: Token).AsTask()
            )
            .ToArray();
        await gate.AllArrived.WaitAsync(Bound, Token);
        Assert.Equal(16, gate.Peak);
        Assert.All(pending, task => Assert.False(task.IsCompleted));

        gate.Release();
        var responses = await Task.WhenAll(pending);

        Assert.Equal(
            [.. Enumerable.Range(0, 16)],
            [.. responses.Select(response => response.Index).Order()]
        );
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task A_subscription_receives_events_in_publish_order_by_default()
    {
        var topic = Unique("sequence", "sequence");
        var received = new Received();
        received.Expect(16);
        using var host = await StartAsync(
            hostLoom => hostLoom.AddSubscriber<OrderPlaced, SequenceHandler>(topic, "sequence"),
            received: received
        );

        // Each publish awaits its confirmation, so the queue order is the loop order; with the
        // default event dispatch concurrency of 1 the handler must see exactly that order.
        for (var i = 0; i < 16; i++)
        {
            await PublisherOf(host).PublishAsync(topic, new OrderPlaced($"S-{i:D2}"), Token);
        }

        Assert.Equal(
            [.. Enumerable.Range(0, 16).Select(i => $"sequence:S-{i:D2}")],
            await received.WaitInOrderAsync(Bound)
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
        var topic = Unique("orders", "audit", "shipping");
        var received = new Received();
        received.Expect(2);
        using var host = await StartAsync(
            hostLoom =>
                hostLoom
                    .AddSubscriber<OrderPlaced, AuditHandler>(topic, "audit")
                    .AddSubscriber<OrderPlaced, ShippingHandler>(topic, "shipping"),
            received: received
        );

        await PublisherOf(host).PublishAsync(topic, new OrderPlaced("A-1"), Token);

        // A fanout exchange with one durable queue per subscription: both must see the event.
        Assert.Equal(["audit:A-1", "shipping:A-1"], await received.WaitAsync(Bound));
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task Handlers_sharing_one_subscription_share_a_single_delivery()
    {
        var topic = Unique("orders-shared", "combined");
        var received = new Received();
        received.Expect(2);
        using var host = await StartAsync(
            hostLoom =>
                hostLoom
                    .AddSubscriber<OrderPlaced, AuditHandler>(topic, "combined")
                    .AddSubscriber<OrderPlaced, ShippingHandler>(topic, "combined"),
            received: received
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

    /// <summary>
    /// Mints a name no other run can collide with and records what it may become on the broker:
    /// a request queue, a topic exchange, and one subscription queue per name given. Nothing here
    /// knows which of those a test will declare, and deleting a name that never existed is free.
    /// </summary>
    private string Unique(string prefix, params string[] subscriptions)
    {
        var name = $"it-{prefix}-{Guid.NewGuid():N}";
        _scope.Request(name);
        _scope.Topic(name);
        foreach (var subscription in subscriptions)
        {
            _scope.Subscription(name, subscription);
        }

        return name;
    }

    private static IRequestClient<TRequest, TResponse> ClientOf<TRequest, TResponse>(IHost host)
        where TRequest : class, IRequest<TResponse>
        where TResponse : class =>
        host.Services.GetRequiredService<IRequestClient<TRequest, TResponse>>();

    private static IPublishEndpoint PublisherOf(IHost host) =>
        host.Services.GetRequiredService<IPublishEndpoint>();

    private static async Task<IHost> StartAsync(
        Action<HostLoomBuilder> configure,
        Action<IServiceCollection>? services = null,
        Received? received = null
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received ?? new Received());
        services?.Invoke(builder.Services);
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
