using System.Text;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using HostLoom.IntegrationTests.Infrastructure;
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
public sealed class KafkaTransportTests : IAsyncLifetime
{
    private readonly List<string> _topics = [];

    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);

    // The documented instrument and tag names, spelled out so a rename fails here.
    private const string ClientTag = "hostloom.kafka.client";
    private const string DestinationTag = "messaging.destination.name";
    private const string KindTag = "hostloom.kafka.kind";
    private const string ReasonTag = "hostloom.kafka.reason";
    private const string OutcomeTag = "hostloom.kafka.outcome";
    private const string Produced = "hostloom.kafka.produced";
    private const string Consumed = "hostloom.kafka.consumed";
    private const string Committed = "hostloom.kafka.committed";
    private const string Skipped = "hostloom.kafka.records.skipped";
    private const string Rewound = "hostloom.kafka.records.rewound";
    private const string Initializations = "hostloom.kafka.reply_consumer.initializations";
    private const string PendingRequests = "hostloom.kafka.requests.pending";

    public static bool Available => BrokerAvailability.Kafka;

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task Request_and_response_round_trip_over_a_real_broker()
    {
        var address = await CreateTopicAsync("greeter");
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
        var address = await CreateTopicAsync("concurrent");
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
        var address = await CreateTopicAsync("failures");
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
                    await CreateTopicAsync("nobody-home"),
                    new Greet("Ada"),
                    timeout: TimeSpan.FromSeconds(5),
                    cancellationToken: Token
                )
        );
    }

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task Each_subscription_is_its_own_consumer_group_and_sees_every_event()
    {
        var topic = await CreateTopicAsync("orders");
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
        var topic = await CreateTopicAsync("orders-shared");
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
        var topic = await CreateTopicAsync("offsets");
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

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task A_round_trip_meters_both_produces_the_listener_and_one_reply_consumer_start()
    {
        // The request topic is the address itself, so the listener's loop measurements carry it.
        var address = await CreateTopicAsync("metered");
        var clientId = Unique("client");
        using var client = new MeterCapture(KafkaDiagnostics.MeterName, ClientTag, clientId);
        using var listener = new MeterCapture(KafkaDiagnostics.MeterName, DestinationTag, address);
        var gate = new Gate(1);
        using var host = await StartAsync(
            hostLoom =>
            {
                hostLoom.Services.AddSingleton(gate);
                hostLoom.AddHandler<Hold, Held, HoldingHandler>(address);
            },
            clientId: clientId
        );

        var response = ClientOf<Hold, Held>(host)
            .GetResponseAsync(address, new Hold(7), cancellationToken: Token)
            .AsTask();
        await gate.AllArrived.WaitAsync(Bound, Token);
        // The handler is holding the request, so its caller is still awaiting the reply.
        Assert.Equal(1, client.Observe(PendingRequests));
        gate.Release();
        Assert.Equal(7, (await response.WaitAsync(Bound, Token)).Index);

        // The listener commits after the reply has been produced, so the commit, and with it the
        // reply count, may trail the response the caller already holds.
        using var deadline = Deadline();
        await listener.WaitForAsync(Committed, 1, deadline.Token);
        Assert.Equal(1, client.Sum(Produced, (KindTag, "request")));
        Assert.Equal(1, client.Sum(Produced, (KindTag, "reply")));
        Assert.Equal(0, client.Sum(Produced, (KindTag, "event")));
        Assert.Equal(1, client.Sum(Initializations, (OutcomeTag, "succeeded")));
        Assert.Equal(0, client.Sum(Initializations, (OutcomeTag, "failed")));
        Assert.Equal(0, client.Observe(PendingRequests));
        Assert.Equal(1, listener.Sum(Consumed));
        Assert.Equal(1, listener.Sum(Committed));
        Assert.Equal(0, listener.Sum(Skipped));
        Assert.Equal(0, listener.Sum(Rewound));
    }

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task A_failed_event_is_rewound_and_redelivered_before_its_offset_is_committed()
    {
        var topic = await CreateTopicAsync("invoices");
        using var loop = new MeterCapture(KafkaDiagnostics.MeterName, DestinationTag, topic);
        var deliveries = new FailFirstDelivery();
        (double Rewound, double Committed)? atRedelivery = null;
        // Taken inside the handler, on the consumer loop, before the redelivery can be committed.
        deliveries.OnAttempt = attempt =>
        {
            if (attempt == 2)
            {
                atRedelivery = (loop.Sum(Rewound), loop.Sum(Committed));
            }
        };
        using var host = await StartAsync(hostLoom =>
        {
            hostLoom.Services.AddSingleton(deliveries);
            hostLoom.AddSubscriber<OrderPlaced, FailFirstDeliveryHandler>(topic, "billing");
        });

        await PublisherOf(host).PublishAsync(topic, new OrderPlaced("I-1"), Token);

        // Events retry until shutdown, so only the deadline bounds a redelivery that never comes.
        using var deadline = Deadline();
        await deliveries.Redelivered.WaitAsync(deadline.Token);
        await loop.WaitForAsync(Committed, 1, deadline.Token);
        Assert.Equal(["I-1", "I-1"], deliveries.Seen);
        Assert.NotNull(atRedelivery);
        Assert.True(
            atRedelivery.Value.Rewound >= 1,
            "The failure was not rewound before redelivery."
        );
        Assert.Equal(0, atRedelivery.Value.Committed);
        Assert.Equal(2, loop.Sum(Consumed));
        Assert.Equal(1, loop.Sum(Committed));
        Assert.Equal(0, loop.Sum(Skipped));
    }

    [Fact(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    public async Task A_record_that_is_not_an_envelope_is_skipped_and_the_listener_keeps_serving()
    {
        var address = await CreateTopicAsync("malformed");
        using var listener = new MeterCapture(KafkaDiagnostics.MeterName, DestinationTag, address);
        using var host = await StartAsync(hostLoom =>
            hostLoom.AddHandler<Greet, Greeting, GreetHandler>(address)
        );

        using (
            var producer = new ProducerBuilder<string, byte[]>(
                new ProducerConfig
                {
                    BootstrapServers = "localhost:9092",
                    MessageTimeoutMs = 15_000,
                }
            ).Build()
        )
        {
            // No headers at all: the transport rejects it before the host sees it.
            await producer
                .ProduceAsync(
                    address,
                    new Message<string, byte[]> { Value = "not an envelope"u8.ToArray() },
                    Token
                )
                .WaitAsync(Bound, Token);
            // The transport's headers around a body the host cannot decode as an envelope.
            await producer
                .ProduceAsync(
                    address,
                    new Message<string, byte[]>
                    {
                        Value = [0xFF, 0x00, 0x7B],
                        Headers = new Headers
                        {
                            {
                                "hostloom-correlation-id",
                                Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"))
                            },
                            { "hostloom-reply-to", Encoding.UTF8.GetBytes(Unique("replies")) },
                        },
                    },
                    Token
                )
                .WaitAsync(Bound, Token);
        }

        using var deadline = Deadline();
        await listener.WaitForAsync(Skipped, 2, deadline.Token, (ReasonTag, "malformed"));
        await listener.WaitForAsync(Committed, 2, deadline.Token);

        var response = await ClientOf<Greet, Greeting>(host)
            .GetResponseAsync(address, new Greet("Ada"), cancellationToken: Token);

        Assert.Equal("Hello, Ada!", response.Text);
        await listener.WaitForAsync(Committed, 3, deadline.Token);
        Assert.Equal(2, listener.Sum(Skipped, (ReasonTag, "malformed")));
        Assert.Equal(2, listener.Sum(Skipped));
        Assert.Equal(3, listener.Sum(Consumed));
        Assert.Equal(0, listener.Sum(Rewound));
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        if (_topics.Count == 0)
        {
            return;
        }

        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = "localhost:9092" }
        ).Build();
        await admin
            .DeleteTopicsAsync(
                _topics,
                new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
            )
            .WaitAsync(TimeSpan.FromSeconds(15));
    }

    private async Task<string> CreateTopicAsync(string prefix)
    {
        var topic = Unique(prefix);
        using var admin = new AdminClientBuilder(
            new AdminClientConfig { BootstrapServers = "localhost:9092" }
        ).Build();
        await admin
            .CreateTopicsAsync(
                [
                    new TopicSpecification
                    {
                        Name = topic,
                        NumPartitions = 1,
                        ReplicationFactor = 1,
                    },
                ],
                new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
            )
            .WaitAsync(TimeSpan.FromSeconds(15), Token);
        _topics.Add(topic);
        return topic;
    }

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static CancellationTokenSource Deadline()
    {
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(Token);
        deadline.CancelAfter(Bound);
        return deadline;
    }

    private static string Unique(string prefix) => $"it-{prefix}-{Guid.NewGuid():N}";

    private static IRequestClient<TRequest, TResponse> ClientOf<TRequest, TResponse>(IHost host)
        where TRequest : class, IRequest<TResponse>
        where TResponse : class =>
        host.Services.GetRequiredService<IRequestClient<TRequest, TResponse>>();

    private static IPublishEndpoint PublisherOf(IHost host) =>
        host.Services.GetRequiredService<IPublishEndpoint>();

    private async Task<IHost> StartAsync(
        Action<HostLoomBuilder> configure,
        Received? received = null,
        string? consumerGroup = null,
        string? clientId = null
    )
    {
        var responseTopic = await CreateTopicAsync("responses");
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received ?? new Received());
        configure(
            builder
                .Services.AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(45))
                .UseKafka(options =>
                {
                    options.BootstrapServers = "localhost:9092";
                    options.ConsumerGroup = consumerGroup ?? Unique("group");
                    options.ResponseTopic = responseTopic;
                    if (clientId is not null)
                    {
                        options.ClientId = clientId;
                    }
                })
        );

        var host = builder.Build();
        await host.StartAsync(Token);
        return host;
    }
}
