using System.Diagnostics;
using System.Text;
using System.Text.Json;
using Confluent.Kafka;
using Confluent.Kafka.Admin;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Transport.Kafka;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using RabbitMQ.Client;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// The two dispositions the in-memory and fake-consumer tests cannot show: an expired request
/// leaves the broker without running its handler, and a tripped kill switch holds the next
/// delivery for the restart interval before that handler runs.
/// </summary>
public sealed class BrokerDispositionTests : IAsyncLifetime
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(60);
    private static readonly TimeSpan Restart = TimeSpan.FromSeconds(1);
    private static readonly JsonSerializerOptions Json = new(JsonSerializerDefaults.Web);

    private readonly List<string> _topics = [];
    private readonly RabbitMqTopologyScope _rabbit = new();

    public static bool RabbitMqAvailable => BrokerAvailability.RabbitMq;

    public static bool KafkaAvailable => BrokerAvailability.Kafka;

    [Fact(
        Timeout = 60_000,
        Skip = BrokerAvailability.RabbitMqSkip,
        SkipUnless = nameof(RabbitMqAvailable)
    )]
    public async Task An_expired_RabbitMQ_request_is_rejected_and_a_later_request_is_handled()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var address = _rabbit.Request(Unique("expired-rabbit"));
        var counter = new DispositionCounter();
        using var expired = new MeterCapture("HostLoom", "messaging.destination.name", address);
        using var rejected = new MeterCapture(
            RabbitMqDiagnostics.MeterName,
            "hostloom.rabbitmq.client",
            address
        );
        using var host = await StartRabbitAsync(
            address,
            counter,
            static services =>
                services.AddHandler<Greet, Greeting, CountingGreetHandler>(services.Address)
        );

        await PublishRabbitAsync(address, ExpiredRequest(), cancellationToken);
        using var deadline = Deadline(cancellationToken);
        await expired.WaitForAsync("hostloom.messaging.requests.expired", 1, deadline.Token);
        await rejected.WaitForAsync(
            "hostloom.rabbitmq.deliveries.rejected",
            1,
            deadline.Token,
            ("hostloom.rabbitmq.reason", "expired")
        );
        Assert.Equal(0, counter.Attempts);

        var response = await ClientOf(host)
            .GetResponseAsync(address, new Greet("Ada"), cancellationToken: cancellationToken);
        Assert.Equal("Hello, Ada!", response.Text);
        Assert.Equal(1, counter.Attempts);
    }

    [Fact(
        Timeout = 90_000,
        Skip = BrokerAvailability.KafkaSkip,
        SkipUnless = nameof(KafkaAvailable)
    )]
    public async Task An_expired_Kafka_request_is_skipped_and_a_later_request_is_handled()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var address = await CreateTopicAsync("expired-kafka", cancellationToken);
        var replies = await CreateTopicAsync("expired-kafka-replies", cancellationToken);
        var counter = new DispositionCounter();
        using var expired = new MeterCapture("HostLoom", "messaging.destination.name", address);
        using var skipped = new MeterCapture(
            KafkaDiagnostics.MeterName,
            "messaging.destination.name",
            address
        );
        using var host = await StartKafkaAsync(
            address,
            replies,
            counter,
            static services =>
                services.AddHandler<Greet, Greeting, CountingGreetHandler>(services.Address)
        );

        await PublishKafkaAsync(address, replies, ExpiredRequest(), cancellationToken);
        using var deadline = Deadline(cancellationToken);
        await expired.WaitForAsync("hostloom.messaging.requests.expired", 1, deadline.Token);
        await skipped.WaitForAsync(
            "hostloom.kafka.records.skipped",
            1,
            deadline.Token,
            ("hostloom.kafka.reason", "expired")
        );
        Assert.Equal(0, counter.Attempts);

        var response = await ClientOf(host)
            .GetResponseAsync(address, new Greet("Ada"), cancellationToken: cancellationToken);
        Assert.Equal("Hello, Ada!", response.Text);
        Assert.Equal(1, counter.Attempts);
        Assert.Equal(0, skipped.Sum("hostloom.kafka.records.rewound"));
    }

    [Fact(
        Timeout = 60_000,
        Skip = BrokerAvailability.RabbitMqSkip,
        SkipUnless = nameof(RabbitMqAvailable)
    )]
    public async Task A_tripped_RabbitMQ_destination_pauses_before_the_next_handler_run()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var address = _rabbit.Request(Unique("pause-rabbit"));
        var counter = new DispositionCounter();
        using var suspensions = new MeterCapture("HostLoom", "messaging.destination.name", address);
        using var host = await StartRabbitAsync(
            address,
            counter,
            static services =>
                services.AddHandler<Greet, Greeting, PausingGreetHandler>(services.Address)
        );
        var client = ClientOf(host);

        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync(
                address,
                new Greet("1"),
                cancellationToken: cancellationToken
            )
        );
        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync(
                address,
                new Greet("2"),
                cancellationToken: cancellationToken
            )
        );
        Assert.Equal(2, counter.Attempts);
        using var deadline = Deadline(cancellationToken);
        await suspensions.WaitForAsync("hostloom.receive.suspensions", 1, deadline.Token);

        var started = Stopwatch.GetTimestamp();
        var response = await client.GetResponseAsync(
            address,
            new Greet("3"),
            timeout: TimeSpan.FromSeconds(20),
            cancellationToken: cancellationToken
        );
        Assert.True(Stopwatch.GetElapsedTime(started) >= Restart - TimeSpan.FromMilliseconds(250));
        Assert.Equal("resumed", response.Text);
        Assert.Equal(3, counter.Attempts);
    }

    [Fact(
        Timeout = 90_000,
        Skip = BrokerAvailability.KafkaSkip,
        SkipUnless = nameof(KafkaAvailable)
    )]
    public async Task A_tripped_Kafka_destination_pauses_before_the_next_handler_run()
    {
        var cancellationToken = TestContext.Current.CancellationToken;
        var address = await CreateTopicAsync("pause-kafka", cancellationToken);
        var replies = await CreateTopicAsync("pause-kafka-replies", cancellationToken);
        var counter = new DispositionCounter();
        using var suspensions = new MeterCapture("HostLoom", "messaging.destination.name", address);
        using var host = await StartKafkaAsync(
            address,
            replies,
            counter,
            static services =>
                services.AddHandler<Greet, Greeting, PausingGreetHandler>(services.Address)
        );
        var client = ClientOf(host);

        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync(
                address,
                new Greet("1"),
                cancellationToken: cancellationToken
            )
        );
        await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await client.GetResponseAsync(
                address,
                new Greet("2"),
                cancellationToken: cancellationToken
            )
        );
        Assert.Equal(2, counter.Attempts);
        using var deadline = Deadline(cancellationToken);
        await suspensions.WaitForAsync("hostloom.receive.suspensions", 1, deadline.Token);

        var started = Stopwatch.GetTimestamp();
        var response = await client.GetResponseAsync(
            address,
            new Greet("3"),
            timeout: TimeSpan.FromSeconds(20),
            cancellationToken: cancellationToken
        );
        Assert.True(Stopwatch.GetElapsedTime(started) >= Restart - TimeSpan.FromMilliseconds(250));
        Assert.Equal("resumed", response.Text);
        Assert.Equal(3, counter.Attempts);
    }

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public async ValueTask DisposeAsync()
    {
        await _rabbit.DisposeAsync();
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

    private static CancellationTokenSource Deadline(CancellationToken cancellationToken)
    {
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        deadline.CancelAfter(Bound);
        return deadline;
    }

    private static string Unique(string prefix) => $"it-{prefix}-{Guid.NewGuid():N}";

    private static IRequestClient<Greet, Greeting> ClientOf(IHost host) =>
        host.Services.GetRequiredService<IRequestClient<Greet, Greeting>>();

    private static async Task<IHost> StartRabbitAsync(
        string address,
        DispositionCounter counter,
        Action<HostSetup> configure
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(counter);
        var hostLoom = builder
            .Services.AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(20))
            .UseRabbitMq(options =>
            {
                options.Uri = new Uri("amqp://guest:guest@localhost:5672/");
                options.ClientProvidedName = address;
            });
        hostLoom.ConfigureReceivePipeline(pipe =>
            pipe.UseKillSwitch(
                new KillSwitchPolicy(
                    activationThreshold: 2,
                    tripThreshold: 1,
                    trackingPeriod: TimeSpan.FromMinutes(1),
                    restartTimeout: Restart
                )
            )
        );
        configure(new HostSetup(hostLoom, address));
        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private static async Task<IHost> StartKafkaAsync(
        string address,
        string replies,
        DispositionCounter counter,
        Action<HostSetup> configure
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(counter);
        var hostLoom = builder
            .Services.AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(30))
            .UseKafka(options =>
            {
                options.BootstrapServers = "localhost:9092";
                options.ConsumerGroup = Unique("group");
                options.ResponseTopic = replies;
                options.AllowedReplyTopics.Add(replies);
            });
        hostLoom.ConfigureReceivePipeline(pipe =>
            pipe.UseKillSwitch(
                new KillSwitchPolicy(
                    activationThreshold: 2,
                    tripThreshold: 1,
                    trackingPeriod: TimeSpan.FromMinutes(1),
                    restartTimeout: Restart
                )
            )
        );
        configure(new HostSetup(hostLoom, address));
        var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        return host;
    }

    private async Task<string> CreateTopicAsync(string prefix, CancellationToken cancellationToken)
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
            .WaitAsync(TimeSpan.FromSeconds(15), cancellationToken);
        _topics.Add(topic);
        return topic;
    }

    private static async Task PublishRabbitAsync(
        string address,
        byte[] frame,
        CancellationToken cancellationToken
    )
    {
        await using var connection = await new ConnectionFactory
        {
            Uri = new Uri("amqp://guest:guest@localhost:5672/"),
        }.CreateConnectionAsync(cancellationToken);
        await using var channel = await connection.CreateChannelAsync(
            cancellationToken: cancellationToken
        );
        await channel.BasicPublishAsync(
            string.Empty,
            RabbitMqQueueNames.Request(address),
            mandatory: true,
            new BasicProperties
            {
                ContentType = "application/vnd.hostloom.envelope+json",
                CorrelationId = Guid.NewGuid().ToString("N"),
                ReplyTo = "amq.gen-hostloom-expired",
            },
            frame,
            cancellationToken
        );
    }

    private static async Task PublishKafkaAsync(
        string address,
        string replies,
        byte[] frame,
        CancellationToken cancellationToken
    )
    {
        using var producer = new ProducerBuilder<string, byte[]>(
            new ProducerConfig { BootstrapServers = "localhost:9092", MessageTimeoutMs = 15_000 }
        ).Build();
        await producer
            .ProduceAsync(
                address,
                new Message<string, byte[]>
                {
                    Value = frame,
                    Headers = new Confluent.Kafka.Headers
                    {
                        {
                            "hostloom-correlation-id",
                            Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N"))
                        },
                        { "hostloom-reply-to", Encoding.UTF8.GetBytes(replies) },
                    },
                },
                cancellationToken
            )
            .WaitAsync(Bound, cancellationToken);
    }

    private static byte[] ExpiredRequest()
    {
        var body = Convert.ToBase64String(
            JsonSerializer.SerializeToUtf8Bytes(new Greet("stale"), Json)
        );
        var json = $$"""
            {
              "messageId": "{{Guid.NewGuid()}}",
              "kind": "Request",
              "messageType": "{{Name<Greet>()}}",
              "responseType": "{{Name<Greeting>()}}",
              "sentAt": "2020-01-01T00:00:00Z",
              "expiresAt": "2020-01-01T00:00:01Z",
              "body": "{{body}}"
            }
            """;
        return Encoding.UTF8.GetBytes(json);
    }

    private static string Name<T>() => $"{typeof(T).Assembly.GetName().Name}:{typeof(T).FullName}";

    private sealed class HostSetup(HostLoomBuilder builder, string address)
    {
        public string Address { get; } = address;

        public void AddHandler<TRequest, TResponse, THandler>(string endpoint)
            where TRequest : class, IRequest<TResponse>
            where THandler : class, IRequestHandler<TRequest, TResponse> =>
            builder.AddHandler<TRequest, TResponse, THandler>(endpoint);
    }
}

/// <summary>Counts handler entries for one host.</summary>
public sealed class DispositionCounter
{
    private int _attempts;

    public int Attempts => Volatile.Read(ref _attempts);

    public int Increment() => Interlocked.Increment(ref _attempts);
}

/// <summary>Records that an expired request reached a handler.</summary>
public sealed class CountingGreetHandler(DispositionCounter counter)
    : IRequestHandler<Greet, Greeting>
{
    public ValueTask<Greeting> HandleAsync(Greet request, CancellationToken cancellationToken)
    {
        counter.Increment();
        return ValueTask.FromResult(new Greeting($"Hello, {request.Name}!"));
    }
}

/// <summary>Fails its first two runs, then answers, so a kill switch can trip and resume.</summary>
public sealed class PausingGreetHandler(DispositionCounter counter)
    : IRequestHandler<Greet, Greeting>
{
    public ValueTask<Greeting> HandleAsync(Greet request, CancellationToken cancellationToken)
    {
        if (counter.Increment() <= 2)
        {
            throw new InvalidOperationException("The handler is not ready.");
        }

        return ValueTask.FromResult(new Greeting("resumed"));
    }
}
