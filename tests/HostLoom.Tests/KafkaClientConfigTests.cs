using System.Text;
using Confluent.Kafka;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// How <see cref="KafkaOptions"/> reaches the client library: the typed security options and
/// <see cref="KafkaOptions.ConfigureClient"/> land on every producer and consumer configuration,
/// the password never leaves the configuration, and the reply consumer starts at the end of the
/// response topic only once it has been assigned partitions.
/// </summary>
public sealed class KafkaClientConfigTests
{
    [Fact]
    public void Typed_security_options_are_applied_to_producer_and_consumer_configurations()
    {
        var options = new KafkaOptions
        {
            BootstrapServers = "broker-1:9093",
            SecurityProtocol = SecurityProtocol.SaslSsl,
            SaslMechanism = SaslMechanism.ScramSha512,
            SaslUsername = "catalog-worker",
            SaslPassword = "s3cret",
            SslCaLocation = "/etc/kafka/ca.pem",
        };

        var producer = KafkaRequestBroker.CreateProducerConfig(options);
        var consumer = KafkaRequestBroker.CreateConsumerConfig(
            options,
            clientId: "c",
            groupId: "g",
            enableAutoCommit: false,
            autoOffsetReset: AutoOffsetReset.Earliest
        );

        foreach (ClientConfig config in new ClientConfig[] { producer, consumer })
        {
            Assert.Equal("broker-1:9093", config.BootstrapServers);
            Assert.Equal(SecurityProtocol.SaslSsl, config.SecurityProtocol);
            Assert.Equal(SaslMechanism.ScramSha512, config.SaslMechanism);
            Assert.Equal("catalog-worker", config.SaslUsername);
            Assert.Equal("s3cret", config.SaslPassword);
            Assert.Equal("/etc/kafka/ca.pem", config.SslCaLocation);
        }

        Assert.Equal(Acks.All, producer.Acks);
        Assert.True(producer.EnableIdempotence);
        Assert.Equal("g", consumer.GroupId);
        Assert.False(consumer.EnableAutoCommit);
    }

    [Fact]
    public void Configure_client_runs_last_and_can_override_or_extend_the_typed_options()
    {
        var seen = new List<string>();
        var options = new KafkaOptions
        {
            SecurityProtocol = SecurityProtocol.Ssl,
            SslCaLocation = "/etc/kafka/ca.pem",
            ConfigureClient = config =>
            {
                seen.Add(config.GetType().Name);
                Assert.Equal(SecurityProtocol.Ssl, config.SecurityProtocol);
                config.SslCertificateLocation = "/etc/kafka/client.pem";
                config.SslKeyLocation = "/etc/kafka/client.key";
                config.SecurityProtocol = SecurityProtocol.SaslSsl;
            },
        };

        var producer = KafkaRequestBroker.CreateProducerConfig(options);
        var consumer = KafkaRequestBroker.CreateConsumerConfig(
            options,
            "c",
            "g",
            enableAutoCommit: true,
            AutoOffsetReset.Latest
        );

        Assert.Equal([nameof(ProducerConfig), nameof(ConsumerConfig)], seen);
        Assert.Equal("/etc/kafka/client.pem", producer.SslCertificateLocation);
        Assert.Equal("/etc/kafka/client.key", consumer.SslKeyLocation);
        Assert.Equal(SecurityProtocol.SaslSsl, producer.SecurityProtocol);
        Assert.Equal(SecurityProtocol.SaslSsl, consumer.SecurityProtocol);
    }

    [Fact]
    public void Without_security_options_the_configurations_carry_none()
    {
        var producer = KafkaRequestBroker.CreateProducerConfig(new KafkaOptions());

        Assert.Null(producer.SecurityProtocol);
        Assert.Null(producer.SaslMechanism);
        Assert.Null(producer.SaslUsername);
        Assert.Null(producer.SaslPassword);
        Assert.Null(producer.SslCaLocation);
    }

    [Theory]
    [InlineData(null)]
    [InlineData(SecurityProtocol.Plaintext)]
    [InlineData(SecurityProtocol.Ssl)]
    public void Credentials_without_a_sasl_protocol_are_refused_without_echoing_the_password(
        SecurityProtocol? protocol
    )
    {
        var options = new KafkaOptions
        {
            SecurityProtocol = protocol,
            SaslUsername = "catalog-worker",
            SaslPassword = "hunter2-do-not-print",
        };

        var exception = Assert.Throws<ArgumentException>(() =>
            new KafkaRequestBroker(
                Options.Create(options),
                logger: null,
                Substitute.For<IProducer<string, byte[]>>(),
                consumerFactory: (_, _) => Substitute.For<IConsumer<string, byte[]>>()
            )
        );

        Assert.Contains("SaslPlaintext or SaslSsl", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain("hunter2", exception.ToString(), StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_reply_consumer_starts_at_the_end_and_a_request_waits_for_its_assignment()
    {
        var kafka = new FakeReplyKafka();
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions { ResponseTopic = "billing.replies" }),
            logger: null,
            kafka.Producer,
            kafka.CreateConsumer
        );
        var requestId = Guid.NewGuid();

        var pending = broker
            .RequestAsync(
                "orders",
                "ask"u8.ToArray(),
                requestId,
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            )
            .AsTask();

        // The consumer exists and starts at the end, but nothing has been produced yet: producing
        // before the assignment could land the reply before the consumer's start position.
        var config = await kafka.ConsumerCreated.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken
        );
        Assert.Equal(AutoOffsetReset.Latest, config.AutoOffsetReset);
        Assert.Equal(["billing.replies"], kafka.Subscribed);
        await Task.Delay(50, TestContext.Current.CancellationToken);
        Assert.Empty(kafka.Produced);
        Assert.False(pending.IsCompleted);

        // The broker assigns partitions: each is pinned to its high watermark, and only then does
        // the request leave.
        var offsets = kafka.Assign(new TopicPartition("billing.replies", 0));
        Assert.Equal([new TopicPartitionOffset("billing.replies", 0, 42)], offsets);
        await kafka.WaitForProducedAsync(1);
        var request = Assert.Single(kafka.Produced);
        Assert.Equal("orders", request.Topic);

        kafka.DeliverReply(requestId, "answer");
        var response = await pending;
        Assert.Equal("answer", Encoding.UTF8.GetString(response.ToArray()));
    }

    [Fact]
    public async Task A_request_whose_reply_consumer_is_never_assigned_times_out_without_producing()
    {
        var kafka = new FakeReplyKafka();
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions()),
            logger: null,
            kafka.Producer,
            kafka.CreateConsumer
        );

        var exception = await Assert.ThrowsAsync<RequestTimeoutException>(async () =>
            await broker.RequestAsync(
                "orders",
                "ask"u8.ToArray(),
                Guid.NewGuid(),
                TimeSpan.FromMilliseconds(100),
                TestContext.Current.CancellationToken
            )
        );

        Assert.Contains(
            "not assigned",
            exception.InnerException?.Message,
            StringComparison.Ordinal
        );
        Assert.Empty(kafka.Produced);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Reassignment_resumes_pending_replies_from_local_progress(bool consumedReply)
    {
        var kafka = new FakeReplyKafka();
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions { ResponseTopic = "billing.replies" }),
            null,
            kafka.Producer,
            kafka.CreateConsumer
        );
        var id = Guid.NewGuid();
        var pending = Request(id);
        await kafka.ConsumerCreated.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken
        );
        var partition = new TopicPartition("billing.replies", 0);
        Assert.Equal(new Offset(42), Assert.Single(kafka.Assign(partition)).Offset);
        await kafka.WaitForProducedAsync(1);
        if (consumedReply)
        {
            kafka.DeliverReply(id, "first");
            await pending;
            id = Guid.NewGuid();
            pending = Request(id);
            await kafka.WaitForProducedAsync(2);
        }

        // A response arrived during revocation. The new end must never replace saved progress.
        kafka.HighWatermark = 44;
        var expected = consumedReply ? 43 : 42;
        Assert.Equal(new Offset(expected), Assert.Single(kafka.Assign(partition)).Offset);
        // Newly added partitions may also already contain a pending response.
        Assert.Equal(
            Offset.Beginning,
            Assert.Single(kafka.Assign(new TopicPartition("billing.replies", 1))).Offset
        );
        kafka.DeliverReply(id, "answer", expected);
        Assert.Equal("answer", Encoding.UTF8.GetString((await pending).Span));

        Task<ReadOnlyMemory<byte>> Request(Guid requestId) =>
            broker
                .RequestAsync(
                    "orders",
                    "ask"u8.ToArray(),
                    requestId,
                    TimeSpan.FromSeconds(5),
                    TestContext.Current.CancellationToken
                )
                .AsTask();
    }

    [Fact]
    public async Task A_failed_initial_watermark_fails_readiness_without_publishing()
    {
        var kafka = new FakeReplyKafka { FailedPartition = 1 };
        await using var broker = new KafkaRequestBroker(
            Options.Create(new KafkaOptions()),
            null,
            kafka.Producer,
            kafka.CreateConsumer
        );
        var pending = broker
            .RequestAsync(
                "orders",
                "ask"u8.ToArray(),
                Guid.NewGuid(),
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            )
            .AsTask();
        await kafka.ConsumerCreated.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken
        );

        Assert.Throws<KafkaException>(() =>
            kafka.Assign(
                new TopicPartition("billing.replies", 0),
                new TopicPartition("billing.replies", 1)
            )
        );
        await Assert.ThrowsAsync<KafkaException>(() => pending);
        Assert.Empty(kafka.Produced);
    }

    private sealed record ProducedRecord(string Topic, string? Key, byte[] Value);

    /// <summary>A fake reply topology: records what is produced and lets the test raise the assignment and deliver replies.</summary>
    private sealed class FakeReplyKafka
    {
        private readonly Lock _gate = new();
        private readonly Queue<ConsumeResult<string, byte[]>> _replies = new();
        private readonly SemaphoreSlim _producedSignal = new(0);
        private IConsumer<string, byte[]>? _consumer;
        private PartitionsAssignedHandler? _assigned;

        public FakeReplyKafka()
        {
            Producer = Substitute.For<IProducer<string, byte[]>>();
            Producer
                .ProduceAsync(
                    Arg.Any<string>(),
                    Arg.Any<Message<string, byte[]>>(),
                    Arg.Any<CancellationToken>()
                )
                .Returns(call =>
                {
                    var topic = call.ArgAt<string>(0);
                    var message = call.ArgAt<Message<string, byte[]>>(1);
                    lock (_gate)
                    {
                        Produced.Add(new ProducedRecord(topic, message.Key, message.Value));
                    }

                    _producedSignal.Release();
                    return Task.FromResult(new DeliveryResult<string, byte[]> { Topic = topic });
                });
        }

        public long HighWatermark { get; set; } = 42;

        public int? FailedPartition { get; set; }

        public IProducer<string, byte[]> Producer { get; }

        public TaskCompletionSource<ConsumerConfig> ConsumerCreated { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public List<string> Subscribed { get; } = [];

        public List<ProducedRecord> Produced { get; } = [];

        public IConsumer<string, byte[]> CreateConsumer(
            ConsumerConfig config,
            PartitionsAssignedHandler? partitionsAssigned
        )
        {
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            consumer
                .When(c => c.Subscribe(Arg.Any<string>()))
                .Do(call =>
                {
                    lock (_gate)
                    {
                        Subscribed.Add(call.ArgAt<string>(0));
                    }
                });
            consumer
                .QueryWatermarkOffsets(Arg.Any<TopicPartition>(), Arg.Any<TimeSpan>())
                .Returns(call =>
                    call.Arg<TopicPartition>().Partition.Value == FailedPartition
                        ? throw new KafkaException(ErrorCode.Local_TimedOut)
                        : new WatermarkOffsets(new Offset(0), new Offset(HighWatermark))
                );
            consumer
                .Consume(Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var token = call.Arg<CancellationToken>();
                    while (true)
                    {
                        token.ThrowIfCancellationRequested();
                        lock (_gate)
                        {
                            if (_replies.TryDequeue(out var reply))
                            {
                                return reply;
                            }
                        }

                        Thread.Sleep(1);
                    }
                });

            _consumer = consumer;
            _assigned = partitionsAssigned;
            ConsumerCreated.TrySetResult(config);
            return consumer;
        }

        /// <summary>Raises the assignment the client library would, returning the offsets the broker chose.</summary>
        public List<TopicPartitionOffset> Assign(params TopicPartition[] partitions) =>
            [.. _assigned!(_consumer!, [.. partitions])];

        public void DeliverReply(Guid requestId, string body, long offset = 42)
        {
            lock (_gate)
            {
                _replies.Enqueue(
                    new ConsumeResult<string, byte[]>
                    {
                        Topic = "billing.replies",
                        Partition = new Partition(0),
                        Offset = new Offset(offset),
                        Message = new Message<string, byte[]>
                        {
                            Value = Encoding.UTF8.GetBytes(body),
                            Headers = new Headers
                            {
                                {
                                    "hostloom-correlation-id",
                                    Encoding.UTF8.GetBytes(requestId.ToString("N"))
                                },
                            },
                        },
                    }
                );
            }
        }

        public async Task WaitForProducedAsync(int count)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            while (true)
            {
                lock (_gate)
                {
                    if (Produced.Count >= count)
                    {
                        return;
                    }
                }

                await _producedSignal.WaitAsync(timeout.Token);
            }
        }
    }
}
