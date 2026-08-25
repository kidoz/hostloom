using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Verifies the Kafka publish/subscribe topology against a fake producer and consumer factory.
/// A subscription maps onto a consumer group, which is what makes the fan-out work.
/// </summary>
public sealed class KafkaPublishSubscribeTests
{
    [Fact]
    public async Task Subscribing_joins_a_consumer_group_named_for_the_subscription()
    {
        var kafka = new FakeKafka();
        await using var broker = Create(kafka);

        await using var subscription = await broker.SubscribeAsync(
            "orders",
            "audit",
            (_, _) => ValueTask.CompletedTask,
            TestContext.Current.CancellationToken
        );

        var config = Assert.Single(kafka.ConsumerConfigs);
        Assert.Equal("hostloom.orders.audit", config.GroupId);
        Assert.False(config.EnableAutoCommit);
        Assert.Equal(["orders"], kafka.SubscribedTopics);
    }

    [Fact]
    public async Task Two_subscriptions_on_one_topic_use_distinct_consumer_groups()
    {
        var kafka = new FakeKafka();
        await using var broker = Create(kafka);

        await using var audit = await broker.SubscribeAsync(
            "orders",
            "audit",
            (_, _) => ValueTask.CompletedTask,
            TestContext.Current.CancellationToken
        );
        await using var shipping = await broker.SubscribeAsync(
            "orders",
            "shipping",
            (_, _) => ValueTask.CompletedTask,
            TestContext.Current.CancellationToken
        );

        // Distinct groups are the fan-out. One shared group would make them competing consumers,
        // so each event would reach only one of the two.
        Assert.Equal(
            ["hostloom.orders.audit", "hostloom.orders.shipping"],
            kafka.ConsumerConfigs.Select(config => config.GroupId).Order(StringComparer.Ordinal)
        );
    }

    [Fact]
    public async Task Publishing_produces_the_frame_to_the_topic()
    {
        var kafka = new FakeKafka();
        await using var broker = Create(kafka);

        await broker.PublishAsync(
            "orders",
            Encoding.UTF8.GetBytes("placed"),
            TestContext.Current.CancellationToken
        );

        var produced = Assert.Single(kafka.Produced);
        Assert.Equal("orders", produced.Topic);
        Assert.Equal("placed", Encoding.UTF8.GetString(produced.Value));
        // No key: the broker cannot infer a partition key from an opaque frame.
        Assert.Null(produced.Key);
    }

    [Fact]
    public async Task A_delivered_event_is_handled_then_committed()
    {
        var kafka = new FakeKafka();
        kafka.Enqueue("orders", offset: 0, body: "placed");
        await using var broker = Create(kafka);
        var handled = new List<string>();

        await using var subscription = await broker.SubscribeAsync(
            "orders",
            "audit",
            (frame, _) =>
            {
                handled.Add(Encoding.UTF8.GetString(frame.Span));
                return ValueTask.CompletedTask;
            },
            TestContext.Current.CancellationToken
        );

        await WaitUntilAsync(() => kafka.Commits.Count == 1, "the record commits");

        Assert.Equal(["placed"], handled);
        // Commit(result) commits offset + 1, so a handled record at offset 0 commits 1.
        Assert.Equal([1L], kafka.Commits);
    }

    private static KafkaRequestBroker Create(FakeKafka kafka) =>
        new(Options.Create(new KafkaOptions()), logger: null, kafka.Producer, kafka.CreateConsumer);

    private static async Task WaitUntilAsync(Func<bool> condition, string because)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(10))
        {
            if (condition())
            {
                return;
            }

            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        Assert.Fail($"Timed out waiting until {because}.");
    }

    private sealed class FakeKafka
    {
        private readonly Lock _gate = new();
        private readonly Queue<ConsumeResult<string, byte[]>> _pending = new();

        public FakeKafka()
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

                    return Task.FromResult(new DeliveryResult<string, byte[]> { Topic = topic });
                });
        }

        public IProducer<string, byte[]> Producer { get; }

        public List<ConsumerConfig> ConsumerConfigs { get; } = [];

        public List<string> SubscribedTopics { get; } = [];

        public List<ProducedRecord> Produced { get; } = [];

        public List<long> Commits { get; } = [];

        public void Enqueue(string topic, long offset, string body) =>
            _pending.Enqueue(
                new ConsumeResult<string, byte[]>
                {
                    Topic = topic,
                    Partition = new Partition(0),
                    Offset = new Offset(offset),
                    Message = new Message<string, byte[]>
                    {
                        Value = Encoding.UTF8.GetBytes(body),
                        Headers = new Headers(),
                    },
                }
            );

        public IConsumer<string, byte[]> CreateConsumer(ConsumerConfig config)
        {
            lock (_gate)
            {
                ConsumerConfigs.Add(config);
            }

            var consumer = Substitute.For<IConsumer<string, byte[]>>();
            consumer
                .When(c => c.Subscribe(Arg.Any<string>()))
                .Do(call =>
                {
                    lock (_gate)
                    {
                        SubscribedTopics.Add(call.ArgAt<string>(0));
                    }
                });

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
                            if (_pending.TryDequeue(out var record))
                            {
                                return record;
                            }
                        }

                        Thread.Sleep(1);
                    }
                });

            consumer
                .When(c => c.Commit(Arg.Any<ConsumeResult<string, byte[]>>()))
                .Do(call =>
                {
                    var record = call.ArgAt<ConsumeResult<string, byte[]>>(0);
                    lock (_gate)
                    {
                        Commits.Add(record.Offset.Value + 1);
                    }
                });

            return consumer;
        }
    }

    private sealed record ProducedRecord(string Topic, string? Key, byte[] Value);
}
