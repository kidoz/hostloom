using System.Diagnostics;
using System.Text;
using Confluent.Kafka;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.Options;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The reply topic a request names is caller-controlled. It is checked against Kafka's topic
/// rules and the optional allow list before the handler runs, and a reply that cannot be produced
/// afterwards is logged and skipped rather than used as a reason to run the handler again.
/// </summary>
public sealed class KafkaReplyRoutingTests
{
    [Theory]
    [InlineData("replies")]
    [InlineData("orders.replies-1_a")]
    [InlineData("A.b_C-9")]
    public void Legal_topic_names_are_accepted(string name) =>
        Assert.True(KafkaReplyTopic.IsValidTopicName(name));

    [Theory]
    [InlineData("")]
    [InlineData(null)]
    [InlineData(".")]
    [InlineData("..")]
    [InlineData("../orders")]
    [InlineData("orders replies")]
    [InlineData("orders/replies")]
    [InlineData("orders\n")]
    [InlineData("répliques")]
    public void Illegal_topic_names_are_rejected(string? name) =>
        Assert.False(KafkaReplyTopic.IsValidTopicName(name));

    [Fact]
    public void A_topic_name_at_and_over_the_length_limit()
    {
        Assert.True(KafkaReplyTopic.IsValidTopicName(new string('a', KafkaReplyTopic.MaxLength)));
        Assert.False(
            KafkaReplyTopic.IsValidTopicName(new string('a', KafkaReplyTopic.MaxLength + 1))
        );
    }

    [Fact]
    public void An_allow_list_restricts_the_reply_topic_exactly()
    {
        var allowed = new HashSet<string>(StringComparer.Ordinal) { "orders.replies" };

        Assert.Equal("orders.replies", KafkaReplyTopic.Require("orders.replies", allowed));
        Assert.Throws<MalformedEnvelopeException>(() =>
            KafkaReplyTopic.Require("Orders.Replies", allowed)
        );
        Assert.Throws<MalformedEnvelopeException>(() =>
            KafkaReplyTopic.Require("audit.log", allowed)
        );
        // Syntax is checked before the list, and the rejected value is not echoed back.
        var exception = Assert.Throws<MalformedEnvelopeException>(() =>
            KafkaReplyTopic.Require("audit log", allowed)
        );
        Assert.DoesNotContain("audit log", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("audit log")]
    [InlineData("..")]
    public async Task A_request_naming_an_illegal_reply_topic_is_skipped_before_the_handler_runs(
        string replyTo
    )
    {
        var kafka = new FakeKafka();
        kafka.EnqueueRequest("orders", offset: 0, replyTo: replyTo);
        await using var broker = Create(kafka, new KafkaOptions());
        var handled = 0;

        await using var listener = await broker.ListenAsync(
            "orders",
            (_, _) =>
            {
                handled++;
                return ValueTask.FromResult<ReadOnlyMemory<byte>>("reply"u8.ToArray());
            },
            TestContext.Current.CancellationToken
        );

        await WaitUntilAsync(() => kafka.Commits.Count == 1, "the record is skipped");
        Assert.Equal(0, handled);
        Assert.Empty(kafka.Produced);
        Assert.Empty(kafka.Seeks);
    }

    [Fact]
    public async Task A_request_naming_a_reply_topic_outside_the_allow_list_is_skipped()
    {
        var kafka = new FakeKafka();
        kafka.EnqueueRequest("orders", offset: 0, replyTo: "somebody.elses.topic");
        kafka.EnqueueRequest("orders", offset: 1, replyTo: "billing.replies");
        var options = new KafkaOptions();
        options.AllowedReplyTopics.Add("billing.replies");
        await using var broker = Create(kafka, options);
        var handled = new List<long>();

        await using var listener = await broker.ListenAsync(
            "orders",
            (frame, _) =>
            {
                handled.Add(long.Parse(Encoding.UTF8.GetString(frame.Span), null));
                return ValueTask.FromResult<ReadOnlyMemory<byte>>("reply"u8.ToArray());
            },
            TestContext.Current.CancellationToken
        );

        await WaitUntilAsync(() => kafka.Commits.Count == 2, "both records commit");
        Assert.Equal([1], handled);
        var produced = Assert.Single(kafka.Produced);
        Assert.Equal("billing.replies", produced.Topic);
    }

    [Fact]
    public async Task A_reply_that_cannot_be_produced_is_skipped_without_re_running_the_handler()
    {
        var kafka = new FakeKafka
        {
            ProduceFailure = new KafkaException(ErrorCode.TopicAuthorizationFailed),
        };
        kafka.EnqueueRequest("orders", offset: 0, replyTo: "billing.replies");
        await using var broker = Create(kafka, new KafkaOptions());
        var handled = 0;

        await using var listener = await broker.ListenAsync(
            "orders",
            (_, _) =>
            {
                handled++;
                return ValueTask.FromResult<ReadOnlyMemory<byte>>("reply"u8.ToArray());
            },
            TestContext.Current.CancellationToken
        );

        await WaitUntilAsync(() => kafka.Commits.Count == 1, "the record is committed past");
        // Handled exactly once: the failed produce is not a reason to rewind, because the handler's
        // side effects would repeat and the reply would still have nowhere to go.
        Assert.Equal(1, handled);
        Assert.Empty(kafka.Seeks);
        Assert.Equal([1L], kafka.Commits);
    }

    [Fact]
    public async Task A_request_older_than_the_maximum_age_is_skipped_and_a_fresh_one_handled()
    {
        var clock = new TestClock();
        var kafka = new FakeKafka();
        kafka.EnqueueRequest(
            "orders",
            offset: 0,
            replyTo: "billing.replies",
            timestamp: new Timestamp(clock.GetUtcNow().AddMinutes(-10).UtcDateTime)
        );
        kafka.EnqueueRequest(
            "orders",
            offset: 1,
            replyTo: "billing.replies",
            timestamp: new Timestamp(clock.GetUtcNow().AddSeconds(-5).UtcDateTime)
        );
        await using var broker = Create(
            kafka,
            new KafkaOptions { MaxRequestAge = TimeSpan.FromMinutes(1) },
            clock
        );
        var handled = new List<long>();

        await using var listener = await broker.ListenAsync(
            "orders",
            (frame, _) =>
            {
                handled.Add(long.Parse(Encoding.UTF8.GetString(frame.Span), null));
                return ValueTask.FromResult<ReadOnlyMemory<byte>>("reply"u8.ToArray());
            },
            TestContext.Current.CancellationToken
        );

        await WaitUntilAsync(() => kafka.Commits.Count == 2, "both records commit");
        Assert.Equal([1], handled);
        Assert.Empty(kafka.Seeks);
    }

    [Fact]
    public void A_record_without_a_timestamp_is_not_judged_by_age()
    {
        var now = DateTimeOffset.UtcNow;

        KafkaRequestBroker.RejectIfStale(default, TimeSpan.FromSeconds(1), now);
        KafkaRequestBroker.RejectIfStale(
            new Timestamp(now.AddHours(-1).UtcDateTime),
            maxAge: null,
            now
        );
        Assert.Throws<MalformedEnvelopeException>(() =>
            KafkaRequestBroker.RejectIfStale(
                new Timestamp(now.AddHours(-1).UtcDateTime),
                TimeSpan.FromSeconds(1),
                now
            )
        );
    }

    private static KafkaRequestBroker Create(
        FakeKafka kafka,
        KafkaOptions options,
        TimeProvider? clock = null
    ) => new(Options.Create(options), logger: null, kafka.Producer, kafka.CreateConsumer, clock);

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

    private sealed record ProducedRecord(string Topic, string? Key, byte[] Value);

    /// <summary>A fake producer and single-partition consumer that records commits, seeks, and produces.</summary>
    private sealed class FakeKafka
    {
        private readonly Lock _gate = new();
        private readonly List<ConsumeResult<string, byte[]>> _log = [];
        private long _position;

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
                    if (ProduceFailure is { } failure)
                    {
                        return Task.FromException<DeliveryResult<string, byte[]>>(failure);
                    }

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

        public Exception? ProduceFailure { get; init; }

        public List<ProducedRecord> Produced { get; } = [];

        public List<long> Commits { get; } = [];

        public List<TopicPartitionOffset> Seeks { get; } = [];

        public void EnqueueRequest(
            string topic,
            long offset,
            string replyTo,
            Timestamp timestamp = default
        )
        {
            var headers = new Headers
            {
                { "hostloom-correlation-id", Encoding.UTF8.GetBytes(Guid.NewGuid().ToString("N")) },
                { "hostloom-reply-to", Encoding.UTF8.GetBytes(replyTo) },
            };
            _log.Add(
                new ConsumeResult<string, byte[]>
                {
                    Topic = topic,
                    Partition = new Partition(0),
                    Offset = new Offset(offset),
                    Message = new Message<string, byte[]>
                    {
                        Value = Encoding.UTF8.GetBytes(offset.ToString(null, null)),
                        Headers = headers,
                        Timestamp = timestamp,
                    },
                }
            );
        }

        public IConsumer<string, byte[]> CreateConsumer(
            ConsumerConfig config,
            PartitionsAssignedHandler? partitionsAssigned
        )
        {
            var consumer = Substitute.For<IConsumer<string, byte[]>>();
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
                            if (_position < _log.Count)
                            {
                                return _log[(int)_position++];
                            }
                        }

                        Thread.Sleep(1);
                    }
                });
            consumer
                .When(c => c.Commit(Arg.Any<ConsumeResult<string, byte[]>>()))
                .Do(call =>
                {
                    lock (_gate)
                    {
                        Commits.Add(call.ArgAt<ConsumeResult<string, byte[]>>(0).Offset.Value + 1);
                    }
                });
            consumer
                .When(c => c.Seek(Arg.Any<TopicPartitionOffset>()))
                .Do(call =>
                {
                    var target = call.ArgAt<TopicPartitionOffset>(0);
                    lock (_gate)
                    {
                        Seeks.Add(target);
                        _position = target.Offset.Value;
                    }
                });
            return consumer;
        }
    }
}
