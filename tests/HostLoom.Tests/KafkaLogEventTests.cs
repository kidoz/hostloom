using System.Reflection;
using Confluent.Kafka;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.Logging;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Every line the Kafka transport logs carries a stable, named event id from its own block, so an
/// operator can filter or alert on a consumer-loop condition without matching message text. The
/// loop is driven by a scripted fake consumer through each path that logs.
/// </summary>
public sealed class KafkaLogEventTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Fact]
    public void Kafka_log_events_have_stable_ids_and_names()
    {
        (EventId Event, int Id, string Name)[] expected =
        [
            (KafkaEvents.ConsumeFailed, 1421, "KafkaConsumeFailed"),
            (KafkaEvents.RecordMalformed, 1422, "KafkaRecordMalformed"),
            (KafkaEvents.ReplyUnroutable, 1423, "KafkaReplyUnroutable"),
            (KafkaEvents.RecordRewound, 1424, "KafkaRecordRewound"),
            (KafkaEvents.RecordAttemptsExhausted, 1425, "KafkaRecordAttemptsExhausted"),
            (KafkaEvents.SeekFailed, 1426, "KafkaSeekFailed"),
            (KafkaEvents.CommitFailed, 1427, "KafkaCommitFailed"),
            (KafkaEvents.LoopFaulted, 1428, "KafkaConsumerLoopFaulted"),
            (KafkaEvents.ConsumerCloseFailed, 1429, "KafkaConsumerCloseFailed"),
            (KafkaEvents.ConsumerCleanupFailed, 1430, "KafkaConsumerCleanupFailed"),
        ];

        foreach (var (logged, id, name) in expected)
        {
            // EventId equality compares only the number, so the name is checked on its own.
            Assert.Equal((id, name), (logged.Id, logged.Name));
        }

        // The table covers every id the transport declares.
        var declared = typeof(KafkaEvents)
            .GetFields(BindingFlags.Public | BindingFlags.Static)
            .Where(field => field.FieldType == typeof(EventId))
            .Select(field => (EventId)field.GetValue(null)!)
            .Select(logged => logged.Id)
            .Order();
        Assert.Equal(expected.Select(entry => entry.Id).Order(), declared);
    }

    [Fact(Timeout = 30_000)]
    public async Task Every_consumer_loop_log_line_carries_its_stable_event_id()
    {
        var logger = new RecordingLogger<KafkaRequestBroker>();
        var script = new Queue<Func<ConsumeResult<string, byte[]>>>();
        script.Enqueue(() => throw new KafkaException(ErrorCode.Local_Transport));
        script.Enqueue(() => Record(0));
        script.Enqueue(() => Record(1));
        script.Enqueue(() => Record(2));
        for (var attempt = 0; attempt < 5; attempt++)
        {
            script.Enqueue(() => Record(3));
        }

        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        consumer
            .Consume(Arg.Any<CancellationToken>())
            .Returns(call => Next(script, call.Arg<CancellationToken>()));
        consumer
            .When(c => c.Commit(Arg.Is<ConsumeResult<string, byte[]>>(r => r.Offset == 1)))
            .Do(_ => throw new KafkaException(ErrorCode.NotCoordinatorForGroup));
        consumer
            .When(c => c.Seek(Arg.Is<TopicPartitionOffset>(p => p.Offset == 2)))
            .Do(_ => throw new KafkaException(ErrorCode.Local_State));
        consumer.When(c => c.Close()).Do(_ => throw new KafkaException(ErrorCode.Local_Transport));

        var subscription = ConsumerSubscription.Start(
            consumer,
            "orders",
            (record, _) =>
                record.Offset.Value switch
                {
                    0 => throw new MalformedEnvelopeException("undecodable"),
                    1 => throw UnroutableReplyException.For(
                        "orders.replies",
                        new KafkaException(ErrorCode.TopicAuthorizationFailed)
                    ),
                    _ => throw new InvalidOperationException("inventory unavailable"),
                },
            logger,
            TimeSpan.FromMilliseconds(1),
            commitOnSuccess: true
        );
        await SchedulingTests.WaitUntilAsync(() => logger.Has(new EventId(1425)));
        await subscription
            .DisposeAsync()
            .AsTask()
            .WaitAsync(Bound, TestContext.Current.CancellationToken);

        Assert.All(logger.Entries, entry => Assert.Equal(LogLevel.Error, entry.Level));
        Assert.Equal(
            [
                (1421, "KafkaConsumeFailed"),
                (1422, "KafkaRecordMalformed"),
                (1423, "KafkaReplyUnroutable"),
                (1427, "KafkaCommitFailed"),
                (1424, "KafkaRecordRewound"),
                (1426, "KafkaSeekFailed"),
                (1424, "KafkaRecordRewound"),
                (1424, "KafkaRecordRewound"),
                (1424, "KafkaRecordRewound"),
                (1424, "KafkaRecordRewound"),
                (1425, "KafkaRecordAttemptsExhausted"),
                (1429, "KafkaConsumerCloseFailed"),
            ],
            logger.Entries.Select(entry => (entry.Event.Id, entry.Event.Name))
        );
    }

    [Fact(Timeout = 30_000)]
    public async Task A_consumer_loop_that_faults_is_logged_with_its_stable_event_id()
    {
        // A failure inside the loop's own code is a defect with no other route out; a logging
        // provider that throws is the one way to provoke it from outside.
        var logger = new ThrowingOnceLogger(1424);
        var consumed = 0;
        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        consumer
            .Consume(Arg.Any<CancellationToken>())
            .Returns(call =>
            {
                var token = call.Arg<CancellationToken>();
                if (Interlocked.Increment(ref consumed) == 1)
                {
                    return Record(0);
                }

                token.WaitHandle.WaitOne();
                throw new OperationCanceledException(token);
            });

        var subscription = ConsumerSubscription.Start(
            consumer,
            "orders",
            (_, _) => throw new InvalidOperationException("inventory unavailable"),
            logger,
            TimeSpan.FromMilliseconds(1)
        );
        await SchedulingTests.WaitUntilAsync(() => logger.Thrown);
        await subscription
            .DisposeAsync()
            .AsTask()
            .WaitAsync(Bound, TestContext.Current.CancellationToken);

        var faulted = Assert.Single(logger.Inner.Entries, entry => entry.Event.Id == 1428);
        Assert.Equal("KafkaConsumerLoopFaulted", faulted.Event.Name);
        Assert.Equal(LogLevel.Error, faulted.Level);
    }

    [Fact]
    public void A_consumer_that_fails_to_dispose_after_a_failed_subscribe_is_logged_with_its_stable_event_id()
    {
        var logger = new RecordingLogger<KafkaRequestBroker>();
        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        var failure = new KafkaException(ErrorCode.Local_InvalidArg);
        consumer.When(c => c.Subscribe(Arg.Any<string>())).Do(_ => throw failure);
        consumer
            .When(c => c.Dispose())
            .Do(_ => throw new InvalidOperationException("cleanup failed"));

        Assert.Same(
            failure,
            Assert.Throws<KafkaException>(() =>
                KafkaRequestBroker.SubscribeOwned(consumer, "orders", () => false, this, logger)
            )
        );

        var cleanup = Assert.Single(logger.Entries);
        Assert.Equal((1430, "KafkaConsumerCleanupFailed"), (cleanup.Event.Id, cleanup.Event.Name));
        Assert.Equal(LogLevel.Debug, cleanup.Level);
        Assert.Contains("orders", cleanup.Message, StringComparison.Ordinal);
    }

    private static ConsumeResult<string, byte[]> Next(
        Queue<Func<ConsumeResult<string, byte[]>>> script,
        CancellationToken token
    )
    {
        Func<ConsumeResult<string, byte[]>>? step;
        lock (script)
        {
            script.TryDequeue(out step);
        }

        if (step is not null)
        {
            return step();
        }

        // The script is over; the loop idles until the subscription is disposed.
        token.WaitHandle.WaitOne();
        throw new OperationCanceledException(token);
    }

    private static ConsumeResult<string, byte[]> Record(long offset) =>
        new()
        {
            Topic = "orders",
            Partition = new Partition(0),
            Offset = new Offset(offset),
            Message = new Message<string, byte[]>
            {
                Key = $"order-{offset}",
                Value = [],
                Headers = new Headers(),
            },
        };

    /// <summary>Records every line, but throws instead of recording the first with one event id.</summary>
    private sealed class ThrowingOnceLogger(int failOn) : ILogger
    {
        private int _thrown;

        public RecordingLogger<KafkaRequestBroker> Inner { get; } = new();

        public bool Thrown => Volatile.Read(ref _thrown) == 1;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        )
        {
            if (eventId.Id == failOn && Interlocked.Exchange(ref _thrown, 1) == 0)
            {
                throw new InvalidOperationException("The logging provider failed.");
            }

            Inner.Log(logLevel, eventId, state, exception, formatter);
        }
    }
}
