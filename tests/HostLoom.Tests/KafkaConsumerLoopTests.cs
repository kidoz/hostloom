using System.Diagnostics;
using System.Diagnostics.Metrics;
using System.Text.Json;
using Confluent.Kafka;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Drives the Kafka consumer loop against a fake <see cref="IConsumer{TKey,TValue}"/>, so the
/// offset behaviour is verified without a broker. The fake mirrors the one semantic that matters:
/// a partition tracks a single committed position, and committing a result commits its offset + 1.
/// </summary>
public sealed class KafkaConsumerLoopTests
{
    [Fact]
    public void A_record_with_no_headers_is_malformed_rather_than_a_transient_fault()
    {
        // A record produced without headers — by an operations tool or a replay — carries a null
        // collection, not an empty one. Dereferencing it would classify the record as a transient
        // fault and cost it the full redelivery and backoff budget before being discarded, where
        // the loop commits and skips a malformed envelope immediately.
        var exception = Assert.Throws<MalformedEnvelopeException>(() =>
            KafkaRequestBroker.GetRequiredHeader(null, "hostloom-correlation-id")
        );

        Assert.Contains("hostloom-correlation-id", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_record_missing_one_header_is_malformed()
    {
        var exception = Assert.Throws<MalformedEnvelopeException>(() =>
            KafkaRequestBroker.GetRequiredHeader(new Headers(), "hostloom-reply-to")
        );

        Assert.Contains("hostloom-reply-to", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_present_header_is_decoded_as_utf8()
    {
        var headers = new Headers { { "hostloom-reply-to", "replies"u8.ToArray() } };

        Assert.Equal("replies", KafkaRequestBroker.GetRequiredHeader(headers, "hostloom-reply-to"));
    }

    [Fact]
    public async Task Failed_record_is_redelivered_before_any_later_record_commits()
    {
        var log = new PartitionLog("requests", 2);
        var handled = new List<long>();
        var failed = false;

        await using (
            Start(
                log,
                (record, _) =>
                {
                    handled.Add(record.Offset.Value);
                    if (record.Offset.Value == 0 && !failed)
                    {
                        failed = true;
                        throw new InvalidOperationException("transient");
                    }

                    return ValueTask.CompletedTask;
                }
            )
        )
        {
            await WaitUntilAsync(() => log.Commits.Count == 2, "both records commit");
        }

        // The record that failed is seen again before the next one, and the commits advance in
        // order. Consuming past offset 0 would have committed 2 and dropped it for good.
        Assert.Equal([0, 0, 1], handled);
        Assert.Equal([0], log.Seeks.Select(seek => seek.Offset.Value));
        Assert.Equal([1, 2], log.Commits.Select(commit => commit.Offset.Value));
    }

    [Fact]
    public async Task Malformed_record_is_committed_past_without_a_rewind()
    {
        var log = new PartitionLog("requests", 2);
        var handled = new List<long>();

        await using (
            Start(
                log,
                (record, _) =>
                {
                    handled.Add(record.Offset.Value);
                    if (record.Offset.Value == 0)
                    {
                        throw new MalformedEnvelopeException("undecodable");
                    }

                    return ValueTask.CompletedTask;
                }
            )
        )
        {
            await WaitUntilAsync(() => log.Commits.Count == 2, "both records commit");
        }

        // A poison record can never succeed, so it is skipped immediately rather than retried.
        Assert.Equal([0, 1], handled);
        Assert.Empty(log.Seeks);
        Assert.Equal([1, 2], log.Commits.Select(commit => commit.Offset.Value));
    }

    [Fact]
    public async Task An_event_record_without_a_message_type_is_committed_past_rather_than_rewound()
    {
        // Decoded by the real event dispatcher. An explicit null message type used to reach the
        // subscriber lookup as an argument error, which an event subscription rewinds until
        // shutdown while its partition waits; as a malformed envelope it is skipped at once.
        var handled = 0;
        using var host = await WireEnvelopeValidationTests.StartAsync(() => handled++);
        var broker = (WireEnvelopeValidationTests.FrameBroker)
            host.Services.GetRequiredService<IRequestBroker>();
        var log = new PartitionLog("orders", 2)
        {
            ValueAt = offset =>
                EventFrame(offset == 0 ? null : TypeName<WireEnvelopeValidationTests.Placed>()),
        };

        await using (
            Start(
                log,
                (record, _) => broker.DeliverEventAsync("orders", record.Message.Value),
                retryIndefinitely: true
            )
        )
        {
            await WaitUntilAsync(() => log.Commits.Count == 2, "both records commit");
        }

        Assert.Empty(log.Seeks);
        Assert.Equal([1, 2], log.Commits.Select(commit => commit.Offset.Value));
        Assert.Equal(1, handled);
    }

    [Fact]
    public async Task Application_invalid_data_exception_uses_the_redelivery_policy()
    {
        var log = new PartitionLog("requests", 1);
        var handled = 0;

        await using (
            Start(
                log,
                (record, _) =>
                {
                    handled++;
                    if (handled == 1)
                    {
                        throw new InvalidDataException("application validation failed");
                    }

                    return ValueTask.CompletedTask;
                }
            )
        )
        {
            await WaitUntilAsync(() => log.Commits.Count == 1, "the retried record commits");
        }

        Assert.Equal(2, handled);
        Assert.Equal([0], log.Seeks.Select(seek => seek.Offset.Value));
        Assert.Equal([1], log.Commits.Select(commit => commit.Offset.Value));
    }

    [Fact]
    public async Task Record_that_never_succeeds_is_skipped_after_the_redelivery_cap()
    {
        var log = new PartitionLog("requests", 1);
        var handled = 0;

        await using (
            Start(
                log,
                (_, _) =>
                {
                    handled++;
                    throw new InvalidOperationException("always");
                }
            )
        )
        {
            await WaitUntilAsync(() => log.Commits.Count == 1, "the record is skipped");
        }

        Assert.Equal(ConsumerSubscription.MaxRedeliveryAttempts, handled);
        Assert.Equal(ConsumerSubscription.MaxRedeliveryAttempts - 1, log.Seeks.Count);
        Assert.Equal([1], log.Commits.Select(commit => commit.Offset.Value));
    }

    [Fact]
    public async Task A_stuck_partition_does_not_block_a_healthy_one()
    {
        var log = new PartitionLog("requests", 1, 1);
        var handledByPartition = new Dictionary<int, int>();

        await using (
            Start(
                log,
                (record, _) =>
                {
                    var partition = record.Partition.Value;
                    handledByPartition[partition] =
                        handledByPartition.GetValueOrDefault(partition) + 1;
                    if (partition == 0)
                    {
                        throw new InvalidOperationException("partition 0 is down");
                    }

                    return ValueTask.CompletedTask;
                }
            )
        )
        {
            await WaitUntilAsync(() => log.Commits.Count == 2, "both partitions commit");
        }

        // Retry state is tracked per partition, so partition 0 exhausting its attempts neither
        // consumes partition 1's budget nor holds up its delivery.
        Assert.Equal(ConsumerSubscription.MaxRedeliveryAttempts, handledByPartition[0]);
        Assert.Equal(1, handledByPartition[1]);
        Assert.All(log.Seeks, seek => Assert.Equal(0, seek.Partition.Value));
        Assert.Equal(
            [1],
            log.Commits.Where(c => c.Partition.Value == 0).Select(c => c.Offset.Value)
        );
        Assert.Equal(
            [1],
            log.Commits.Where(c => c.Partition.Value == 1).Select(c => c.Offset.Value)
        );
    }

    [Fact]
    public async Task Commit_failure_does_not_rerun_completed_handlers()
    {
        var log = new PartitionLog("requests", 2) { CommitFailuresRemaining = 1 };
        var handled = new List<long>();
        await using (
            Start(
                log,
                (record, _) =>
                {
                    handled.Add(record.Offset.Value);
                    return ValueTask.CompletedTask;
                }
            )
        )
        {
            await WaitUntilAsync(
                () => log.Commits.Count == 1,
                "the later completed record commits"
            );
        }
        Assert.Equal([0L, 1L], handled);
        Assert.Empty(log.Seeks);
        Assert.Equal(2, Assert.Single(log.Commits).Offset.Value);
    }

    [Fact]
    public async Task Event_failure_beyond_five_attempts_is_retained_until_success()
    {
        var log = new PartitionLog("events", 1);
        var handled = 0;
        await using (
            Start(
                log,
                (_, _) =>
                {
                    if (++handled <= 6)
                        throw new InvalidOperationException("temporarily unavailable");
                    return ValueTask.CompletedTask;
                },
                retryIndefinitely: true
            )
        )
        {
            await WaitUntilAsync(() => log.Commits.Count == 1, "the recovered handler commits");
        }
        Assert.Equal(7, handled);
        Assert.Equal(6, log.Seeks.Count);
    }

    [Fact]
    public async Task A_poison_record_is_metered_as_consumed_committed_and_skipped_as_malformed()
    {
        var log = new PartitionLog(UniqueTopic(), 2);
        using var metrics = new MetricRecorder(log.Topic);

        await using (
            Start(
                log,
                (record, _) =>
                    record.Offset.Value == 0
                        ? throw new MalformedEnvelopeException("undecodable")
                        : ValueTask.CompletedTask
            )
        )
        {
            await WaitUntilAsync(() => log.Commits.Count == 2, "both records commit");
        }

        // Asserted after disposal, which joins the loop, so every measurement has landed.
        Assert.Equal(2, metrics.Sum("hostloom.kafka.consumed"));
        Assert.Equal(2, metrics.Sum("hostloom.kafka.committed"));
        Assert.Equal(
            1,
            metrics.Sum("hostloom.kafka.records.skipped", ("hostloom.kafka.reason", "malformed"))
        );
        Assert.Equal(1, metrics.Sum("hostloom.kafka.records.skipped"));
        Assert.Equal(0, metrics.Sum("hostloom.kafka.records.rewound"));
        Assert.Equal(0, metrics.Sum("hostloom.kafka.loop.faults"));
    }

    [Fact]
    public async Task A_transient_failure_is_metered_as_a_rewind_and_the_redelivery_as_consumed()
    {
        var log = new PartitionLog(UniqueTopic(), 2);
        using var metrics = new MetricRecorder(log.Topic);
        var failed = false;

        await using (
            Start(
                log,
                (record, _) =>
                {
                    if (record.Offset.Value == 0 && !failed)
                    {
                        failed = true;
                        throw new InvalidOperationException("transient");
                    }

                    return ValueTask.CompletedTask;
                }
            )
        )
        {
            await WaitUntilAsync(() => log.Commits.Count == 2, "both records commit");
        }

        Assert.Equal(3, metrics.Sum("hostloom.kafka.consumed"));
        Assert.Equal(2, metrics.Sum("hostloom.kafka.committed"));
        Assert.Equal(1, metrics.Sum("hostloom.kafka.records.rewound"));
        Assert.Equal(0, metrics.Sum("hostloom.kafka.records.skipped"));
    }

    [Fact]
    public async Task A_record_that_exhausts_its_attempts_is_metered_as_skipped()
    {
        var log = new PartitionLog(UniqueTopic(), 1);
        using var metrics = new MetricRecorder(log.Topic);

        await using (Start(log, (_, _) => throw new InvalidOperationException("always")))
        {
            await WaitUntilAsync(() => log.Commits.Count == 1, "the record is skipped");
        }

        Assert.Equal(
            ConsumerSubscription.MaxRedeliveryAttempts,
            metrics.Sum("hostloom.kafka.consumed")
        );
        Assert.Equal(
            ConsumerSubscription.MaxRedeliveryAttempts - 1,
            metrics.Sum("hostloom.kafka.records.rewound")
        );
        Assert.Equal(
            1,
            metrics.Sum(
                "hostloom.kafka.records.skipped",
                ("hostloom.kafka.reason", "attempts_exhausted")
            )
        );
        Assert.Equal(1, metrics.Sum("hostloom.kafka.committed"));
    }

    [Fact]
    public async Task A_commit_failure_is_metered_as_a_commit_fault_without_a_rewind()
    {
        var log = new PartitionLog(UniqueTopic(), 2) { CommitFailuresRemaining = 1 };
        using var metrics = new MetricRecorder(log.Topic);

        await using (Start(log, (_, _) => ValueTask.CompletedTask))
        {
            await WaitUntilAsync(
                () => log.Commits.Count == 1,
                "the later completed record commits"
            );
        }

        Assert.Equal(2, metrics.Sum("hostloom.kafka.consumed"));
        Assert.Equal(1, metrics.Sum("hostloom.kafka.committed"));
        Assert.Equal(
            1,
            metrics.Sum("hostloom.kafka.loop.faults", ("hostloom.kafka.stage", "commit"))
        );
        Assert.Equal(1, metrics.Sum("hostloom.kafka.loop.faults"));
        Assert.Equal(0, metrics.Sum("hostloom.kafka.records.rewound"));
    }

    private static string UniqueTopic() => $"metrics-{Guid.NewGuid():N}";

    private static string TypeName<T>() =>
        $"{typeof(T).Assembly.GetName().Name}:{typeof(T).FullName}";

    private static byte[] EventFrame(string? messageType) =>
        JsonSerializer.SerializeToUtf8Bytes(
            new
            {
                messageId = Guid.NewGuid(),
                kind = "Event",
                messageType,
                responseType = "",
                sentAt = DateTimeOffset.UnixEpoch,
                body = JsonSerializer.SerializeToUtf8Bytes(
                    new WireEnvelopeValidationTests.Placed("A-1")
                ),
            }
        );

    /// <summary>
    /// Collects Kafka consumer-loop measurements for one topic. The meter is process-wide and
    /// static, so filtering by the destination tag keeps concurrently running tests apart.
    /// </summary>
    private sealed class MetricRecorder : IDisposable
    {
        private readonly string _topic;
        private readonly MeterListener _listener = new();
        private readonly List<(
            string Name,
            double Value,
            KeyValuePair<string, object?>[] Tags
        )> _measurements = [];
        private readonly Lock _gate = new();

        public MetricRecorder(string topic)
        {
            _topic = topic;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == KafkaDiagnostics.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.Start();
        }

        public double Sum(string instrument, params (string Key, string Value)[] tags)
        {
            lock (_gate)
            {
                return _measurements
                    .Where(m => m.Name == instrument && tags.All(tag => Has(m.Tags, tag)))
                    .Sum(m => m.Value);
            }
        }

        public void Dispose() => _listener.Dispose();

        private static bool Has(
            KeyValuePair<string, object?>[] tags,
            (string Key, string Value) expected
        ) => tags.Any(tag => tag.Key == expected.Key && (tag.Value as string) == expected.Value);

        private void Record(
            Instrument instrument,
            long value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags
        )
        {
            var copy = tags.ToArray();
            if (!Has(copy, ("messaging.destination.name", _topic)))
            {
                return;
            }

            lock (_gate)
            {
                _measurements.Add((instrument.Name, value, copy));
            }
        }
    }

    private static ConsumerSubscription Start(
        PartitionLog log,
        Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler,
        bool retryIndefinitely = false
    )
    {
        var consumer = Substitute.For<IConsumer<string, byte[]>>();
        consumer
            .Consume(Arg.Any<CancellationToken>())
            .Returns(call => log.Next(call.Arg<CancellationToken>()));
        consumer
            .When(c => c.Commit(Arg.Any<ConsumeResult<string, byte[]>>()))
            .Do(call => log.Commit(call.Arg<ConsumeResult<string, byte[]>>()));
        consumer
            .When(c => c.Seek(Arg.Any<TopicPartitionOffset>()))
            .Do(call => log.Seek(call.Arg<TopicPartitionOffset>()));

        return ConsumerSubscription.Start(
            consumer,
            log.Topic,
            handler,
            NullLogger.Instance,
            TimeSpan.FromMilliseconds(1),
            commitOnSuccess: true,
            retryIndefinitely: retryIndefinitely
        );
    }

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

    /// <summary>
    /// An in-memory partitioned log. Records are served round-robin across partitions, and a seek
    /// rewinds only the partition it names, matching how a real assignment behaves.
    /// </summary>
    private sealed class PartitionLog
    {
        private readonly Dictionary<int, int> _counts = [];
        private readonly Dictionary<int, long> _positions = [];
        private readonly Lock _gate = new();
        private int _cursor;

        public PartitionLog(string topic, params int[] recordsPerPartition)
        {
            Topic = topic;
            for (var partition = 0; partition < recordsPerPartition.Length; partition++)
            {
                _counts[partition] = recordsPerPartition[partition];
                _positions[partition] = 0;
            }
        }

        public string Topic { get; }

        public int CommitFailuresRemaining { get; set; }

        /// <summary>The record value served at an offset; empty unless a test decodes it.</summary>
        public Func<long, byte[]> ValueAt { get; init; } = static _ => [];

        public List<TopicPartitionOffset> Commits { get; } = [];

        public List<TopicPartitionOffset> Seeks { get; } = [];

        public ConsumeResult<string, byte[]> Next(CancellationToken cancellationToken)
        {
            while (true)
            {
                cancellationToken.ThrowIfCancellationRequested();
                lock (_gate)
                {
                    var partitions = _counts.Keys.Order().ToList();
                    for (var i = 0; i < partitions.Count; i++)
                    {
                        var partition = partitions[(_cursor + i) % partitions.Count];
                        if (_positions[partition] >= _counts[partition])
                        {
                            continue;
                        }

                        var offset = _positions[partition]++;
                        _cursor = (_cursor + i + 1) % partitions.Count;
                        return Record(partition, offset);
                    }
                }

                // Nothing available; the loop is idle until the test disposes the subscription.
                Thread.Sleep(1);
            }
        }

        // Mirrors Confluent's documented behaviour: Commit(result) commits result.Offset + 1.
        public void Commit(ConsumeResult<string, byte[]> record)
        {
            if (CommitFailuresRemaining-- > 0)
                throw new KafkaException(ErrorCode.NotCoordinatorForGroup);
            lock (_gate)
            {
                Commits.Add(
                    new TopicPartitionOffset(
                        record.TopicPartition,
                        new Offset(record.Offset.Value + 1)
                    )
                );
            }
        }

        public void Seek(TopicPartitionOffset offset)
        {
            lock (_gate)
            {
                Seeks.Add(offset);
                _positions[offset.Partition.Value] = offset.Offset.Value;
            }
        }

        private ConsumeResult<string, byte[]> Record(int partition, long offset) =>
            new()
            {
                Topic = Topic,
                Partition = new Partition(partition),
                Offset = new Offset(offset),
                Message = new Message<string, byte[]>
                {
                    Key = $"{partition}-{offset}",
                    Value = ValueAt(offset),
                    Headers = new Headers(),
                },
            };
    }
}
