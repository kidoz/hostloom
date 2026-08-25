using System.Diagnostics;
using Confluent.Kafka;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.Logging.Abstractions;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Drives the Kafka consumer loop against a fake <see cref="IConsumer{TKey,TValue}"/>, so the
/// offset behaviour is verified without a broker. The fake mirrors the one semantic that matters:
/// a partition tracks a single committed position, and committing a result commits its offset + 1.
/// </summary>
/// <remarks>
/// These handlers commit on success, because that is the real handler's job — the production one
/// commits only after the reply has been produced. The loop itself commits only when skipping.
/// </remarks>
public sealed class KafkaConsumerLoopTests
{
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

                    log.Commit(record);
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
                        throw new InvalidDataException("undecodable");
                    }

                    log.Commit(record);
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

                    log.Commit(record);
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

    private static ConsumerSubscription Start(
        PartitionLog log,
        Func<ConsumeResult<string, byte[]>, CancellationToken, ValueTask> handler
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
            TimeSpan.FromMilliseconds(1)
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
                    Value = [],
                    Headers = new Headers(),
                },
            };
    }
}
