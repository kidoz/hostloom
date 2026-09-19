using HostLoom.Leadership;
using HostLoom.Leadership.Testing;
using HostLoom.Locking;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Controlled faults against a channel gated on a real elector: a refused renewal, a provider
/// outage during candidacy, writers racing a leadership flip, a listener that throws ahead of the
/// channel's own, and disposal mid-term. Two electors over one in-process lock and a fake clock
/// stand in for two instances, and one feed writes every item to both channels, as replicas
/// consuming the same topic would. The hypothesis under every fault is the same: an item is
/// buffered on at most one instance, every write is either buffered or counted, and no writer
/// ever sees an exception.
/// </summary>
public sealed class LeaderChannelChaosTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan Renew = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task A_refused_renewal_moves_the_feed_to_the_new_leader_and_no_item_lands_on_both()
    {
        var cluster = new Cluster(extendFailures: 1);
        await using var first = cluster.Instance("first", retry: TimeSpan.FromSeconds(4));
        await using var second = cluster.Instance("second", retry: TimeSpan.FromSeconds(3));
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.IsLeader);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        // Lease and warning timers of the handle, the leader's renewal, and the follower's retry.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);
        var feed = new Feed(first, second);

        // Steady state: the leader buffers, the follower discards.
        feed.Write(10);
        Assert.Equal(Enumerable.Range(1, 10), first.Drain());
        Assert.Empty(second.Drain());

        // The follower's attempt at three seconds is refused and it re-arms for six.
        cluster.Clock.Advance(TimeSpan.FromSeconds(3));
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);
        Assert.False(second.Elector.IsLeader);

        // Fault: the renewal at five seconds is refused, the leader steps down, and for a moment
        // nobody leads. Items written then land nowhere; that is the gate closing, not a loss.
        cluster.Clock.Advance(TimeSpan.FromSeconds(2));
        await SchedulingTests.WaitUntilAsync(() => !first.Elector.IsLeader);
        Assert.False(second.Elector.IsLeader);
        feed.Write(5);
        Assert.Empty(first.Drain());
        Assert.Empty(second.Drain());

        // Recovery: the follower acquires at six seconds and the feed follows it.
        cluster.Clock.Advance(TimeSpan.FromSeconds(1));
        await SchedulingTests.WaitUntilAsync(() => second.Elector.IsLeader);
        feed.Write(10);
        Assert.Empty(first.Drain());
        Assert.Equal(Enumerable.Range(16, 10), second.Drain());

        Assert.Empty(first.Received.Intersect(second.Received));
        Assert.Equal(
            Enumerable.Range(11, 5),
            Enumerable.Range(1, feed.Written).Except(first.Received).Except(second.Received)
        );
        Assert.Equal(15, first.Channel.FollowerDrops);
        Assert.Equal(15, second.Channel.FollowerDrops);
        Assert.Equal(0, first.Channel.CapacityDrops + second.Channel.CapacityDrops);
        Assert.Equal(feed.Written, first.Received.Count + first.Channel.FollowerDrops);
        Assert.Equal(feed.Written, second.Received.Count + second.Channel.FollowerDrops);
    }

    [Fact]
    public async Task A_provider_outage_during_candidacy_discards_every_write_and_the_first_term_reports_the_tail()
    {
        var cluster = new Cluster(acquireFailures: 3);
        var logger = new RecordingLogger<LeaderChannel<int>>();
        await using var instance = cluster.Instance("first", logger: logger);
        await instance.Elector.StartAsync(TestContext.Current.CancellationToken);
        // The attempt at start fails and the candidate arms its retry; advance only once armed.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers == 1);
        Assert.Equal(1, cluster.AcquireFaults!.Faulted);
        var feed = new Feed(instance);

        // The first discard is reported at once; the rest of the outage is counted.
        feed.Write(4);
        var first = Assert.Single(logger.Entries);
        Assert.Equal(LeadershipEvents.ChannelFollowerDropped.Id, first.Event.Id);
        Assert.False(instance.Elector.IsLeader);

        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers == 1);
        Assert.Equal(2, cluster.AcquireFaults.Faulted);
        feed.Write(4);
        Assert.Single(logger.Entries);
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers == 1);
        Assert.Equal(3, cluster.AcquireFaults.Faulted);
        Assert.Empty(instance.Drain());

        // The provider heals; the next attempt acquires and the tail is flushed on that change.
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => instance.Elector.IsLeader);
        await SchedulingTests.WaitUntilAsync(() => logger.Entries.Count == 2);
        Assert.Contains("discarded 7 items", logger.Entries[1].Message, StringComparison.Ordinal);

        feed.Write(3);
        Assert.Equal([9, 10, 11], instance.Drain());
        Assert.Equal(8, instance.Channel.FollowerDrops);
        Assert.Equal(feed.Written, instance.Received.Count + instance.Channel.FollowerDrops);
    }

    [Fact]
    public async Task Writers_racing_a_leadership_flip_never_fail_and_every_item_is_buffered_or_counted()
    {
        const int writers = 4;
        const int perWriter = 2_000;
        const int flips = 50;
        var token = TestContext.Current.CancellationToken;
        using var leadership = new ManualLeadership("reconciler");
        using var channel = new LeaderChannel<int>(
            "inventory-changes",
            leadership,
            new LeaderChannelOptions { Capacity = writers * perWriter }
        );
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var producers = Enumerable
            .Range(0, writers)
            .Select(_ =>
                Task.Run(
                    async () =>
                    {
                        await gate.Task;
                        for (var i = 0; i < perWriter; i++)
                        {
                            if ((i & 1) == 0)
                            {
                                Assert.True(channel.Writer.TryWrite(i));
                            }
                            else
                            {
                                await channel.Writer.WriteAsync(i, token);
                            }
                        }
                    },
                    token
                )
            )
            .ToArray();
        var flipper = Task.Run(
            async () =>
            {
                await gate.Task;
                for (var i = 0; i < flips; i++)
                {
                    leadership.Acquire();
                    await Task.Yield();
                    leadership.Lose();
                    await Task.Yield();
                }

                leadership.Acquire();
            },
            token
        );

        gate.SetResult();
        await Task.WhenAll([.. producers, flipper]).WaitAsync(TimeSpan.FromSeconds(30), token);

        var buffered = 0;
        while (channel.Reader.TryRead(out _))
        {
            buffered++;
        }

        Assert.Equal(writers * perWriter, buffered + channel.FollowerDrops);
        Assert.Equal(0, channel.CapacityDrops);
        Assert.InRange(buffered, 1, writers * perWriter - 1);
        Assert.Equal(flips + 1, leadership.Term);
    }

    [Fact]
    public async Task A_listener_that_throws_ahead_of_the_channel_does_not_lose_the_drop_summary()
    {
        var cluster = new Cluster();
        var logger = new RecordingLogger<LeaderChannel<int>>();
        await using var instance = cluster.Instance(
            "first",
            logger: logger,
            beforeChannel: elector =>
                elector.OnChange(_ => throw new InvalidOperationException("a broken listener"))
        );
        var feed = new Feed(instance);
        feed.Write(3);
        Assert.Single(logger.Entries);

        // The elector logs the broken listener and keeps going; the channel's own listener runs.
        await instance.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => instance.Elector.IsLeader);
        await SchedulingTests.WaitUntilAsync(() => logger.Entries.Count == 2);
        Assert.Contains("discarded 2 items", logger.Entries[1].Message, StringComparison.Ordinal);

        feed.Write(2);
        Assert.Equal([4, 5], instance.Drain());
    }

    [Fact]
    public async Task A_channel_disposed_mid_term_stays_gated_and_the_elector_stops_cleanly()
    {
        var cluster = new Cluster();
        var logger = new RecordingLogger<LeaderChannel<int>>();
        await using var instance = cluster.Instance("first", logger: logger);
        await instance.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => instance.Elector.IsLeader);
        var feed = new Feed(instance);
        feed.Write(2);
        Assert.Equal([1, 2], instance.Drain());

        instance.Channel.Dispose();
        Assert.Empty(logger.Entries);

        // Still gated after disposal: the elector stops, the gate closes, and writes are
        // discarded. They fall inside the report interval of the disposal flush, so they are
        // counted; a second disposal flushes them, and a third has nothing left to report.
        await instance.Elector.StopAsync(TestContext.Current.CancellationToken);
        Assert.False(instance.Elector.IsLeader);
        feed.Write(2);
        Assert.Empty(instance.Drain());
        Assert.Equal(2, instance.Channel.FollowerDrops);
        Assert.Empty(logger.Entries);
        instance.Channel.Dispose();
        var summary = Assert.Single(logger.Entries);
        Assert.Equal(LeadershipEvents.ChannelFollowerDropped.Id, summary.Event.Id);
        Assert.Contains("discarded 2 items", summary.Message, StringComparison.Ordinal);
        instance.Channel.Dispose();
        Assert.Single(logger.Entries);
    }

    /// <summary>One lock backend, one clock, and the fault decorators every instance shares.</summary>
    private sealed class Cluster
    {
        private readonly ILockProvider _provider;

        public Cluster(int acquireFailures = 0, int extendFailures = 0)
        {
            Backend = new InMemoryLockProvider(Clock);
            ILockProvider provider = Backend;
            if (acquireFailures > 0)
            {
                AcquireFaults = new FaultingLockProvider(
                    provider,
                    LockOperation.Acquire,
                    LockFailureKind.Unavailable,
                    acquireFailures
                );
                provider = AcquireFaults;
            }

            if (extendFailures > 0)
            {
                provider = new FaultingLockProvider(
                    provider,
                    LockOperation.Extend,
                    LockFailureKind.Timeout,
                    extendFailures
                );
            }

            _provider = provider;
        }

        public TestClock Clock { get; } = new();

        public InMemoryLockProvider Backend { get; }

        public FaultingLockProvider? AcquireFaults { get; }

        public Instance Instance(
            string name,
            TimeSpan? retry = null,
            ILogger? logger = null,
            Action<LeaderElector>? beforeChannel = null
        )
        {
            // CA2000: ownership of the lock, the elector, and the channel transfers to the instance.
#pragma warning disable CA2000
            var locks = new DistributedLock(
                new LockingOptions
                {
                    Namespace = "billing",
                    DefaultLease = Lease,
                    MaxLease = TimeSpan.FromMinutes(5),
                    MaxHold = TimeSpan.FromMinutes(10),
                },
                _provider,
                Clock
            );
            var elector = new LeaderElector(
                "scheduler",
                locks,
                new LeadershipOptions
                {
                    Lease = Lease,
                    RenewInterval = Renew,
                    RetryInterval = retry ?? Retry,
                    RetryJitter = TimeSpan.Zero,
                },
                Clock
            );
            beforeChannel?.Invoke(elector);
            var channel = new LeaderChannel<int>(
                "inventory-changes",
                elector,
                new LeaderChannelOptions { Capacity = 1_000 },
                logger,
                Clock
            );
#pragma warning restore CA2000
            return new Instance(name, elector, locks, channel);
        }
    }

    private sealed class Instance(
        string name,
        LeaderElector elector,
        DistributedLock locks,
        LeaderChannel<int> channel
    ) : IAsyncDisposable
    {
        public string Name { get; } = name;

        public LeaderElector Elector { get; } = elector;

        public LeaderChannel<int> Channel { get; } = channel;

        /// <summary>Every item the reader has taken so far, across every drain.</summary>
        public List<int> Received { get; } = [];

        public List<int> Drain()
        {
            List<int> items = [];
            while (Channel.Reader.TryRead(out var item))
            {
                items.Add(item);
            }

            Received.AddRange(items);
            return items;
        }

        public async ValueTask DisposeAsync()
        {
            Channel.Dispose();
            await Elector.DisposeAsync();
            await locks.DisposeAsync();
        }
    }

    /// <summary>One feed every instance consumes: each item is written to every channel.</summary>
    private sealed class Feed(params Instance[] instances)
    {
        public int Written { get; private set; }

        public void Write(int count)
        {
            for (var i = 0; i < count; i++)
            {
                var item = ++Written;
                foreach (var instance in instances)
                {
                    Assert.True(instance.Channel.Writer.TryWrite(item), instance.Name);
                }
            }
        }
    }
}
