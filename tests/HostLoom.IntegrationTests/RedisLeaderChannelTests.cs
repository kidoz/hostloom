using System.Globalization;
using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Leadership;
using HostLoom.Locking;
using HostLoom.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// A leader-gated channel on each of two instances over a real Redis lock and the system clock,
/// fed by one clock-driven producer that writes every item to both, as replicas consuming one
/// topic would. The outage case is an opt-in network experiment behind HOSTLOOM_REDIS_CHAOS=1
/// with a private loopback proxy in front of the leader's connection only; it measures how many
/// items land on both instances while the cut-off leader still believes it leads.
/// </summary>
[Collection(nameof(RedisLeaderChannelTests))]
[CollectionDefinition(nameof(RedisLeaderChannelTests), DisableParallelization = true)]
public sealed class RedisLeaderChannelTests
{
    private const string ChaosSkip =
        "Set HOSTLOOM_REDIS_CHAOS=1 and start the local Redis fixture to run isolated network faults.";

    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(2);
    private static readonly TimeSpan FeedPeriod = TimeSpan.FromMilliseconds(50);

    public static bool Redis => RedisAvailability.Redis;

    public static bool ChaosEnabled =>
        Environment.GetEnvironmentVariable("HOSTLOOM_REDIS_CHAOS") == "1"
        && RedisAvailability.Redis;

    [Fact(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Redis))]
    public async Task Every_item_lands_on_one_instance_and_the_feed_follows_a_graceful_hand_over()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "leader-channel-" + Guid.NewGuid().ToString("N");
        await using var first = await Instance.StartAsync(
            "first",
            ns,
            RedisAvailability.Configuration,
            token
        );
        await using var second = await Instance.StartAsync(
            "second",
            ns,
            RedisAvailability.Configuration,
            token
        );
        await CacheConformance.WaitUntilAsync(
            () => Task.FromResult(first.Elector.IsLeader || second.Elector.IsLeader),
            20
        );
        var (leader, follower) = first.Elector.IsLeader ? (first, second) : (second, first);
        var feed = new Feed(first, second);

        // Steady state: two seconds of feed, several renewals, every item on the leader only.
        await feed.RunAsync(TimeSpan.FromSeconds(2), token);
        await leader.SettleAsync(feed.Written);
        Assert.Equal(feed.Written, leader.Received.Count);
        Assert.Empty(follower.Received);

        // A graceful stop hands over; the feed resumes once the follower leads.
        await leader.Elector.StopAsync(token);
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(follower.Elector.IsLeader), 20);
        var handOver = feed.Written;
        await feed.RunAsync(TimeSpan.FromSeconds(2), token);
        await follower.SettleAsync(feed.Written - handOver);

        Assert.Equal(Enumerable.Range(1, handOver), leader.Received.Select(item => item.Sequence));
        Assert.Equal(
            Enumerable.Range(handOver + 1, feed.Written - handOver),
            follower.Received.Select(item => item.Sequence)
        );
        Assert.Equal(feed.Written, leader.Received.Count + leader.Channel.FollowerDrops);
        Assert.Equal(feed.Written, follower.Received.Count + follower.Channel.FollowerDrops);
        Assert.Equal(0, leader.Channel.CapacityDrops + follower.Channel.CapacityDrops);
    }

    [Fact(Timeout = 90_000, Skip = ChaosSkip, SkipUnless = nameof(ChaosEnabled))]
    public async Task A_leader_cut_from_Redis_hands_the_feed_over_inside_one_lease_and_nothing_is_lost_afterwards()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "leader-channel-outage-" + Guid.NewGuid().ToString("N");
        await using var proxy = new TcpFaultProxy(RedisAvailability.Host, RedisAvailability.Port);
        // The leader-to-be goes through the proxy; the follower talks to Redis directly.
        await using var first = await Instance.StartAsync("first", ns, proxy.Configuration, token);
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(first.Elector.IsLeader), 20);
        await using var second = await Instance.StartAsync(
            "second",
            ns,
            RedisAvailability.Configuration,
            token
        );
        var feed = new Feed(first, second);
        using var feeding = CancellationTokenSource.CreateLinkedTokenSource(token);
        var producing = feed.RunAsync(Timeout.InfiniteTimeSpan, feeding.Token);

        // Baseline: one second of feed with both instances up and one leader.
        await Task.Delay(TimeSpan.FromSeconds(1), token);
        Assert.True(first.Elector.IsLeader);
        Assert.False(second.Elector.IsLeader);
        var faultAt = DateTimeOffset.UtcNow;

        proxy.SetEnabled(false);
        DateTimeOffset lostAt;
        DateTimeOffset acquiredAt;
        try
        {
            await CacheConformance.WaitUntilAsync(
                () => Task.FromResult(!first.Elector.IsLeader),
                20
            );
            lostAt = DateTimeOffset.UtcNow;
            await CacheConformance.WaitUntilAsync(
                () => Task.FromResult(second.Elector.IsLeader),
                20
            );
            acquiredAt = DateTimeOffset.UtcNow;
        }
        finally
        {
            proxy.SetEnabled(true);
        }

        // Recovery: two more seconds of feed with the connection restored.
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        await feeding.CancelAsync();
        await producing;
        var settledAt = DateTimeOffset.UtcNow;
        await second.SettleUntilAsync(feed.Written);
        Assert.True(second.Elector.IsLeader);
        Assert.False(first.Elector.IsLeader);

        var onFirst = first.Received.ToDictionary(item => item.Sequence, item => item.WrittenAt);
        var onSecond = second.Received.ToDictionary(item => item.Sequence, item => item.WrittenAt);
        var onBoth = onFirst.Keys.Intersect(onSecond.Keys).ToList();
        var onNeither = Enumerable
            .Range(1, feed.Written)
            .Where(sequence => !onFirst.ContainsKey(sequence) && !onSecond.ContainsKey(sequence))
            .ToList();

        TestContext.Current.TestOutputHelper?.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"written {feed.Written}; on both {onBoth.Count}; on neither {onNeither.Count}; "
                    + $"leader stepped down {(lostAt - faultAt).TotalMilliseconds:F0} ms after the cut; "
                    + $"follower acquired {(acquiredAt - faultAt).TotalMilliseconds:F0} ms after the cut"
            )
        );

        // Every write is buffered or counted on each instance; the gate never loses one in transit.
        Assert.Equal(feed.Written, first.Received.Count + first.Channel.FollowerDrops);
        Assert.Equal(feed.Written, second.Received.Count + second.Channel.FollowerDrops);
        Assert.Equal(0, first.Channel.CapacityDrops + second.Channel.CapacityDrops);

        // The double-delivery window is the lease tier's documented overlap: the follower holds
        // the lease from server-side expiry while the cut-off leader believes it leads until its
        // local lease timer fires. The items that landed on both span less than one lease.
        if (onBoth.Count > 0)
        {
            var span = onBoth.Max(seq => onFirst[seq]) - onBoth.Min(seq => onFirst[seq]);
            Assert.True(
                span < Lease,
                $"{onBoth.Count} items landed on both instances across {span}, longer than the {Lease} lease"
            );
        }

        // Items on neither instance were written while nobody led, between the fault and the
        // follower's acquisition, allowing one feed period of skew at either edge.
        Assert.All(
            onNeither,
            sequence =>
            {
                var writtenAt = feed.WrittenAt(sequence);
                Assert.InRange(writtenAt, faultAt - FeedPeriod, acquiredAt + FeedPeriod);
            }
        );

        // Nothing landed on the old leader after it stepped down, and everything written after
        // the new leader acquired landed on the new leader alone.
        Assert.All(onFirst.Values, writtenAt => Assert.True(writtenAt <= lostAt + FeedPeriod));
        var afterAcquisition = Enumerable
            .Range(1, feed.Written)
            .Where(sequence => feed.WrittenAt(sequence) > acquiredAt + FeedPeriod)
            .ToList();
        Assert.NotEmpty(afterAcquisition);
        Assert.All(afterAcquisition, sequence => Assert.Contains(sequence, onSecond.Keys));
        Assert.True(
            settledAt - acquiredAt >= TimeSpan.FromSeconds(2),
            "the recovery phase ran shorter than planned"
        );
    }

    /// <summary>One item of the shared feed: its sequence and when the producer wrote it.</summary>
    private sealed record Item(int Sequence, DateTimeOffset WrittenAt);

    /// <summary>One producer writing every item to every instance on a fixed period.</summary>
    private sealed class Feed(params Instance[] instances)
    {
        private readonly Lock _gate = new();
        private readonly List<DateTimeOffset> _writtenAt = [];

        public int Written
        {
            get
            {
                lock (_gate)
                {
                    return _writtenAt.Count;
                }
            }
        }

        public DateTimeOffset WrittenAt(int sequence)
        {
            lock (_gate)
            {
                return _writtenAt[sequence - 1];
            }
        }

        /// <summary>Writes on the period for <paramref name="duration"/>, or until cancelled when infinite.</summary>
        public async Task RunAsync(TimeSpan duration, CancellationToken cancellationToken)
        {
            using var timer = new PeriodicTimer(FeedPeriod);
            var deadline =
                duration == Timeout.InfiniteTimeSpan
                    ? DateTimeOffset.MaxValue
                    : DateTimeOffset.UtcNow + duration;
            try
            {
                while (
                    DateTimeOffset.UtcNow < deadline
                    && await timer.WaitForNextTickAsync(cancellationToken)
                )
                {
                    Item item;
                    lock (_gate)
                    {
                        item = new Item(_writtenAt.Count + 1, DateTimeOffset.UtcNow);
                        _writtenAt.Add(item.WrittenAt);
                    }

                    foreach (var instance in instances)
                    {
                        Assert.True(instance.Channel.Writer.TryWrite(item), instance.Name);
                    }
                }
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                // The experiment ended the feed.
            }
        }
    }

    private sealed class Instance : IAsyncDisposable
    {
        private readonly RedisConnection _connection;
        private readonly RedisLockProvider _provider;
        private readonly DistributedLock _locks;
        private readonly Lock _gate = new();
        private readonly List<Item> _received = [];
        private readonly Task _reading;

        private Instance(
            string name,
            RedisConnection connection,
            RedisLockProvider provider,
            DistributedLock locks,
            LeaderElector elector,
            LeaderChannel<Item> channel
        )
        {
            Name = name;
            _connection = connection;
            _provider = provider;
            _locks = locks;
            Elector = elector;
            Channel = channel;
            _reading = ReadAsync();
        }

        public string Name { get; }

        public LeaderElector Elector { get; }

        public LeaderChannel<Item> Channel { get; }

        /// <summary>Every item the reader has taken, in order.</summary>
        public IReadOnlyList<Item> Received
        {
            get
            {
                lock (_gate)
                {
                    return [.. _received];
                }
            }
        }

        public static async Task<Instance> StartAsync(
            string name,
            string ns,
            string configuration,
            CancellationToken token
        )
        {
            // CA2000: ownership of the connection, provider, lock, elector, and channel transfers to the instance.
#pragma warning disable CA2000
            var connection = new RedisConnection(
                new RedisOptions
                {
                    Configuration = configuration,
                    ConnectTimeout = TimeSpan.FromSeconds(1),
                    CommandTimeout = TimeSpan.FromSeconds(1),
                    HealthTimeout = TimeSpan.FromMilliseconds(300),
                }
            );
            var provider = new RedisLockProvider(connection);
            var locks = new DistributedLock(
                new LockingOptions { Namespace = ns, DefaultLease = TimeSpan.FromSeconds(2) },
                provider
            );
            var elector = new LeaderElector(
                "scheduler",
                locks,
                new LeadershipOptions
                {
                    Lease = Lease,
                    RenewInterval = TimeSpan.FromMilliseconds(500),
                    RetryInterval = TimeSpan.FromMilliseconds(300),
                    RetryJitter = TimeSpan.FromMilliseconds(100),
                }
            );
            var channel = new LeaderChannel<Item>(
                "inventory-changes",
                elector,
                new LeaderChannelOptions { Capacity = 10_000 }
            );
            var instance = new Instance(name, connection, provider, locks, elector, channel);
#pragma warning restore CA2000
            await elector.StartAsync(token);
            return instance;
        }

        /// <summary>Waits until the reader has taken <paramref name="count"/> items.</summary>
        public Task SettleAsync(int count) =>
            CacheConformance.WaitUntilAsync(() => Task.FromResult(Received.Count == count), 10);

        /// <summary>Waits until the reader has taken every item up to <paramref name="last"/> that it will get.</summary>
        public Task SettleUntilAsync(int last) =>
            CacheConformance.WaitUntilAsync(
                () => Task.FromResult(Received.Count == 0 || Received[^1].Sequence == last),
                10
            );

        private async Task ReadAsync()
        {
            await foreach (var item in Channel.Reader.ReadAllAsync())
            {
                lock (_gate)
                {
                    _received.Add(item);
                }
            }
        }

        public async ValueTask DisposeAsync()
        {
            Channel.Writer.TryComplete();
            await _reading;
            Channel.Dispose();
            await Elector.DisposeAsync();
            await _locks.DisposeAsync();
            await _provider.DisposeAsync();
            await _connection.DisposeAsync();
        }
    }
}
