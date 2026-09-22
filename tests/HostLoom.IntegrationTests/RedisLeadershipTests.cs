using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Leadership;
using HostLoom.Locking;
using HostLoom.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Leader election over a real Redis lock on the system clock. The outage case is an opt-in
/// network experiment behind HOSTLOOM_REDIS_CHAOS=1 with a private loopback proxy in front of the
/// leader's connection only.
/// </summary>
[Collection(nameof(RedisLeadershipTests))]
[CollectionDefinition(nameof(RedisLeadershipTests), DisableParallelization = true)]
public sealed class RedisLeadershipTests
{
    private const string ChaosSkip =
        "Set HOSTLOOM_REDIS_CHAOS=1 and start the local Redis fixture to run isolated network faults.";

    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(2);

    public static bool Redis => RedisAvailability.Redis;

    public static bool ChaosEnabled =>
        Environment.GetEnvironmentVariable("HOSTLOOM_REDIS_CHAOS") == "1"
        && RedisAvailability.Redis;

    [Fact(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Redis))]
    public async Task Two_electors_agree_on_one_leader_and_hand_over_on_stop()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "leader-" + Guid.NewGuid().ToString("N");
        var journal = new Journal();
        await using var first = await Instance.StartAsync(
            "first",
            ns,
            RedisAvailability.Configuration,
            journal,
            token
        );
        await using var second = await Instance.StartAsync(
            "second",
            ns,
            RedisAvailability.Configuration,
            journal,
            token
        );

        await CacheConformance.WaitUntilAsync(
            () => Task.FromResult(first.Elector.IsLeader || second.Elector.IsLeader),
            20
        );
        var (leader, follower) = first.Elector.IsLeader ? (first, second) : (second, first);
        await Task.Delay(TimeSpan.FromSeconds(3), token);

        // Three seconds is several renewals and several refused attempts: still one leader.
        Assert.True(leader.Elector.IsLeader);
        Assert.False(follower.Elector.IsLeader);
        Assert.Equal(0, journal.MaxLeaders - 1);
        Assert.DoesNotContain(
            journal.Changes,
            change => change.Reason == LeadershipChangeReason.Lost
        );

        await leader.Elector.StopAsync(token);
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(follower.Elector.IsLeader), 20);
        Assert.Equal(1, journal.MaxLeaders);
        Assert.Contains(
            journal.Changes,
            change =>
                change.Instance == leader.Name && change.Reason == LeadershipChangeReason.Stopped
        );
    }

    [Fact(Timeout = 90_000, Skip = ChaosSkip, SkipUnless = nameof(ChaosEnabled))]
    public async Task A_leader_cut_from_Redis_steps_down_and_the_follower_takes_over_without_overlap()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "leader-outage-" + Guid.NewGuid().ToString("N");
        var journal = new Journal();
        await using var proxy = new TcpFaultProxy(RedisAvailability.Host, RedisAvailability.Port);
        // The leader-to-be goes through the proxy; the follower talks to Redis directly.
        await using var first = await Instance.StartAsync(
            "first",
            ns,
            proxy.Configuration,
            journal,
            token
        );
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(first.Elector.IsLeader), 20);
        await using var second = await Instance.StartAsync(
            "second",
            ns,
            RedisAvailability.Configuration,
            journal,
            token
        );
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        Assert.True(first.Elector.IsLeader);
        Assert.False(second.Elector.IsLeader);

        proxy.SetEnabled(false);
        try
        {
            // Renewals fail at the proxy; the leader steps down within a renewal plus a timeout,
            // and the follower acquires once the server-side lease expires.
            await CacheConformance.WaitUntilAsync(
                () => Task.FromResult(!first.Elector.IsLeader),
                20
            );
            await CacheConformance.WaitUntilAsync(
                () => Task.FromResult(second.Elector.IsLeader),
                20
            );
        }
        finally
        {
            proxy.SetEnabled(true);
        }

        await Task.Delay(TimeSpan.FromSeconds(3), token);
        Assert.True(second.Elector.IsLeader);
        Assert.False(first.Elector.IsLeader);
        var lost = Assert.Single(
            journal.Changes,
            change => change.Instance == "first" && change.Reason == LeadershipChangeReason.Lost
        );
        var acquired = Assert.Single(
            journal.Changes,
            change =>
                change.Instance == "second" && change.Reason == LeadershipChangeReason.Acquired
        );

        // The lease tier's documented window: the follower takes the lease when it expires on
        // the server, while the cut-off leader believes it leads until its local lease timer
        // fires. Both are bounded by the lease, so any overlap is shorter than one lease.
        var overlap = lost.At - acquired.At;
        Assert.True(
            overlap < Lease,
            $"the previous leader believed it led for {overlap} after the follower acquired, longer than the {Lease} lease"
        );
        Assert.InRange(journal.MaxLeaders, 1, 2);
        Assert.Equal(
            2,
            journal.Changes.Count(change => change.Reason == LeadershipChangeReason.Acquired)
        );
    }

    /// <summary>Every change from every instance with a timestamp, and the most leaders believed at once.</summary>
    private sealed class Journal
    {
        private readonly Lock _gate = new();
        private readonly List<(
            string Instance,
            LeadershipChangeReason Reason,
            DateTimeOffset At
        )> _changes = [];
        private readonly HashSet<string> _leading = [];

        public int MaxLeaders { get; private set; }

        public IReadOnlyList<(
            string Instance,
            LeadershipChangeReason Reason,
            DateTimeOffset At
        )> Changes
        {
            get
            {
                lock (_gate)
                {
                    return [.. _changes];
                }
            }
        }

        public void Record(string instance, LeadershipChange change)
        {
            lock (_gate)
            {
                _changes.Add((instance, change.Reason, DateTimeOffset.UtcNow));
                if (change.IsLeader)
                {
                    _leading.Add(instance);
                }
                else
                {
                    _leading.Remove(instance);
                }

                MaxLeaders = Math.Max(MaxLeaders, _leading.Count);
            }
        }
    }

    private sealed class Instance : IAsyncDisposable
    {
        private readonly RedisConnection _connection;
        private readonly RedisLockProvider _provider;
        private readonly DistributedLock _locks;
        private readonly IDisposable _subscription;

        private Instance(
            string name,
            RedisConnection connection,
            RedisLockProvider provider,
            DistributedLock locks,
            LeaderElector elector,
            Journal journal
        )
        {
            Name = name;
            _connection = connection;
            _provider = provider;
            _locks = locks;
            Elector = elector;
            _subscription = elector.OnChange(change => journal.Record(name, change));
        }

        public string Name { get; }

        public LeaderElector Elector { get; }

        public static async Task<Instance> StartAsync(
            string name,
            string ns,
            string configuration,
            Journal journal,
            CancellationToken token
        )
        {
            // CA2000: ownership of the connection, provider, lock, and elector transfers to the instance.
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
            var instance = new Instance(name, connection, provider, locks, elector, journal);
#pragma warning restore CA2000
            await elector.StartAsync(token);
            return instance;
        }

        public async ValueTask DisposeAsync()
        {
            _subscription.Dispose();
            await Elector.DisposeAsync();
            await _locks.DisposeAsync();
            await _provider.DisposeAsync();
            await _connection.DisposeAsync();
        }
    }
}
