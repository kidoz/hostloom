using HostLoom.Leadership;
using HostLoom.Locking;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Two electors stand in for two instances sharing one clock and one lock backend. The lock's
/// automatic-extension cap is set low on purpose, to prove the elector renews on its own.
/// </summary>
public sealed class LeadershipTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan Renew = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task Exactly_one_of_two_electors_leads_and_the_other_waits()
    {
        var cluster = new Cluster();
        await using var first = cluster.Elector("first");
        await using var second = cluster.Elector("second");
        var firstChanges = new List<LeadershipChange>();
        using var _ = first.Elector.OnChange(firstChanges.Add);

        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() =>
            first.Elector.IsLeader || second.Elector.IsLeader
        );
        var (leader, follower) = first.Elector.IsLeader ? (first, second) : (second, first);

        Assert.False(follower.Elector.IsLeader);
        Assert.Equal(1, leader.Elector.Term);
        Assert.Equal(0, follower.Elector.Term);
        Assert.False(leader.Elector.LeadershipToken.IsCancellationRequested);
        Assert.True(follower.Elector.LeadershipToken.IsCancellationRequested);
        Assert.Equal(LeadershipStatus.Leader, leader.Elector.Status);
        Assert.Equal(LeadershipStatus.Candidate, follower.Elector.Status);
        var waiting = follower.Elector.WaitForLeadershipAsync(
            TestContext.Current.CancellationToken
        );
        Assert.False(waiting.IsCompleted);
        await leader.Elector.WaitForLeadershipAsync(TestContext.Current.CancellationToken);
        Assert.Equal(1, cluster.Backend.Count);

        // The follower keeps trying on its retry cadence and keeps being refused.
        for (var i = 0; i < 3; i++)
        {
            cluster.Clock.Advance(Retry);
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.False(follower.Elector.IsLeader);
        Assert.Equal(1, leader.Elector.Term);
    }

    [Fact]
    public async Task The_leader_renews_on_its_own_past_the_lock_extension_cap()
    {
        var cluster = new Cluster(maxHold: TimeSpan.FromSeconds(20));
        await using var leader = cluster.Elector("first");
        var changes = new List<LeadershipChange>();
        using var _ = leader.Elector.OnChange(changes.Add);
        await leader.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => leader.Elector.IsLeader);

        // Four times the cap and twelve renewals: a lease left to the lock's own heartbeat would
        // have expired at the cap, and this elector would have recorded a loss.
        for (var elapsed = 0; elapsed < 80; elapsed += 5)
        {
            // The handle's lease and warning timers plus the elector's renewal timer.
            await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 3);
            cluster.Clock.Advance(Renew);
        }

        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 3);

        Assert.True(leader.Elector.IsLeader);
        Assert.Equal(1, leader.Elector.Term);
        Assert.Equal(1, cluster.Backend.Count);
        Assert.DoesNotContain(changes, change => change.Reason != LeadershipChangeReason.Acquired);
    }

    [Fact]
    public async Task A_transient_renewal_failure_keeps_leadership_and_the_next_success_keeps_the_term()
    {
        var cluster = new Cluster(extendFailures: 1);
        await using var first = cluster.Elector("first", retry: TimeSpan.FromSeconds(4));
        await using var second = cluster.Elector("second", retry: TimeSpan.FromSeconds(3));
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.IsLeader);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        var changes = new List<LeadershipChange>();
        using var _ = first.Elector.OnChange(changes.Add);
        var token = first.Elector.LeadershipToken;
        // Lease and warning timers of the handle, the leader's renewal, and the follower's retry.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        // The renewal at five seconds times out at the provider. The lease it renews still runs
        // until fifteen seconds, so the leader keeps leading and retries at the renewal cadence,
        // which is sooner than half the ten seconds left.
        cluster.Clock.Advance(Renew);
        await SchedulingTests.WaitUntilAsync(() =>
            cluster.ExtendFaults!.Faulted == 1 && cluster.Clock.PendingTimers >= 4
        );
        Assert.True(first.Elector.IsLeader);
        Assert.False(token.IsCancellationRequested);

        // The retry at ten seconds succeeds, so the lease outlives its original end at fifteen,
        // and the renewal due then succeeds as well: one term, and the follower never leads.
        cluster.Clock.Advance(Renew);
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);
        cluster.Clock.Advance(Renew);
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        Assert.True(first.Elector.IsLeader);
        Assert.False(second.Elector.IsLeader);
        Assert.False(token.IsCancellationRequested);
        Assert.Equal(1, first.Elector.Term);
        Assert.Equal(1, cluster.ExtendFaults!.Faulted);
        Assert.Equal(1, cluster.Backend.Count);
        Assert.Empty(changes);
    }

    [Fact]
    public async Task Persistent_renewal_failures_step_the_leader_down_at_the_lease_end_and_not_before()
    {
        var cluster = new Cluster(extendFailures: int.MaxValue);
        await using var leader = cluster.Elector("first");
        await leader.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => leader.Elector.IsLeader);
        var changes = new List<LeadershipChange>();
        using var _ = leader.Elector.OnChange(changes.Add);
        var token = leader.Elector.LeadershipToken;
        // The handle's lease (fifteen seconds) and warning (twelve) timers and the renewal.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 3);

        // Every renewal fails: at 5, 10, 12.5, 13.75, and 14.5 seconds. A retry comes at the
        // renewal cadence or at half of what is left, whichever is sooner, but never sooner than
        // a twentieth of the lease (0.75 s) unless that would pass the lease end at fifteen.
        (TimeSpan Step, int Timers)[] schedule =
        [
            (TimeSpan.FromSeconds(5), 3),
            (TimeSpan.FromSeconds(5), 3),
            (TimeSpan.FromSeconds(2.5), 2),
            (TimeSpan.FromSeconds(1.25), 2),
            (TimeSpan.FromSeconds(0.75), 2),
        ];
        var failures = 0;
        foreach (var (step, timers) in schedule)
        {
            cluster.Clock.Advance(step);
            failures++;
            await SchedulingTests.WaitUntilAsync(() =>
                cluster.ExtendFaults!.Faulted == failures && cluster.Clock.PendingTimers >= timers
            );
            Assert.True(leader.Elector.IsLeader);
            Assert.False(token.IsCancellationRequested);
        }

        // Half a second before the lease end the leader still leads; at the end it steps down.
        Assert.Empty(changes);
        cluster.Clock.Advance(TimeSpan.FromSeconds(0.5));
        await SchedulingTests.WaitUntilAsync(() => changes.Count == 1);

        Assert.False(leader.Elector.IsLeader);
        Assert.True(token.IsCancellationRequested);
        Assert.Equal(LeadershipChangeReason.Lost, Assert.Single(changes).Reason);
        Assert.Equal(1, leader.Elector.Term);
        Assert.Equal(LeadershipStatus.Candidate, leader.Elector.Status);
    }

    [Fact]
    public async Task A_refused_renewal_steps_the_leader_down_and_the_other_instance_takes_over()
    {
        var cluster = new Cluster(extendRefusals: 1);
        await using var first = cluster.Elector("first", retry: TimeSpan.FromSeconds(4));
        await using var second = cluster.Elector("second", retry: TimeSpan.FromSeconds(3));
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.IsLeader);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        var changes = new List<LeadershipChange>();
        using var _ = first.Elector.OnChange(changes.Add);
        var token = first.Elector.LeadershipToken;
        // Lease and warning timers of the handle, the leader's renewal, and the follower's retry.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        // The backend refuses the next renewal as an owner mismatch: the lease is gone, so the
        // elector steps down at once and releases, and the follower acquires on its next attempt.
        cluster.Clock.Advance(Renew);
        await SchedulingTests.WaitUntilAsync(() => !first.Elector.IsLeader);
        // Status changes first, the release follows, and the change is recorded last.
        await SchedulingTests.WaitUntilAsync(() => changes.Count == 1);

        Assert.True(token.IsCancellationRequested);
        Assert.Equal(LeadershipChangeReason.Lost, Assert.Single(changes).Reason);
        Assert.Equal(LeadershipStatus.Candidate, first.Elector.Status);

        // The loser waits one retry interval (four seconds) before trying again; the follower's
        // next attempt comes first, at three seconds after its last, and takes the lease.
        cluster.Clock.Advance(TimeSpan.FromSeconds(3));
        await SchedulingTests.WaitUntilAsync(() => second.Elector.IsLeader);
        cluster.Clock.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(20, TestContext.Current.CancellationToken);
        Assert.False(first.Elector.IsLeader);
        Assert.Equal(1, first.Elector.Term);
        Assert.Equal(1, second.Elector.Term);
        Assert.Equal(1, cluster.Backend.Count);
        Assert.Single(changes);
    }

    [Fact]
    public async Task Resigning_hands_over_and_the_resigner_waits_one_retry_before_running_again()
    {
        var cluster = new Cluster();
        await using var first = cluster.Elector("first", retry: TimeSpan.FromSeconds(4));
        await using var second = cluster.Elector("second", retry: TimeSpan.FromSeconds(1));
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.IsLeader);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        var changes = new List<LeadershipChange>();
        using var _ = first.Elector.OnChange(changes.Add);
        // The follower must observe contention and arm its retry before the leader resigns.
        // Otherwise its initial acquisition can legally run after the release and take the lease.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        await first.Elector.ResignAsync(TestContext.Current.CancellationToken);

        Assert.False(first.Elector.IsLeader);
        Assert.Equal(LeadershipChangeReason.Resigned, Assert.Single(changes).Reason);
        Assert.Equal(0, cluster.Backend.Count);
        // The follower's retry and the resigner's pause must both be armed before time moves.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 2);
        cluster.Clock.Advance(TimeSpan.FromSeconds(1));
        await SchedulingTests.WaitUntilAsync(() => second.Elector.IsLeader);
        Assert.Equal(LeadershipStatus.Candidate, first.Elector.Status);

        // A resign while not leading does nothing.
        await first.Elector.ResignAsync(TestContext.Current.CancellationToken);
        Assert.Single(changes);
    }

    [Fact]
    public async Task Stopping_releases_the_lease_at_once_so_the_next_instance_leads_on_its_retry()
    {
        var cluster = new Cluster();
        await using var first = cluster.Elector("first");
        await using var second = cluster.Elector("second");
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.IsLeader);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        var changes = new List<LeadershipChange>();
        using var _ = first.Elector.OnChange(changes.Add);

        // The follower must observe contention and arm its retry before the leader stops.
        // Otherwise its initial acquisition can legally run after the release and take the lease.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        await first.Elector.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(LeadershipStatus.Stopped, first.Elector.Status);
        Assert.Equal(LeadershipChangeReason.Stopped, Assert.Single(changes).Reason);
        Assert.Equal(0, cluster.Backend.Count);
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => second.Elector.IsLeader);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await first.Elector.DisposeAsync();
            await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task An_unreachable_provider_makes_nobody_leader_until_it_answers()
    {
        var cluster = new Cluster(acquireFailures: 4);
        await using var first = cluster.Elector("first");
        await using var second = cluster.Elector("second");
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => cluster.Faults >= 2);

        Assert.False(first.Elector.IsLeader || second.Elector.IsLeader);
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => cluster.Faults >= 4);
        Assert.False(first.Elector.IsLeader || second.Elector.IsLeader);

        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() =>
            first.Elector.IsLeader || second.Elector.IsLeader
        );
        Assert.NotEqual(first.Elector.IsLeader, second.Elector.IsLeader);
    }

    [Fact]
    public async Task Listeners_waiters_and_the_probe_follow_the_transitions()
    {
        var cluster = new Cluster();
        await using var elector = cluster.Elector("first");
        var changes = new List<LeadershipChange>();
        using var subscription = elector.Elector.OnChange(changes.Add);
        var waiting = elector.Elector.WaitForLeadershipAsync(TestContext.Current.CancellationToken);
        var before = LeadershipProbe.Describe(elector.Elector);
        Assert.Equal(LeadershipStatus.Idle, before.Status);

        await elector.Elector.StartAsync(TestContext.Current.CancellationToken);
        await waiting;
        var token = elector.Elector.LeadershipToken;
        await elector.Elector.ResignAsync(TestContext.Current.CancellationToken);

        Assert.True(token.IsCancellationRequested);
        Assert.Equal(
            [LeadershipChangeReason.Acquired, LeadershipChangeReason.Resigned],
            changes.Select(change => change.Reason)
        );
        Assert.Equal([true, false], changes.Select(change => change.IsLeader));
        Assert.All(changes, change => Assert.Equal("scheduler", change.Role));
        subscription.Dispose();
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => elector.Elector.IsLeader);
        Assert.Equal(2, changes.Count);
        Assert.Equal(2, elector.Elector.Term);

        var probe = LeadershipProbe.Describe(elector.Elector);
        Assert.Equal(LeadershipStatus.Leader, probe.Status);
        Assert.Equal(2, probe.Term);
        Assert.Contains(
            probe.Lines,
            line => line.StartsWith("Role = scheduler: leader, term 2", StringComparison.Ordinal)
        );
        Assert.Contains(
            probe.Lines,
            line => line.Contains("Leadership:Lease = 00:00:15", StringComparison.Ordinal)
        );
        Assert.Contains(probe.Lines, line => line.Contains("no fencing", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Options_and_roles_are_validated_with_messages_naming_the_key()
    {
        var problems = new LeadershipOptions
        {
            Lease = TimeSpan.FromSeconds(10),
            RenewInterval = TimeSpan.FromSeconds(6),
            RetryInterval = TimeSpan.Zero,
            RetryJitter = TimeSpan.FromSeconds(-1),
        }.Validate();

        Assert.Contains(
            problems,
            p => p.Contains("Leadership:RenewInterval", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            p => p.Contains("Leadership:RetryInterval", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            p => p.Contains("Leadership:RetryJitter", StringComparison.Ordinal)
        );
        Assert.Empty(new LeadershipOptions().Validate());

        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "billing" },
            new InMemoryLockProvider()
        );
        Assert.Throws<ArgumentException>(() =>
            new LeaderElector("has space", locks, new LeadershipOptions())
        );
        var invalid = Assert.Throws<ArgumentException>(() =>
            new LeaderElector("scheduler", locks, new LeadershipOptions { Lease = TimeSpan.Zero })
        );
        Assert.Contains("Leadership:Lease", invalid.Message, StringComparison.Ordinal);
        Assert.Equal("leader:scheduler", LeadershipRole.KeyFor("scheduler"));
    }

    private sealed class Cluster
    {
        private readonly ILockProvider _provider;
        private readonly TimeSpan _maxHold;
        private readonly CountingProvider? _counting;

        public Cluster(
            TimeSpan? maxHold = null,
            int acquireFailures = 0,
            int extendFailures = 0,
            int extendRefusals = 0
        )
        {
            _maxHold = maxHold ?? TimeSpan.FromMinutes(10);
            Backend = new InMemoryLockProvider(Clock);
            ILockProvider provider = Backend;
            if (extendRefusals > 0)
            {
                provider = new RefusingLockProvider(provider, extendRefusals);
            }

            if (acquireFailures > 0)
            {
                _counting = new CountingProvider(
                    new FaultingLockProvider(
                        Backend,
                        LockOperation.Acquire,
                        LockFailureKind.Unavailable,
                        acquireFailures
                    )
                );
                provider = _counting;
            }

            if (extendFailures > 0)
            {
                ExtendFaults = new FaultingLockProvider(
                    provider,
                    LockOperation.Extend,
                    LockFailureKind.Timeout,
                    extendFailures
                );
                provider = ExtendFaults;
            }

            _provider = provider;
        }

        public TestClock Clock { get; } = new();

        public InMemoryLockProvider Backend { get; }

        public int Faults => _counting?.Faults ?? 0;

        /// <summary>The decorator failing extensions with a provider Timeout, when one was asked for.</summary>
        public FaultingLockProvider? ExtendFaults { get; }

        public Instance Elector(string name, TimeSpan? retry = null)
        {
            // CA2000: ownership of the lock and the elector transfers to the instance.
#pragma warning disable CA2000
            var locks = new DistributedLock(
                new LockingOptions
                {
                    Namespace = "billing",
                    DefaultLease = Lease,
                    MaxLease = TimeSpan.FromMinutes(5),
                    MaxHold = _maxHold,
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
            return new Instance(name, elector, locks);
#pragma warning restore CA2000
        }
    }

    private sealed class Instance(string name, LeaderElector elector, DistributedLock locks)
        : IAsyncDisposable
    {
        public string Name { get; } = name;

        public LeaderElector Elector { get; } = elector;

        public async ValueTask DisposeAsync()
        {
            await Elector.DisposeAsync();
            await locks.DisposeAsync();
        }
    }

    /// <summary>Counts acquisitions that threw, so a test can wait for the outage to be observed.</summary>
    private sealed class CountingProvider(ILockProvider inner) : ILockProvider
    {
        private int _faults;

        public int Faults => Volatile.Read(ref _faults);

        public async ValueTask<bool> TryAcquireAsync(
            string key,
            string owner,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        )
        {
            try
            {
                return await inner.TryAcquireAsync(key, owner, lease, cancellationToken);
            }
            catch (LockProviderException)
            {
                Interlocked.Increment(ref _faults);
                throw;
            }
        }

        public ValueTask<bool> ReleaseAsync(
            string key,
            string owner,
            CancellationToken cancellationToken = default
        ) => inner.ReleaseAsync(key, owner, cancellationToken);

        public ValueTask<bool> ExtendAsync(
            string key,
            string owner,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        ) => inner.ExtendAsync(key, owner, lease, cancellationToken);
    }
}
