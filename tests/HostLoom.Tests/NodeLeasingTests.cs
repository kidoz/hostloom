using HostLoom.Locking;
using HostLoom.NodeLeasing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Several electors stand in for several instances sharing one clock and one lock backend. The
/// lock's automatic-extension cap is set low on purpose, to prove the elector renews on its own.
/// </summary>
public sealed class NodeLeasingTests
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan Renew = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task Two_instances_hold_distinct_ids_and_a_third_waits_when_the_pool_is_exhausted()
    {
        var cluster = new Cluster();
        await using var first = cluster.Elector("first", poolSize: 2);
        await using var second = cluster.Elector("second", poolSize: 2);
        await using var third = cluster.Elector("third", poolSize: 2);

        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        await third.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => Assigned(first, second, third).Count() == 2);
        var holders = Assigned(first, second, third).ToArray();
        var waiting = new[] { first, second, third }.Except(holders).Single();

        Assert.Equal(2, holders.Select(holder => holder.Elector.NodeId).Distinct().Count());
        Assert.All(
            holders,
            holder =>
            {
                Assert.InRange(holder.Elector.NodeId!.Value, 0, 1);
                Assert.Equal(1, holder.Elector.Term);
                Assert.False(holder.Elector.AssignmentToken.IsCancellationRequested);
                Assert.Equal(NodeLeasingStatus.Assigned, holder.Elector.Status);
            }
        );
        Assert.Null(waiting.Elector.NodeId);
        Assert.Equal(0, waiting.Elector.Term);
        Assert.True(waiting.Elector.AssignmentToken.IsCancellationRequested);
        Assert.Equal(NodeLeasingStatus.Candidate, waiting.Elector.Status);
        var pending = waiting.Elector.WaitForAssignmentAsync(TestContext.Current.CancellationToken);
        Assert.False(pending.IsCompleted);
        Assert.Equal(2, cluster.Backend.Count);

        // The waiter keeps scanning on its retry cadence and keeps finding the pool exhausted.
        for (var i = 0; i < 3; i++)
        {
            cluster.Clock.Advance(Retry);
            await Task.Delay(10, TestContext.Current.CancellationToken);
        }

        Assert.Null(waiting.Elector.NodeId);
        Assert.All(holders, holder => Assert.Equal(1, holder.Elector.Term));
        Assert.Equal(2, cluster.Backend.Count);
    }

    private static IEnumerable<Instance> Assigned(params Instance[] instances) =>
        instances.Where(instance => instance.Elector.NodeId is not null);

    [Fact]
    public async Task The_holder_renews_on_its_own_past_the_lock_extension_cap_and_keeps_its_id()
    {
        var cluster = new Cluster(maxHold: TimeSpan.FromSeconds(20));
        await using var holder = cluster.Elector("first", poolSize: 1);
        var changes = new List<NodeLeaseChange>();
        using var _ = holder.Elector.OnChange(changes.Add);
        await holder.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => holder.Elector.NodeId is not null);

        // Four times the cap and twelve renewals: a lease left to the lock's own heartbeat would
        // have expired at the cap, and this elector would have recorded a loss.
        for (var elapsed = 0; elapsed < 80; elapsed += 5)
        {
            // The handle's lease and warning timers plus the elector's renewal timer.
            await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 3);
            cluster.Clock.Advance(Renew);
        }

        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 3);

        Assert.Equal(0, holder.Elector.NodeId ?? -1);
        Assert.Equal(1, holder.Elector.Term);
        Assert.Equal(1, cluster.Backend.Count);
        Assert.DoesNotContain(changes, change => change.Reason != NodeLeaseChangeReason.Acquired);
    }

    [Fact]
    public async Task A_transient_renewal_failure_keeps_the_assignment_and_the_next_success_keeps_the_term()
    {
        var cluster = new Cluster(extendFailures: 1);
        await using var first = cluster.Elector(
            "first",
            poolSize: 1,
            retry: TimeSpan.FromSeconds(4)
        );
        await using var second = cluster.Elector(
            "second",
            poolSize: 1,
            retry: TimeSpan.FromSeconds(3)
        );
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.NodeId is not null);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        var changes = new List<NodeLeaseChange>();
        using var _ = first.Elector.OnChange(changes.Add);
        var token = first.Elector.AssignmentToken;
        // Lease and warning timers of the handle, the holder's renewal, and the waiter's retry.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        // The renewal at five seconds times out at the provider. The lease it renews still runs
        // until fifteen seconds, so the holder keeps its ID and retries at the renewal cadence,
        // which is sooner than half the ten seconds left.
        cluster.Clock.Advance(Renew);
        await SchedulingTests.WaitUntilAsync(() =>
            cluster.ExtendFaults!.Faulted == 1 && cluster.Clock.PendingTimers >= 4
        );
        Assert.Equal(0, first.Elector.NodeId ?? -1);
        Assert.False(token.IsCancellationRequested);

        // The retry at ten seconds succeeds, so the lease outlives its original end at fifteen,
        // and the renewal due then succeeds as well: one term, and the waiter never gets an ID.
        cluster.Clock.Advance(Renew);
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);
        cluster.Clock.Advance(Renew);
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        Assert.Equal(0, first.Elector.NodeId ?? -1);
        Assert.Null(second.Elector.NodeId);
        Assert.False(token.IsCancellationRequested);
        Assert.Equal(1, first.Elector.Term);
        Assert.Equal(1, cluster.ExtendFaults!.Faulted);
        Assert.Equal(1, cluster.Backend.Count);
        Assert.Empty(changes);
    }

    [Fact]
    public async Task Persistent_renewal_failures_step_the_holder_down_at_the_lease_end_and_not_before()
    {
        var cluster = new Cluster(extendFailures: int.MaxValue);
        await using var holder = cluster.Elector("first", poolSize: 1);
        await holder.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => holder.Elector.NodeId is not null);
        var changes = new List<NodeLeaseChange>();
        using var _ = holder.Elector.OnChange(changes.Add);
        var token = holder.Elector.AssignmentToken;
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
            Assert.Equal(0, holder.Elector.NodeId ?? -1);
            Assert.False(token.IsCancellationRequested);
        }

        // Half a second before the lease end the holder still holds; at the end it steps down.
        Assert.Empty(changes);
        cluster.Clock.Advance(TimeSpan.FromSeconds(0.5));
        await SchedulingTests.WaitUntilAsync(() => changes.Count == 1);

        Assert.Null(holder.Elector.NodeId);
        Assert.True(token.IsCancellationRequested);
        Assert.Equal(NodeLeaseChangeReason.Lost, Assert.Single(changes).Reason);
        Assert.Null(Assert.Single(changes).NodeId);
        Assert.Equal(1, holder.Elector.Term);
        Assert.Equal(NodeLeasingStatus.Candidate, holder.Elector.Status);
    }

    [Fact]
    public async Task A_refused_renewal_ends_the_assignment_and_the_other_instance_takes_the_id()
    {
        var cluster = new Cluster(extendRefusals: 1);
        await using var first = cluster.Elector(
            "first",
            poolSize: 1,
            retry: TimeSpan.FromSeconds(4)
        );
        await using var second = cluster.Elector(
            "second",
            poolSize: 1,
            retry: TimeSpan.FromSeconds(3)
        );
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.NodeId is not null);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        var changes = new List<NodeLeaseChange>();
        using var _ = first.Elector.OnChange(changes.Add);
        var token = first.Elector.AssignmentToken;
        // Lease and warning timers of the handle, the holder's renewal, and the waiter's retry.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        // The backend refuses the next renewal as an owner mismatch: the lease is gone, so the
        // elector steps down at once and releases, and the waiter takes the ID on its next scan.
        cluster.Clock.Advance(Renew);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.NodeId is null);
        // Status changes first, the release follows, and the change is recorded last.
        await SchedulingTests.WaitUntilAsync(() => changes.Count == 1);

        Assert.True(token.IsCancellationRequested);
        Assert.Equal(NodeLeaseChangeReason.Lost, Assert.Single(changes).Reason);
        Assert.Equal(NodeLeasingStatus.Candidate, first.Elector.Status);

        // The loser waits one retry interval (four seconds) before scanning again; the waiter's
        // next scan comes first, at three seconds after its last, and takes the ID.
        cluster.Clock.Advance(TimeSpan.FromSeconds(3));
        await SchedulingTests.WaitUntilAsync(() => second.Elector.NodeId is not null);
        cluster.Clock.Advance(TimeSpan.FromSeconds(1));
        await Task.Delay(20, TestContext.Current.CancellationToken);
        Assert.Null(first.Elector.NodeId);
        Assert.Equal(1, first.Elector.Term);
        Assert.Equal(0, second.Elector.NodeId ?? -1);
        Assert.Equal(1, second.Elector.Term);
        Assert.Equal(1, cluster.Backend.Count);
        Assert.Single(changes);
    }

    [Fact]
    public async Task Releasing_hands_over_the_id_and_the_releaser_waits_one_retry_before_scanning_again()
    {
        var cluster = new Cluster();
        await using var first = cluster.Elector(
            "first",
            poolSize: 1,
            retry: TimeSpan.FromSeconds(4)
        );
        await using var second = cluster.Elector(
            "second",
            poolSize: 1,
            retry: TimeSpan.FromSeconds(1)
        );
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.NodeId is not null);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        var changes = new List<NodeLeaseChange>();
        using var _ = first.Elector.OnChange(changes.Add);
        // The waiter must observe the exhausted pool and arm its retry before the holder
        // releases. Otherwise its initial scan can legally run after the release and take the ID.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        await first.Elector.ReleaseAsync(TestContext.Current.CancellationToken);

        Assert.Null(first.Elector.NodeId);
        Assert.Equal(NodeLeaseChangeReason.Released, Assert.Single(changes).Reason);
        Assert.Equal(0, cluster.Backend.Count);
        // The waiter's retry and the releaser's pause must both be armed before time moves.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 2);
        cluster.Clock.Advance(TimeSpan.FromSeconds(1));
        await SchedulingTests.WaitUntilAsync(() => second.Elector.NodeId is not null);
        Assert.Equal(NodeLeasingStatus.Candidate, first.Elector.Status);

        // A release while unassigned does nothing.
        await first.Elector.ReleaseAsync(TestContext.Current.CancellationToken);
        Assert.Single(changes);
    }

    [Fact]
    public async Task A_holder_that_scans_again_after_a_release_keeps_its_last_id()
    {
        var cluster = new Cluster();
        await using var elector = cluster.Elector("first", poolSize: 4);
        await elector.Elector.StartAsync(TestContext.Current.CancellationToken);
        var id = await elector.Elector.WaitForAssignmentAsync(
            TestContext.Current.CancellationToken
        );

        // The release makes the ID free; the next scan tries the last-held ID first and no one
        // else contends, so the same ID comes back in a new term.
        await elector.Elector.ReleaseAsync(TestContext.Current.CancellationToken);
        Assert.Null(elector.Elector.NodeId);
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 1);
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => elector.Elector.NodeId is not null);

        Assert.Equal(id, elector.Elector.NodeId);
        Assert.Equal(2, elector.Elector.Term);
    }

    [Fact]
    public async Task Stopping_releases_the_id_at_once_so_the_next_instance_takes_it_on_its_retry()
    {
        var cluster = new Cluster();
        await using var first = cluster.Elector("first", poolSize: 1);
        await using var second = cluster.Elector("second", poolSize: 1);
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.Elector.NodeId is not null);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        var changes = new List<NodeLeaseChange>();
        using var _ = first.Elector.OnChange(changes.Add);

        // The waiter must observe the exhausted pool and arm its retry before the holder stops.
        // Otherwise its initial scan can legally run after the release and take the ID.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 4);

        await first.Elector.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(NodeLeasingStatus.Stopped, first.Elector.Status);
        Assert.Equal(NodeLeaseChangeReason.Stopped, Assert.Single(changes).Reason);
        Assert.Equal(0, cluster.Backend.Count);
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => second.Elector.NodeId is not null);
        Assert.Equal(0, second.Elector.NodeId ?? -1);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
        {
            await first.Elector.DisposeAsync();
            await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        });
    }

    [Fact]
    public async Task An_unreachable_provider_assigns_no_ids_until_it_answers()
    {
        var cluster = new Cluster(acquireFailures: 4);
        await using var first = cluster.Elector("first", poolSize: 2);
        await using var second = cluster.Elector("second", poolSize: 2);
        await first.Elector.StartAsync(TestContext.Current.CancellationToken);
        await second.Elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => cluster.Faults >= 2);
        await WaitForRetriesAsync();

        Assert.Null(first.Elector.NodeId);
        Assert.Null(second.Elector.NodeId);
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => cluster.Faults >= 4);
        await WaitForRetriesAsync();
        Assert.Null(first.Elector.NodeId);
        Assert.Null(second.Elector.NodeId);

        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() =>
            first.Elector.NodeId is not null && second.Elector.NodeId is not null
        );
        Assert.NotEqual(first.Elector.NodeId, second.Elector.NodeId);
        Assert.Equal(2, cluster.Backend.Count);

        Task WaitForRetriesAsync() =>
            SchedulingTests.WaitUntilAsync(() =>
                cluster.Clock.PendingTimersDueAt(cluster.Clock.GetUtcNow() + Retry) == 2
            );
    }

    [Fact]
    public async Task A_throwing_listener_does_not_kill_the_loop_or_block_other_listeners()
    {
        var cluster = new Cluster();
        var logger = new RecordingLogger<NodeIdElector>();
        await using var elector = cluster.Elector("first", poolSize: 1, logger: logger);
        var changes = new List<NodeLeaseChange>();
        using var _throwing = elector.Elector.OnChange(_ =>
            throw new InvalidOperationException("The listener failed.")
        );
        using var _recording = elector.Elector.OnChange(changes.Add);

        await elector.Elector.StartAsync(TestContext.Current.CancellationToken);
        var id = await elector.Elector.WaitForAssignmentAsync(
            TestContext.Current.CancellationToken
        );
        await elector.Elector.ReleaseAsync(TestContext.Current.CancellationToken);

        Assert.Equal(
            [NodeLeaseChangeReason.Acquired, NodeLeaseChangeReason.Released],
            changes.Select(change => change.Reason)
        );
        Assert.Equal(id, changes[0].NodeId);
        Assert.Null(changes[1].NodeId);
        var failures = logger.Entries.Where(entry =>
            entry.Event.Id == NodeLeasingEvents.ListenerFailed.Id
        );
        var failure = Assert.Single(failures.Take(1));
        Assert.Equal(2, failures.Count());
        Assert.Equal(LogLevel.Warning, failure.Level);
        Assert.IsType<InvalidOperationException>(failure.Exception);
        Assert.Equal(NodeLeasingStatus.Candidate, elector.Elector.Status);

        // The loop is alive: one retry interval later it holds an ID again in a new term.
        await SchedulingTests.WaitUntilAsync(() => cluster.Clock.PendingTimers >= 1);
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => elector.Elector.NodeId is not null);
        Assert.Equal(2, elector.Elector.Term);
    }

    [Fact]
    public async Task Listeners_waiters_and_the_probe_follow_the_transitions()
    {
        var cluster = new Cluster();
        await using var elector = cluster.Elector("first", poolSize: 1);
        var changes = new List<NodeLeaseChange>();
        using var subscription = elector.Elector.OnChange(changes.Add);
        var waiting = elector.Elector.WaitForAssignmentAsync(TestContext.Current.CancellationToken);
        var before = NodeLeasingProbe.Describe(elector.Elector);
        Assert.Equal(NodeLeasingStatus.Idle, before.Status);

        await elector.Elector.StartAsync(TestContext.Current.CancellationToken);
        var id = await waiting;
        var token = elector.Elector.AssignmentToken;
        await elector.Elector.ReleaseAsync(TestContext.Current.CancellationToken);

        Assert.Equal(0, id);
        Assert.True(token.IsCancellationRequested);
        Assert.Equal(
            [NodeLeaseChangeReason.Acquired, NodeLeaseChangeReason.Released],
            changes.Select(change => change.Reason)
        );
        Assert.Equal(new int?[] { 0, null }, changes.Select(change => change.NodeId));
        Assert.All(changes, change => Assert.Equal("orders-workers", change.Pool));
        subscription.Dispose();
        cluster.Clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => elector.Elector.NodeId is not null);
        Assert.Equal(2, changes.Count);
        Assert.Equal(2, elector.Elector.Term);

        var probe = NodeLeasingProbe.Describe(elector.Elector);
        Assert.Equal(NodeLeasingStatus.Assigned, probe.Status);
        Assert.Equal(0, probe.NodeId ?? -1);
        Assert.Equal(2, probe.Term);
        Assert.Contains(
            probe.Lines,
            line =>
                line.StartsWith(
                    "Pool = orders-workers: assigned, node 0, term 2",
                    StringComparison.Ordinal
                )
        );
        Assert.Contains(
            probe.Lines,
            line => line.Contains("NodeLeasing:Lease = 00:00:15", StringComparison.Ordinal)
        );
        Assert.Contains(probe.Lines, line => line.Contains("no fencing", StringComparison.Ordinal));
    }

    [Fact]
    public async Task Options_and_pools_are_validated_with_messages_naming_the_key()
    {
        var problems = new NodeLeasingOptions
        {
            PoolSize = 0,
            Lease = TimeSpan.FromSeconds(10),
            RenewInterval = TimeSpan.FromSeconds(6),
            RetryInterval = TimeSpan.Zero,
            RetryJitter = TimeSpan.FromSeconds(-1),
        }.Validate();

        Assert.Contains(
            problems,
            problem => problem.Contains("NodeLeasing:PoolSize", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            problem => problem.Contains("NodeLeasing:RenewInterval", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            problem => problem.Contains("NodeLeasing:RetryInterval", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            problem => problem.Contains("NodeLeasing:RetryJitter", StringComparison.Ordinal)
        );
        Assert.Empty(new NodeLeasingOptions().Validate());
        Assert.Empty(new NodeLeasingOptions { PoolSize = 1 }.Validate());
        Assert.Empty(new NodeLeasingOptions { PoolSize = 65535 }.Validate());
        Assert.Contains(
            new NodeLeasingOptions { PoolSize = 65536 }.Validate(),
            problem => problem.Contains("NodeLeasing:PoolSize", StringComparison.Ordinal)
        );
        Assert.Contains(
            new NodeLeasingOptions
            {
                WhenUncoordinated = (UncoordinatedNodeAssignment)7,
            }.Validate(),
            problem => problem.Contains("NodeLeasing:WhenUncoordinated", StringComparison.Ordinal)
        );

        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "orders" },
            new InMemoryLockProvider()
        );
        Assert.Throws<ArgumentException>(() =>
            new NodeIdElector("has space", locks, new NodeLeasingOptions())
        );
        Assert.Throws<ArgumentException>(() =>
            new NodeIdElector("orders_workers", locks, new NodeLeasingOptions())
        );
        var invalid = Assert.Throws<ArgumentException>(() =>
            new NodeIdElector(
                "orders-workers",
                locks,
                new NodeLeasingOptions { Lease = TimeSpan.Zero }
            )
        );
        Assert.Contains("NodeLeasing:Lease", invalid.Message, StringComparison.Ordinal);
        Assert.Equal("nodeid:orders-workers:0", NodeLeasingPool.KeyFor("orders-workers", 0));
    }

    private sealed class Cluster
    {
        private readonly ILockProvider _provider;
        private readonly TimeSpan _maxHold;

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
                AcquireFaults = new HostLoom.Locking.Testing.FaultingLockProvider(Backend);
                AcquireFaults.Fail(LockFailureKind.Unavailable, acquireFailures);
                provider = AcquireFaults;
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

        public int Faults => AcquireFaults?.Faulted ?? 0;

        /// <summary>The decorator failing acquisitions as provider outages, when one was asked for.</summary>
        public HostLoom.Locking.Testing.FaultingLockProvider? AcquireFaults { get; }

        /// <summary>The decorator failing extensions with a provider Timeout, when one was asked for.</summary>
        public FaultingLockProvider? ExtendFaults { get; }

        public Instance Elector(
            string name,
            int poolSize,
            TimeSpan? retry = null,
            RecordingLogger<NodeIdElector>? logger = null
        )
        {
            // CA2000: ownership of the lock and the elector transfers to the instance.
#pragma warning disable CA2000
            var locks = new DistributedLock(
                new LockingOptions
                {
                    Namespace = "orders",
                    DefaultLease = Lease,
                    MaxLease = TimeSpan.FromMinutes(5),
                    MaxHold = _maxHold,
                },
                _provider,
                Clock
            );
            var elector = new NodeIdElector(
                "orders-workers",
                locks,
                new NodeLeasingOptions
                {
                    PoolSize = poolSize,
                    Lease = Lease,
                    RenewInterval = Renew,
                    RetryInterval = retry ?? Retry,
                    RetryJitter = TimeSpan.Zero,
                },
                Clock,
                logger
            );
            return new Instance(name, elector, locks);
#pragma warning restore CA2000
        }
    }

    private sealed class Instance(string name, NodeIdElector elector, DistributedLock locks)
        : IAsyncDisposable
    {
        public string Name { get; } = name;

        public NodeIdElector Elector { get; } = elector;

        public async ValueTask DisposeAsync()
        {
            await Elector.DisposeAsync();
            await locks.DisposeAsync();
        }
    }
}
