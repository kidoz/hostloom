using HostLoom.Locking;
using HostLoom.Scheduling;
using HostLoom.Scheduling.Locking;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Two scheduler instances stand in for two pods sharing one clock and one lock backend. Local
/// schedules run everywhere; exclusive schedules run on one instance per occurrence, and the
/// failure cases show what each instance records when the guard or the lease misbehaves.
/// </summary>
public sealed class SchedulingClusterTests
{
    private static readonly TimeSpan Period = TimeSpan.FromSeconds(10);

    [Fact]
    public async Task A_local_schedule_runs_on_every_instance()
    {
        var cluster = new Cluster();
        await using var first = cluster.Instance("first", exclusive: false);
        await using var second = cluster.Instance("second", exclusive: false);

        await Cluster.StartAsync(first, second);
        await cluster.WaitForRunsAsync(1, first, second);
        cluster.Clock.Advance(Period);
        await cluster.WaitForRunsAsync(2, first, second);

        Assert.Equal(
            ["first", "first", "second", "second"],
            cluster.Executed.Order(StringComparer.Ordinal)
        );
        Assert.All(
            new[] { first, second },
            pod => Assert.Equal(ScheduleRunOutcome.Succeeded, pod.State.LastOutcome)
        );
    }

    [Fact]
    public async Task An_exclusive_schedule_runs_on_exactly_one_instance_per_occurrence()
    {
        var cluster = new Cluster();
        await using var first = cluster.Instance("first", exclusive: true);
        await using var second = cluster.Instance("second", exclusive: true);

        await Cluster.StartAsync(first, second);
        await cluster.WaitForRunsAsync(1, first, second);
        Assert.Single(cluster.Executed);
        Assert.Equal(1, Cluster.Outcomes(ScheduleRunOutcome.Succeeded, first, second));
        Assert.Equal(1, Cluster.Outcomes(ScheduleRunOutcome.Skipped, first, second));

        for (var occurrence = 2; occurrence <= 4; occurrence++)
        {
            cluster.Clock.Advance(Period);
            await cluster.WaitForRunsAsync(occurrence, first, second);
            Assert.Equal(occurrence, cluster.Executed.Count);
        }

        Assert.Equal(4, Cluster.Outcomes(ScheduleRunOutcome.Succeeded, first, second));
        Assert.Equal(4, Cluster.Outcomes(ScheduleRunOutcome.Skipped, first, second));
        Assert.Equal(0, cluster.MaxConcurrent);
    }

    [Fact]
    public async Task A_claim_is_kept_for_the_lease_so_a_later_instance_is_refused_then_admitted()
    {
        // One instance is alone at the occurrence; a second reaches it inside the lease and is
        // refused, then reaches it after the lease and may run.
        var cluster = new Cluster(lease: TimeSpan.FromSeconds(3));
        await using var first = cluster.Instance("first", exclusive: true);
        await Cluster.StartAsync(first);
        await cluster.WaitForRunsAsync(1, first);
        Assert.Equal(
            cluster.Clock.GetUtcNow() + TimeSpan.FromSeconds(3),
            first.State.ClaimHeldUntil
        );

        await using var late = cluster.Instance("late", exclusive: true);
        await Cluster.StartAsync(late);
        await cluster.WaitForRunsAsync(1, late);
        Assert.Equal(ScheduleRunOutcome.Skipped, late.State.LastOutcome);
        Assert.Single(cluster.Executed);

        // The first instance releases at the lease end, before its own next occurrence; the late
        // one, whose next occurrence comes after that, then claims for itself.
        cluster.Clock.Advance(TimeSpan.FromSeconds(3));
        await SchedulingTests.WaitUntilAsync(() => first.State.ClaimHeldUntil is null);
        cluster.Clock.Advance(Period - TimeSpan.FromSeconds(3));
        await cluster.WaitForRunsAsync(2, late);
        Assert.Equal(ScheduleRunOutcome.Succeeded, late.State.LastOutcome);
    }

    [Fact]
    public async Task An_instance_that_stops_mid_run_hands_the_next_occurrence_to_another()
    {
        var cluster = new Cluster(jobBlocks: true);
        await using var first = cluster.Instance("first", exclusive: true);
        await using var second = cluster.Instance("second", exclusive: true);

        await Cluster.StartAsync(first, second);
        await cluster.WaitForRunsAsync(1, first, second);
        var runner = cluster.Executed.Single() == "first" ? first : second;
        var other = ReferenceEquals(runner, first) ? second : first;
        Assert.True(runner.State.Running);
        Assert.Equal(ScheduleRunOutcome.Skipped, other.State.LastOutcome);

        // A graceful stop cancels the run and releases the claim; the other instance takes the
        // next occurrence. A crash would leave the key to expire at the lease end instead.
        await runner.Scheduler.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(ScheduleRunOutcome.Canceled, runner.State.LastOutcome);
        Assert.Equal(0, cluster.Backend.Count);

        cluster.Clock.Advance(Period);
        await cluster.WaitForRunsAsync(2, other);
        Assert.True(other.State.Running);
        Assert.Equal([runner.Name, other.Name], cluster.Executed);
        Assert.Equal(1, cluster.Backend.Count);
    }

    [Fact]
    public async Task A_lock_backend_outage_skips_the_occurrence_on_every_instance_until_it_recovers()
    {
        var cluster = new Cluster(acquireFailures: 2);
        await using var first = cluster.Instance("first", exclusive: true);
        await using var second = cluster.Instance("second", exclusive: true);

        await Cluster.StartAsync(first, second);
        await cluster.WaitForRunsAsync(1, first, second);

        // Nobody can tell whether another instance runs the job, so nobody runs it.
        Assert.Empty(cluster.Executed);
        Assert.All(
            new[] { first, second },
            pod => Assert.Equal(ScheduleRunOutcome.GuardFailed, pod.State.LastOutcome)
        );

        cluster.Clock.Advance(Period);
        await cluster.WaitForRunsAsync(2, first, second);
        Assert.Single(cluster.Executed);
        Assert.Equal(1, Cluster.Outcomes(ScheduleRunOutcome.Succeeded, first, second));
        Assert.Equal(1, Cluster.Outcomes(ScheduleRunOutcome.Skipped, first, second));
    }

    [Fact]
    public async Task A_lease_that_cannot_be_extended_cancels_the_run_and_frees_the_schedule()
    {
        // The lock stops heartbeating at MaxHold, so a run longer than that loses its claim: the
        // job's token is cancelled, the outcome says so, and the key expires for the others.
        var cluster = new Cluster(
            jobBlocks: true,
            period: TimeSpan.FromMinutes(5),
            lease: TimeSpan.FromSeconds(10),
            maxHold: TimeSpan.FromSeconds(15)
        );
        await using var first = cluster.Instance("first", exclusive: true);
        await Cluster.StartAsync(first);
        await cluster.WaitForRunsAsync(1, first);
        Assert.True(first.State.Running);

        for (var elapsed = 0; elapsed < 30 && first.State.Running; elapsed += 5)
        {
            cluster.Clock.Advance(TimeSpan.FromSeconds(5));
            await Task.Delay(20, TestContext.Current.CancellationToken);
        }

        await SchedulingTests.WaitUntilAsync(() => !first.State.Running);
        Assert.Equal(ScheduleRunOutcome.ClaimLost, first.State.LastOutcome);
        Assert.Null(first.State.ClaimHeldUntil);
        Assert.Equal(0, cluster.Backend.Count);
    }

    /// <summary>One shared clock and lock backend, and the job bookkeeping every instance reports into.</summary>
    private sealed class Cluster
    {
        private readonly Lock _gate = new();
        private readonly List<string> _executed = [];
        private readonly TimeSpan _period;
        private readonly TimeSpan _lease;
        private readonly TimeSpan _maxHold;
        private readonly bool _jobBlocks;
        private readonly ILockProvider _provider;
        private int _inFlight;

        public Cluster(
            bool jobBlocks = false,
            TimeSpan? period = null,
            TimeSpan? lease = null,
            TimeSpan? maxHold = null,
            int acquireFailures = 0
        )
        {
            _jobBlocks = jobBlocks;
            _period = period ?? Period;
            _lease = lease ?? TimeSpan.FromSeconds(30);
            _maxHold = maxHold ?? TimeSpan.FromMinutes(10);
            Backend = new InMemoryLockProvider(Clock);
            _provider =
                acquireFailures == 0
                    ? Backend
                    : new FaultingLockProvider(
                        Backend,
                        LockOperation.Acquire,
                        LockFailureKind.Unavailable,
                        acquireFailures
                    );
        }

        public TestClock Clock { get; } = new();

        public InMemoryLockProvider Backend { get; }

        public int MaxConcurrent { get; private set; }

        public IReadOnlyList<string> Executed
        {
            get
            {
                lock (_gate)
                {
                    return [.. _executed];
                }
            }
        }

        public Instance Instance(string name, bool exclusive)
        {
            // CA2000: ownership of the lock and the scheduler transfers to the instance, which disposes both.
#pragma warning disable CA2000
            var locks = new DistributedLock(
                new LockingOptions
                {
                    Namespace = "catalog",
                    DefaultLease = _lease,
                    MaxLease = TimeSpan.FromMinutes(10),
                    MaxHold = _maxHold,
                },
                _provider,
                Clock
            );
            var scheduler = new Scheduler(
                new SchedulingOptions { DefaultLease = _lease },
                [
                    new ScheduleDefinition(
                        "catalog:rebuild",
                        ScheduleTrigger.FixedRate(_period),
                        (run, token) => RunAsync(name, token),
                        new ScheduleOptions { Exclusive = exclusive }
                    ),
                ],
                new DistributedLockScheduleGuard(locks),
                Clock
            );
            return new Instance(name, scheduler, locks);
#pragma warning restore CA2000
        }

        public static async Task StartAsync(params Instance[] instances)
        {
            foreach (var instance in instances)
            {
                await instance.Scheduler.StartAsync(TestContext.Current.CancellationToken);
            }
        }

        public Task WaitForRunsAsync(long runs, params Instance[] instances) =>
            SchedulingTests.WaitUntilAsync(() =>
                instances.All(instance => instance.State.Runs >= runs && !instance.State.Running)
                || (_jobBlocks && instances.All(instance => instance.State.Runs >= runs))
            );

        public static int Outcomes(ScheduleRunOutcome outcome, params Instance[] instances) =>
            instances.Sum(instance => instance.Count(outcome));

        private async ValueTask RunAsync(string name, CancellationToken token)
        {
            lock (_gate)
            {
                _executed.Add(name);
                _inFlight++;
                MaxConcurrent = Math.Max(MaxConcurrent, _inFlight - 1);
            }

            try
            {
                if (_jobBlocks)
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                }
            }
            finally
            {
                lock (_gate)
                {
                    _inFlight--;
                }
            }
        }
    }

    private sealed class Instance(string name, Scheduler scheduler, DistributedLock locks)
        : IAsyncDisposable
    {
        private readonly Dictionary<ScheduleRunOutcome, int> _outcomes = [];
        private long _counted;

        public string Name { get; } = name;

        public Scheduler Scheduler { get; } = scheduler;

        public ScheduleState State
        {
            get
            {
                var state = Scheduler.GetState("catalog:rebuild");
                if (state.LastOutcome is { } outcome && state.Runs > _counted && !state.Running)
                {
                    _counted = state.Runs;
                    _outcomes[outcome] = _outcomes.GetValueOrDefault(outcome) + 1;
                }

                return state;
            }
        }

        public int Count(ScheduleRunOutcome outcome)
        {
            _ = State;
            return _outcomes.GetValueOrDefault(outcome);
        }

        public async ValueTask DisposeAsync()
        {
            await Scheduler.DisposeAsync();
            await locks.DisposeAsync();
        }
    }
}
