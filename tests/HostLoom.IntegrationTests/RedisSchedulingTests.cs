using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Locking;
using HostLoom.Redis;
using HostLoom.Scheduling;
using HostLoom.Scheduling.Locking;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Cluster schedules over a real Redis lock: several scheduler instances with their own
/// connections contend for one exclusive schedule on the system clock. The outage case is an
/// opt-in network experiment behind HOSTLOOM_REDIS_CHAOS=1 with a private loopback proxy per test.
/// </summary>
[Collection(nameof(RedisSchedulingTests))]
[CollectionDefinition(nameof(RedisSchedulingTests), DisableParallelization = true)]
public sealed class RedisSchedulingTests
{
    private const string ChaosSkip =
        "Set HOSTLOOM_REDIS_CHAOS=1 and start the local Redis fixture to run isolated network faults.";

    private static readonly TimeSpan Period = TimeSpan.FromMilliseconds(400);

    public static bool Redis => RedisAvailability.Redis;

    public static bool ChaosEnabled =>
        Environment.GetEnvironmentVariable("HOSTLOOM_REDIS_CHAOS") == "1"
        && RedisAvailability.Redis;

    [Fact(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Redis))]
    public async Task Three_instances_run_an_exclusive_schedule_one_at_a_time()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "schedule-" + Guid.NewGuid().ToString("N");
        var ledger = new Ledger();
        await using var first = await Instance.StartAsync(
            "first",
            ns,
            RedisAvailability.Configuration,
            ledger,
            token
        );
        await using var second = await Instance.StartAsync(
            "second",
            ns,
            RedisAvailability.Configuration,
            ledger,
            token
        );
        await using var third = await Instance.StartAsync(
            "third",
            ns,
            RedisAvailability.Configuration,
            ledger,
            token
        );

        await CacheConformance.WaitUntilAsync(() => Task.FromResult(ledger.Succeeded >= 5), 30);
        await CacheConformance.WaitUntilAsync(
            () =>
                Task.FromResult(
                    new[] { first, second, third }.Sum(i => i.Count(ScheduleRunOutcome.Skipped))
                        >= 3
                ),
            30
        );

        // The invariant: the job body never overlaps across instances, however the claims fell.
        Assert.Equal(0, ledger.MaxOverlap);
        Assert.Equal(0, ledger.Failed);
        Assert.Contains(
            ScheduleRunOutcome.Skipped,
            new[] { first, second, third }.SelectMany(i => i.Outcomes)
        );
    }

    [Fact(Timeout = 60_000, Skip = ChaosSkip, SkipUnless = nameof(ChaosEnabled))]
    public async Task A_Redis_outage_skips_occurrences_on_every_instance_and_recovery_resumes_one_at_a_time()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "schedule-outage-" + Guid.NewGuid().ToString("N");
        var ledger = new Ledger();
        await using var proxy = new RedisFaultProxy();
        await using var first = await Instance.StartAsync(
            "first",
            ns,
            proxy.Configuration,
            ledger,
            token,
            throughProxy: true
        );
        await using var second = await Instance.StartAsync(
            "second",
            ns,
            proxy.Configuration,
            ledger,
            token,
            throughProxy: true
        );

        // Baseline: the schedule runs and the claims contend.
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(ledger.Succeeded >= 3), 30);
        var beforeFault = ledger.Succeeded;

        proxy.SetEnabled(false);
        try
        {
            await CacheConformance.WaitUntilAsync(
                () =>
                    Task.FromResult(
                        first.Count(ScheduleRunOutcome.GuardFailed) >= 1
                            && second.Count(ScheduleRunOutcome.GuardFailed) >= 1
                    ),
                30
            );
            var duringFault = ledger.Succeeded;
            await Task.Delay(Period * 3, token);
            // Nothing runs while nobody can claim: a job that must not run twice does not run at all.
            Assert.Equal(duringFault, ledger.Succeeded);
            Assert.Equal(0, ledger.MaxOverlap);
        }
        finally
        {
            proxy.SetEnabled(true);
        }

        await CacheConformance.WaitUntilAsync(
            () => Task.FromResult(ledger.Succeeded >= beforeFault + 3),
            40
        );
        Assert.Equal(0, ledger.MaxOverlap);
        Assert.Equal(0, ledger.Failed);
        Assert.True(
            first.Count(ScheduleRunOutcome.GuardFailed)
                + second.Count(ScheduleRunOutcome.GuardFailed)
                >= 2
        );
    }

    /// <summary>What every instance's job reports into: successes, failures, and overlap.</summary>
    private sealed class Ledger
    {
        private readonly Lock _gate = new();
        private int _inFlight;

        public int Succeeded { get; private set; }

        public int Failed { get; private set; }

        public int MaxOverlap { get; private set; }

        public async ValueTask RunAsync(CancellationToken token)
        {
            lock (_gate)
            {
                _inFlight++;
                MaxOverlap = Math.Max(MaxOverlap, _inFlight - 1);
            }

            try
            {
                // Long enough that a second instance claiming the same occurrence would overlap.
                await Task.Delay(100, token);
                lock (_gate)
                {
                    Succeeded++;
                }
            }
            catch (Exception)
            {
                lock (_gate)
                {
                    Failed++;
                }

                throw;
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

    private sealed class Instance : IAsyncDisposable
    {
        private readonly RedisConnection _connection;
        private readonly RedisLockProvider _provider;
        private readonly DistributedLock _locks;
        private readonly Lock _gate = new();
        private readonly List<ScheduleRunOutcome> _outcomes = [];
        private long _counted;

        private Instance(
            RedisConnection connection,
            RedisLockProvider provider,
            DistributedLock locks,
            Scheduler scheduler
        )
        {
            _connection = connection;
            _provider = provider;
            _locks = locks;
            Scheduler = scheduler;
        }

        public Scheduler Scheduler { get; }

        public IReadOnlyList<ScheduleRunOutcome> Outcomes
        {
            get
            {
                Observe();
                lock (_gate)
                {
                    return [.. _outcomes];
                }
            }
        }

        public static async Task<Instance> StartAsync(
            string name,
            string ns,
            string configuration,
            Ledger ledger,
            CancellationToken token,
            bool throughProxy = false
        )
        {
            // CA2000: ownership of the connection, provider, lock, and scheduler transfers to the instance.
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
            var scheduler = new Scheduler(
                new SchedulingOptions { DefaultLease = TimeSpan.FromSeconds(1) },
                [
                    new ScheduleDefinition(
                        "catalog:rebuild",
                        ScheduleTrigger.FixedRate(Period),
                        (_, cancellation) => ledger.RunAsync(cancellation),
                        new ScheduleOptions { Exclusive = true, Timeout = TimeSpan.FromSeconds(5) }
                    ),
                ],
                new DistributedLockScheduleGuard(locks)
            );
            _ = name;
            _ = throughProxy;
            await scheduler.StartAsync(token);
            return new Instance(connection, provider, locks, scheduler);
#pragma warning restore CA2000
        }

        public int Count(ScheduleRunOutcome outcome)
        {
            Observe();
            lock (_gate)
            {
                return _outcomes.Count(o => o == outcome);
            }
        }

        public async ValueTask DisposeAsync()
        {
            await Scheduler.DisposeAsync();
            await _locks.DisposeAsync();
            await _provider.DisposeAsync();
            await _connection.DisposeAsync();
        }

        /// <summary>Folds the scheduler's last outcome into the history once per completed run.</summary>
        private void Observe()
        {
            var state = Scheduler.GetState("catalog:rebuild");
            lock (_gate)
            {
                if (state.LastOutcome is { } outcome && !state.Running && state.Runs > _counted)
                {
                    _counted = state.Runs;
                    _outcomes.Add(outcome);
                }
            }
        }
    }
}
