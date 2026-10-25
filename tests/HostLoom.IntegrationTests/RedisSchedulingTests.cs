using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Locking;
using HostLoom.Redis;
using HostLoom.Scheduling;
using HostLoom.Scheduling.Locking;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.IntegrationTests;

[Collection(nameof(RedisSchedulingTests))]
[CollectionDefinition(nameof(RedisSchedulingTests), DisableParallelization = true)]
public sealed class RedisSchedulingTests
{
    private const string ChaosSkip =
        "Set HOSTLOOM_REDIS_CHAOS=1 and start the local Redis fixture to run isolated network faults.";
    private static readonly TimeSpan Period = TimeSpan.FromMilliseconds(400);
    private static CancellationToken Token => TestContext.Current.CancellationToken;
    public static bool Redis => RedisAvailability.Redis;
    public static bool ChaosEnabled =>
        Environment.GetEnvironmentVariable("HOSTLOOM_REDIS_CHAOS") == "1" && Redis;

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
        await UntilAsync(() => ledger.Succeeded >= 5);
        await UntilAsync(() =>
            new[] { first, second, third }.Sum(i => i.Count(SchedulingEvents.RunSkipped)) >= 3
        );
        AssertHealthy(ledger);
    }

    [Fact(Timeout = 60_000, Skip = ChaosSkip, SkipUnless = nameof(ChaosEnabled))]
    public async Task A_Redis_outage_skips_occurrences_on_every_instance_and_recovery_resumes_one_at_a_time()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "schedule-outage-" + Guid.NewGuid().ToString("N");
        var ledger = new Ledger();
        await using var proxy = new TcpFaultProxy(RedisAvailability.Host, RedisAvailability.Port);
        await using var first = await Instance.StartAsync(
            "first",
            ns,
            proxy.Configuration,
            ledger,
            token
        );
        await using var second = await Instance.StartAsync(
            "second",
            ns,
            proxy.Configuration,
            ledger,
            token
        );
        await UntilAsync(() => ledger.Succeeded >= 3);

        // Both sequential loops must reach the gate after finishing their previous job and
        // releasing its claim. No accepted job can straddle this particular fault boundary.
        first.Guard.Pause();
        second.Guard.Pause();
        try
        {
            await Task.WhenAll(first.Guard.Blocked, second.Guard.Blocked)
                .WaitAsync(TimeSpan.FromSeconds(10), Token);
            var baseline = ledger.Succeeded;
            ledger.Phase = "outage";
            proxy.SetEnabled(false);
            first.Guard.Resume();
            second.Guard.Resume();
            await UntilAsync(() =>
                first.Count(SchedulingEvents.GuardFailed) >= 3
                && second.Count(SchedulingEvents.GuardFailed) >= 3
            );
            Assert.Equal(baseline, ledger.Succeeded);
            Assert.DoesNotContain(ledger.Runs, run => run.StartPhase == "outage");
            ledger.Phase = "recovery";
            proxy.SetEnabled(true);
            await UntilAsync(() => ledger.Succeeded >= baseline + 3);
            AssertHealthy(ledger);
        }
        finally
        {
            proxy.SetEnabled(true);
            first.Guard.Resume();
            second.Guard.Resume();
            ledger.Dump();
        }
    }

    [Fact(Timeout = 60_000, Skip = ChaosSkip, SkipUnless = nameof(ChaosEnabled))]
    public async Task A_Redis_outage_during_a_job_cancels_it_on_claim_loss_and_recovers_without_overlap()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "schedule-held-" + Guid.NewGuid().ToString("N");
        var ledger = new Ledger(holdFirst: true);
        await using var proxy = new TcpFaultProxy(RedisAvailability.Host, RedisAvailability.Port);
        await using var first = await Instance.StartAsync(
            "first",
            ns,
            proxy.Configuration,
            ledger,
            token
        );
        await ledger.Held.WaitAsync(TimeSpan.FromSeconds(10), Token);
        await using var second = await Instance.StartAsync(
            "second",
            ns,
            proxy.Configuration,
            ledger,
            token
        );
        try
        {
            ledger.Phase = "outage";
            proxy.SetEnabled(false);
            await UntilAsync(() => first.Count(SchedulingEvents.ClaimLost) == 1);
            await UntilAsync(() =>
                first.Count(SchedulingEvents.GuardFailed) >= 1
                && second.Count(SchedulingEvents.GuardFailed) >= 1
            );
            var canceled = Assert.Single(ledger.Runs, run => run.Outcome == "canceled");
            Assert.Equal("first", canceled.Instance);
            Assert.Equal("outage", canceled.EndPhase);
            Assert.True(canceled.TokenCanceled);
            Assert.IsAssignableFrom<OperationCanceledException>(canceled.Error);
            var beforeRecovery = ledger.Succeeded;
            ledger.Phase = "recovery";
            proxy.SetEnabled(true);
            await UntilAsync(() => ledger.Succeeded >= beforeRecovery + 3);
            AssertHealthy(ledger, expectedCancellations: 1);
            Assert.Equal(
                0,
                first.Count(SchedulingEvents.RunTimedOut)
                    + second.Count(SchedulingEvents.RunTimedOut)
            );
        }
        finally
        {
            proxy.SetEnabled(true);
            ledger.Dump();
        }
    }

    private static Task UntilAsync(Func<bool> condition) =>
        CacheConformance.WaitUntilAsync(() => Task.FromResult(condition()), 30);

    private static void AssertHealthy(Ledger ledger, int expectedCancellations = 0)
    {
        Assert.True(ledger.MaxOverlap == 0, ledger.Describe());
        Assert.DoesNotContain(ledger.Runs, run => run.Outcome == "failed");
        Assert.Equal(expectedCancellations, ledger.Runs.Count(run => run.Outcome == "canceled"));
        Assert.DoesNotContain(
            ledger.Events,
            entry =>
                entry.Event == SchedulingEvents.LoopFaulted
                || entry.Event == SchedulingEvents.RunFailed
        );
    }

    private sealed record RunRecord(
        string Instance,
        long Sequence,
        string StartPhase,
        string EndPhase,
        string Outcome,
        Exception? Error,
        bool TokenCanceled
    );

    private sealed record EventRecord(
        string Instance,
        string Phase,
        EventId Event,
        string Message,
        Exception? Error
    );

    private sealed class Ledger(bool holdFirst = false)
    {
        private readonly Lock _gate = new();
        private readonly List<RunRecord> _runs = [];
        private readonly List<EventRecord> _events = [];
        private readonly TaskCompletionSource _held = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private bool _hold = holdFirst;
        private int _inFlight;
        private int _maxOverlap;
        private string _phase = "baseline";
        public Task Held => _held.Task;
        public string Phase
        {
            get
            {
                lock (_gate)
                    return _phase;
            }
            set
            {
                lock (_gate)
                    _phase = value;
            }
        }
        public RunRecord[] Runs
        {
            get
            {
                lock (_gate)
                    return [.. _runs];
            }
        }
        public EventRecord[] Events
        {
            get
            {
                lock (_gate)
                    return [.. _events];
            }
        }
        public int Succeeded => Runs.Count(run => run.Outcome == "succeeded");
        public int MaxOverlap
        {
            get
            {
                lock (_gate)
                    return _maxOverlap;
            }
        }

        public async ValueTask RunAsync(string instance, ScheduledRun run, CancellationToken token)
        {
            string phase;
            bool hold;
            lock (_gate)
            {
                phase = _phase;
                hold = _hold;
                _hold = false;
                _inFlight++;
                _maxOverlap = Math.Max(_maxOverlap, _inFlight - 1);
            }
            var outcome = "succeeded";
            Exception? error = null;
            try
            {
                if (hold)
                {
                    _held.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                }
                else
                {
                    // Workload duration, deliberately long enough to expose overlapping claims.
                    await Task.Delay(100, token);
                }
            }
            catch (OperationCanceledException exception) when (token.IsCancellationRequested)
            {
                outcome = "canceled";
                error = exception;
                throw;
            }
            catch (Exception exception)
            {
                outcome = "failed";
                error = exception;
                throw;
            }
            finally
            {
                lock (_gate)
                {
                    _runs.Add(
                        new RunRecord(
                            instance,
                            run.Sequence,
                            phase,
                            _phase,
                            outcome,
                            error,
                            token.IsCancellationRequested
                        )
                    );
                    _inFlight--;
                }
            }
        }

        public void Record(string instance, EventId id, string message, Exception? error)
        {
            lock (_gate)
                _events.Add(new EventRecord(instance, _phase, id, message, error));
        }

        public string Describe() =>
            string.Join(
                Environment.NewLine,
                Runs.Select(run => run.ToString()).Concat(Events.Select(entry => entry.ToString()))
            );

        public void Dump() => TestContext.Current.TestOutputHelper!.WriteLine(Describe());
    }

    private sealed class JournalLogger(Ledger ledger, string instance) : ILogger<Scheduler>
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        ) => ledger.Record(instance, eventId, formatter(state, exception), exception);
    }

    private sealed class ClaimGate(IScheduleGuard inner) : IScheduleGuard
    {
        private readonly Lock _gate = new();
        private TaskCompletionSource? _release;
        private readonly TaskCompletionSource _blocked = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        public bool IsCoordinated => inner.IsCoordinated;
        public Task Blocked => _blocked.Task;

        public void Pause()
        {
            lock (_gate)
                _release = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }

        public void Resume()
        {
            TaskCompletionSource? release;
            lock (_gate)
            {
                release = _release;
                _release = null;
            }
            release?.TrySetResult();
        }

        public async ValueTask<IScheduleClaim?> TryClaimAsync(
            string schedule,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        )
        {
            Task? wait;
            lock (_gate)
                wait = _release?.Task;
            if (wait is not null)
            {
                _blocked.TrySetResult();
                await wait.WaitAsync(cancellationToken);
            }
            return await inner.TryClaimAsync(schedule, lease, cancellationToken);
        }
    }

    private sealed class Instance(
        RedisConnection connection,
        RedisLockProvider provider,
        DistributedLock locks,
        Scheduler scheduler,
        ClaimGate guard,
        Ledger ledger,
        string name
    ) : IAsyncDisposable
    {
        public Scheduler Scheduler { get; } = scheduler;
        public ClaimGate Guard { get; } = guard;

        public int Count(EventId id) =>
            ledger.Events.Count(entry => entry.Instance == name && entry.Event == id);

        public static async Task<Instance> StartAsync(
            string name,
            string ns,
            string configuration,
            Ledger ledger,
            CancellationToken token
        )
        {
#pragma warning disable CA2000 // Ownership transfers to Instance and Scheduler.
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
            var guard = new ClaimGate(new DistributedLockScheduleGuard(locks));
            var scheduler = new Scheduler(
                new SchedulingOptions { DefaultLease = TimeSpan.FromSeconds(1) },
                [
                    new ScheduleDefinition(
                        "catalog:rebuild",
                        ScheduleTrigger.FixedRate(Period),
                        (run, cancellation) => ledger.RunAsync(name, run, cancellation),
                        new ScheduleOptions { Exclusive = true, Timeout = TimeSpan.FromSeconds(5) }
                    ),
                ],
                guard,
                logger: new JournalLogger(ledger, name)
            );
            var instance = new Instance(
                connection,
                provider,
                locks,
                scheduler,
                guard,
                ledger,
                name
            );
#pragma warning restore CA2000
            try
            {
                await scheduler.StartAsync(token);
                return instance;
            }
            catch
            {
                await instance.DisposeAsync();
                throw;
            }
        }

        public async ValueTask DisposeAsync()
        {
            Guard.Resume();
            await Scheduler.DisposeAsync();
            await locks.DisposeAsync();
            await provider.DisposeAsync();
            await connection.DisposeAsync();
        }
    }
}
