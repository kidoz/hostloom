using HostLoom.Scheduling;
using HostLoom.Scheduling.Testing;
using Xunit;

namespace HostLoom.Tests;

public sealed class SchedulingTests
{
    private static readonly TimeSpan TenSeconds = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData("timeout", false)]
    [InlineData("claim", false)]
    [InlineData("stop", false)]
    [InlineData("timeout", true)]
    [InlineData("claim", true)]
    [InlineData("stop", true)]
    [InlineData("timeout_claim", false)]
    [InlineData("timeout_claim", true)]
    [InlineData("stop_timeout_claim", false)]
    [InlineData("stop_timeout_claim", true)]
    public async Task Cooperative_returns_preserve_cancellation_outcomes(
        string reason,
        bool throwCancellation
    )
    {
        var clock = new TestClock();
        var guard = new ManualScheduleGuard();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        await using var scheduler = new Scheduler(
            new(),
            [
                new ScheduleDefinition(
                    "catalog",
                    ScheduleTrigger.FixedRate(TenSeconds),
                    async (_, token) =>
                    {
                        using var registration = token.Register(() => cancelled.TrySetResult());
                        entered.TrySetResult();
                        await cancelled.Task;
                        await release.Task;
                        if (throwCancellation)
                            token.ThrowIfCancellationRequested();
                    },
                    new ScheduleOptions { Exclusive = true, Timeout = TimeSpan.FromSeconds(1) }
                ),
            ],
            guard,
            clock
        );
        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await entered.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken
        );
        Task? stop = null;
        try
        {
            if (reason.Contains("claim", StringComparison.Ordinal))
                Assert.True(guard.Lose("catalog"));
            if (reason.Contains("timeout", StringComparison.Ordinal))
                clock.Advance(TimeSpan.FromSeconds(1));
            if (reason.StartsWith("stop", StringComparison.Ordinal))
                stop = scheduler.StopAsync(TestContext.Current.CancellationToken);
        }
        finally
        {
            release.TrySetResult();
        }
        if (stop is not null)
            await stop;
        await WaitUntilAsync(() => !scheduler.GetState("catalog").Running);
        Assert.Equal(
            reason switch
            {
                "timeout" or "timeout_claim" => ScheduleRunOutcome.TimedOut,
                "claim" => ScheduleRunOutcome.ClaimLost,
                _ => ScheduleRunOutcome.Canceled,
            },
            scheduler.GetState("catalog").LastOutcome
        );
    }

    [Fact]
    public async Task A_fixed_delay_schedule_runs_after_the_delay_counted_from_completion()
    {
        var clock = new TestClock();
        var probe = new JobProbe();
        await using var scheduler = new Scheduler(
            new SchedulingOptions(),
            [new ScheduleDefinition("sync", ScheduleTrigger.FixedDelay(TenSeconds), probe.Run)],
            timeProvider: clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.Equal(clock.GetUtcNow() + TenSeconds, scheduler.GetState("sync").NextDue);

        clock.Advance(TimeSpan.FromSeconds(9));
        Assert.Equal(0, probe.Started);
        clock.Advance(TimeSpan.FromSeconds(1));
        var first = await probe.NextRunAsync();

        Assert.Equal(1, first.Sequence);
        Assert.Equal(DateTimeOffset.UnixEpoch + TenSeconds, first.DueAt);
        Assert.Equal(TimeSpan.Zero, first.Lag);
        await WaitUntilAsync(() => clock.PendingTimers == 1);
        var state = scheduler.GetState("sync");
        Assert.Equal(ScheduleRunOutcome.Succeeded, state.LastOutcome);
        Assert.Equal(1, state.Runs);
        Assert.Equal(first.StartedAt + TenSeconds, state.NextDue);

        clock.Advance(TenSeconds);
        var second = await probe.NextRunAsync();
        Assert.Equal(2, second.Sequence);
    }

    [Fact]
    public async Task A_fixed_rate_schedule_counts_from_the_due_time_and_runs_a_late_one_at_once()
    {
        var clock = new TestClock();
        var probe = new JobProbe();
        var slowFirstRun = true;
        await using var scheduler = new Scheduler(
            new SchedulingOptions(),
            [
                new ScheduleDefinition(
                    "tick",
                    ScheduleTrigger.FixedRate(TenSeconds),
                    (run, token) =>
                    {
                        if (slowFirstRun)
                        {
                            slowFirstRun = false;
                            clock.Advance(TimeSpan.FromSeconds(15));
                        }

                        return probe.Run(run, token);
                    }
                ),
            ],
            timeProvider: clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        var first = await probe.NextRunAsync();
        Assert.Equal(DateTimeOffset.UnixEpoch, first.DueAt);

        // The second period was already due when the first run ended, so it starts at once.
        var second = await probe.NextRunAsync();
        Assert.Equal(DateTimeOffset.UnixEpoch.AddSeconds(15), second.DueAt);
        Assert.Equal(TimeSpan.Zero, second.Lag);

        await WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.Equal(DateTimeOffset.UnixEpoch.AddSeconds(25), scheduler.GetState("tick").NextDue);
        clock.Advance(TenSeconds);
        var third = await probe.NextRunAsync();
        Assert.Equal(3, third.Sequence);
    }

    [Fact]
    public async Task A_cron_schedule_runs_at_the_next_occurrence()
    {
        var clock = new TestClock();
        var probe = new JobProbe();
        await using var scheduler = new Scheduler(
            new SchedulingOptions(),
            [new ScheduleDefinition("hourly", ScheduleTrigger.Cron("0 0 * * * *"), probe.Run)],
            timeProvider: clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(TimeSpan.FromHours(1));
        var run = await probe.NextRunAsync();

        Assert.Equal(DateTimeOffset.UnixEpoch.AddHours(1), run.DueAt);
        Assert.Equal("hourly", run.Schedule);
    }

    [Fact]
    public async Task A_failing_run_is_recorded_and_the_schedule_continues()
    {
        var clock = new TestClock();
        var probe = new JobProbe();
        var attempts = 0;
        await using var scheduler = new Scheduler(
            new SchedulingOptions(),
            [
                new ScheduleDefinition(
                    "flaky",
                    ScheduleTrigger.FixedRate(TenSeconds),
                    async (run, token) =>
                    {
                        await probe.Run(run, token);
                        if (++attempts == 1)
                        {
                            throw new InvalidOperationException("first run fails");
                        }
                    }
                ),
            ],
            timeProvider: clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        _ = await probe.NextRunAsync();
        await WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.Equal(ScheduleRunOutcome.Failed, scheduler.GetState("flaky").LastOutcome);

        clock.Advance(TenSeconds);
        _ = await probe.NextRunAsync();
        await WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.Equal(ScheduleRunOutcome.Succeeded, scheduler.GetState("flaky").LastOutcome);
        Assert.Equal(2, scheduler.GetState("flaky").Runs);
    }

    [Fact]
    public async Task A_run_past_its_timeout_is_cancelled_and_reported_as_timed_out()
    {
        var clock = new TestClock();
        var probe = new JobProbe();
        await using var scheduler = new Scheduler(
            new SchedulingOptions(),
            [
                new ScheduleDefinition(
                    "slow",
                    ScheduleTrigger.FixedRate(TimeSpan.FromMinutes(1)),
                    async (run, token) =>
                    {
                        await probe.Run(run, token);
                        await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    },
                    new ScheduleOptions { Timeout = TimeSpan.FromSeconds(5) }
                ),
            ],
            timeProvider: clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        _ = await probe.NextRunAsync();
        Assert.True(scheduler.GetState("slow").Running);
        await WaitUntilAsync(() => clock.PendingTimers == 1);

        clock.Advance(TimeSpan.FromSeconds(5));
        await WaitUntilAsync(() => !scheduler.GetState("slow").Running);

        Assert.Equal(ScheduleRunOutcome.TimedOut, scheduler.GetState("slow").LastOutcome);
    }

    [Fact]
    public async Task An_exclusive_schedule_is_skipped_while_another_instance_holds_the_claim()
    {
        var clock = new TestClock();
        var guard = new ManualScheduleGuard();
        var probe = new JobProbe();
        Assert.True(guard.Hold("nightly"));
        await using var scheduler = new Scheduler(
            new SchedulingOptions { DefaultLease = TimeSpan.FromMinutes(2) },
            [
                new ScheduleDefinition(
                    "nightly",
                    ScheduleTrigger.FixedRate(TenSeconds),
                    probe.Run,
                    new ScheduleOptions { Exclusive = true }
                ),
            ],
            guard,
            clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await WaitUntilAsync(() => scheduler.GetState("nightly").Runs == 1);
        await WaitUntilAsync(() => clock.PendingTimers == 1);

        Assert.Equal(0, probe.Started);
        Assert.Equal(ScheduleRunOutcome.Skipped, scheduler.GetState("nightly").LastOutcome);
        var refused = Assert.Single(guard.Claims);
        Assert.Equal(
            ("nightly", TimeSpan.FromMinutes(2), false),
            (refused.Schedule, refused.Lease, refused.Granted)
        );

        Assert.True(guard.Release("nightly"));
        clock.Advance(TenSeconds);
        var run = await probe.NextRunAsync();
        await WaitUntilAsync(() => clock.PendingTimers == 1);

        Assert.Equal(2, run.Sequence);
        var state = scheduler.GetState("nightly");
        Assert.Equal(ScheduleRunOutcome.Succeeded, state.LastOutcome);
        Assert.True(guard.Claims[^1].Granted);
        // The claim outlives the run: another instance reaching this occurrence is refused until
        // the lease ends or this one's next occurrence is due, whichever comes first.
        Assert.Contains("nightly", guard.Active);
        Assert.Equal(run.StartedAt + TimeSpan.FromMinutes(2), state.ClaimHeldUntil);
        clock.Advance(TenSeconds);
        _ = await probe.NextRunAsync();
        Assert.Equal(3, guard.Claims.Count);
    }

    [Fact]
    public async Task A_guard_failure_skips_the_run_instead_of_running_unguarded()
    {
        var clock = new TestClock();
        var guard = new ManualScheduleGuard();
        var probe = new JobProbe();
        guard.FailNext(new InvalidOperationException("backend unreachable"));
        await using var scheduler = new Scheduler(
            new SchedulingOptions(),
            [
                new ScheduleDefinition(
                    "nightly",
                    ScheduleTrigger.FixedRate(TenSeconds),
                    probe.Run,
                    new ScheduleOptions { Exclusive = true }
                ),
            ],
            guard,
            clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await WaitUntilAsync(() => scheduler.GetState("nightly").Runs == 1);
        await WaitUntilAsync(() => clock.PendingTimers == 1);

        Assert.Equal(0, probe.Started);
        Assert.Equal(ScheduleRunOutcome.GuardFailed, scheduler.GetState("nightly").LastOutcome);

        clock.Advance(TenSeconds);
        _ = await probe.NextRunAsync();
    }

    [Fact]
    public async Task A_lost_claim_cancels_the_run_and_is_reported()
    {
        var clock = new TestClock();
        var guard = new ManualScheduleGuard();
        var probe = new JobProbe();
        await using var scheduler = new Scheduler(
            new SchedulingOptions(),
            [
                new ScheduleDefinition(
                    "nightly",
                    ScheduleTrigger.FixedRate(TenSeconds),
                    async (run, token) =>
                    {
                        await probe.Run(run, token);
                        await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    },
                    new ScheduleOptions { Exclusive = true }
                ),
            ],
            guard,
            clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        _ = await probe.NextRunAsync();
        Assert.Contains("nightly", guard.Active);

        Assert.True(guard.Lose("nightly"));
        await WaitUntilAsync(() => !scheduler.GetState("nightly").Running);

        Assert.Equal(ScheduleRunOutcome.ClaimLost, scheduler.GetState("nightly").LastOutcome);
        Assert.Empty(guard.Active);
    }

    [Fact]
    public async Task Stopping_cancels_a_running_job_and_ends_the_loop()
    {
        var clock = new TestClock();
        var probe = new JobProbe();
        var scheduler = new Scheduler(
            new SchedulingOptions(),
            [
                new ScheduleDefinition(
                    "long",
                    ScheduleTrigger.FixedRate(TenSeconds),
                    async (run, token) =>
                    {
                        await probe.Run(run, token);
                        await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    }
                ),
            ],
            timeProvider: clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        _ = await probe.NextRunAsync();

        await scheduler.StopAsync(TestContext.Current.CancellationToken);

        var state = scheduler.GetState("long");
        Assert.Equal(ScheduleRunOutcome.Canceled, state.LastOutcome);
        Assert.False(state.Running);
        Assert.Null(state.NextDue);
        await scheduler.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            scheduler.StartAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task A_disabled_scheduler_runs_nothing_and_says_so()
    {
        var clock = new TestClock();
        var probe = new JobProbe();
        await using var scheduler = new Scheduler(
            new SchedulingOptions { Enabled = false },
            [new ScheduleDefinition("sync", ScheduleTrigger.FixedRate(TenSeconds), probe.Run)],
            timeProvider: clock
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await Task.Delay(50, TestContext.Current.CancellationToken);

        Assert.Equal(0, clock.PendingTimers);
        Assert.Equal(0, probe.Started);
        var description = SchedulingProbe.Describe(scheduler);
        Assert.False(description.Enabled);
        Assert.Contains(
            description.Lines,
            line => line.Contains("(disabled)", StringComparison.Ordinal)
        );
    }

    [Fact]
    public void Composition_rejects_duplicate_names_and_an_exclusive_schedule_without_a_guard()
    {
        var trigger = ScheduleTrigger.FixedRate(TenSeconds);
        static ValueTask Nothing(ScheduledRun run, CancellationToken token) =>
            ValueTask.CompletedTask;

        var duplicate = Assert.Throws<ArgumentException>(() =>
            new Scheduler(
                new SchedulingOptions(),
                [
                    new ScheduleDefinition("a", trigger, Nothing),
                    new ScheduleDefinition("a", trigger, Nothing),
                ]
            )
        );
        Assert.Contains(
            "'a' is defined more than once",
            duplicate.Message,
            StringComparison.Ordinal
        );

        var unguarded = Assert.Throws<ArgumentException>(() =>
            new Scheduler(
                new SchedulingOptions(),
                [
                    new ScheduleDefinition(
                        "a",
                        trigger,
                        Nothing,
                        new ScheduleOptions { Exclusive = true }
                    ),
                ]
            )
        );
        Assert.Contains("IScheduleGuard", unguarded.Message, StringComparison.Ordinal);

        var options = Assert.Throws<ArgumentException>(() =>
            new Scheduler(new SchedulingOptions { DefaultLease = TimeSpan.Zero }, [])
        );
        Assert.Contains("Scheduling:DefaultLease", options.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Definitions_and_triggers_validate_their_arguments()
    {
        static ValueTask Nothing(ScheduledRun run, CancellationToken token) =>
            ValueTask.CompletedTask;
        var trigger = ScheduleTrigger.FixedRate(TenSeconds);

        Assert.Throws<ArgumentException>(() =>
            new ScheduleDefinition("has space", trigger, Nothing)
        );
        Assert.Throws<ArgumentException>(() =>
            new ScheduleDefinition(new string('x', 129), trigger, Nothing)
        );
        var bad = Assert.Throws<ArgumentException>(() =>
            new ScheduleDefinition(
                "a",
                trigger,
                Nothing,
                new ScheduleOptions { Timeout = TimeSpan.Zero, Lease = TimeSpan.FromSeconds(-1) }
            )
        );
        Assert.Contains("Timeout must be positive", bad.Message, StringComparison.Ordinal);
        Assert.Contains("Lease must be positive", bad.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentOutOfRangeException>(() => ScheduleTrigger.FixedRate(TimeSpan.Zero));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            ScheduleTrigger.FixedDelay(TenSeconds, TimeSpan.FromSeconds(-1))
        );
        Assert.Throws<FormatException>(() => ScheduleTrigger.Cron("* * *"));
        Assert.Equal("fixed rate 00:00:10, initial delay 00:00:00", trigger.Description);
        Assert.Equal(
            "fixed delay 00:00:10, initial delay 00:00:10",
            ScheduleTrigger.FixedDelay(TenSeconds).Description
        );
        Assert.Equal("cron '0 0 * * * *' (UTC)", ScheduleTrigger.Cron("0 0 * * * *").Description);
    }

    [Fact]
    public async Task The_probe_describes_every_schedule_without_running_anything()
    {
        var clock = new TestClock();
        var guard = new ManualScheduleGuard();
        var probe = new JobProbe();
        await using var scheduler = new Scheduler(
            new SchedulingOptions(),
            [
                new ScheduleDefinition("hourly", ScheduleTrigger.Cron("0 0 * * * *"), probe.Run),
                new ScheduleDefinition(
                    "nightly",
                    ScheduleTrigger.FixedDelay(TenSeconds),
                    probe.Run,
                    new ScheduleOptions { Exclusive = true, Timeout = TimeSpan.FromMinutes(1) }
                ),
            ],
            guard,
            clock
        );

        var before = SchedulingProbe.Describe(scheduler);
        Assert.Equal(nameof(ManualScheduleGuard), before.Guard);
        Assert.Equal(2, before.Schedules.Count);
        Assert.Contains(
            before.Lines,
            line => line.Contains("(not scheduled)", StringComparison.Ordinal)
        );

        await scheduler.StartAsync(TestContext.Current.CancellationToken);
        await WaitUntilAsync(() => clock.PendingTimers == 2);
        var after = SchedulingProbe.Describe(scheduler);

        Assert.Equal(0, probe.Started);
        var nightly = after.Schedules.Single(s => s.Name == "nightly");
        Assert.True(nightly.Exclusive);
        Assert.Equal(TimeSpan.FromMinutes(5), nightly.Lease);
        Assert.Equal(TimeSpan.FromMinutes(1), nightly.Timeout);
        Assert.Equal(DateTimeOffset.UnixEpoch + TenSeconds, nightly.NextDue);
        Assert.Null(nightly.LastOutcome);
        Assert.Contains(
            after.Lines,
            line =>
                line.StartsWith(
                    "hourly: cron '0 0 * * * *' (UTC); exclusive = False; timeout = none; next due = 1970-01-01T01:00:00",
                    StringComparison.Ordinal
                )
        );
        Assert.Contains("Scheduling:Enabled = true", after.Lines);
        Assert.Contains("Guard = ManualScheduleGuard", after.Lines);
    }

    internal static async Task WaitUntilAsync(Func<bool> condition)
    {
        var deadline = DateTime.UtcNow + TimeSpan.FromSeconds(10);
        while (!condition())
        {
            if (DateTime.UtcNow > deadline)
            {
                throw new TimeoutException("The condition was not met within 10 seconds.");
            }

            await Task.Delay(5, TestContext.Current.CancellationToken);
        }
    }

    /// <summary>Records each run and lets a test await the next one.</summary>
    internal sealed class JobProbe
    {
        private readonly SemaphoreSlim _signal = new(0);
        private readonly Queue<ScheduledRun> _runs = new();
        private readonly Lock _gate = new();

        public int Started
        {
            get
            {
                lock (_gate)
                {
                    return _runs.Count + _taken;
                }
            }
        }

        private int _taken;

        public ValueTask Run(ScheduledRun run, CancellationToken cancellationToken)
        {
            lock (_gate)
            {
                _runs.Enqueue(run);
            }

            _signal.Release();
            return ValueTask.CompletedTask;
        }

        public async Task<ScheduledRun> NextRunAsync()
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                timeout.Token,
                TestContext.Current.CancellationToken
            );
            await _signal.WaitAsync(linked.Token);
            lock (_gate)
            {
                _taken++;
                return _runs.Dequeue();
            }
        }
    }
}
