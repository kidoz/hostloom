using HostLoom.Leadership;
using HostLoom.Leadership.DependencyInjection;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Scheduling;
using HostLoom.Scheduling.DependencyInjection;
using HostLoom.Scheduling.Leadership;
using HostLoom.Scheduling.Locking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Two electors over one disabled lock stand in for two replicas deployed with
/// <c>Locking:Enabled = false</c>: the lock grants every instance a placeholder lease, so the
/// election has to say so instead of letting both lead as if they had won.
/// </summary>
public sealed class UncoordinatedLeadershipTests
{
    private static readonly TimeSpan Renew = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task A_disabled_lock_and_its_placeholder_lease_report_no_coordination()
    {
        await using var disabled = Disabled();
        await using var coordinated = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            new InMemoryLockProvider()
        );

        Assert.False(disabled.IsCoordinated);
        Assert.True(coordinated.IsCoordinated);
        await using var placeholder = await disabled.TryAcquireAsync(
            "catalog:eu",
            cancellationToken: TestContext.Current.CancellationToken
        );
        await using var lease = await coordinated.TryAcquireAsync(
            "catalog:eu",
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.NotNull(placeholder);
        Assert.True(placeholder.IsHeld);
        Assert.False(placeholder.IsCoordinated);
        Assert.NotNull(lease);
        Assert.True(lease.IsCoordinated);
    }

    [Fact]
    public async Task By_default_nobody_leads_over_a_disabled_lock_and_every_gate_says_why()
    {
        var clock = new TestClock();
        await using var locks = Disabled();
        var logger = new RecordingLogger<LeaderElector>();
        await using var first = Elector(locks, clock, logger);
        await using var second = Elector(locks, clock);
        var changes = new List<LeadershipChange>();
        using var _ = first.OnChange(changes.Add);
        Assert.Equal(UncoordinatedLeadership.Follow, new LeadershipOptions().WhenUncoordinated);
        Assert.False(first.IsCoordinated);

        await first.StartAsync(TestContext.Current.CancellationToken);
        await second.StartAsync(TestContext.Current.CancellationToken);
        // Each candidate refuses its placeholder and arms one retry timer; three rounds prove
        // it keeps doing so.
        for (var round = 0; round < 3; round++)
        {
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 2);
            clock.Advance(Retry);
        }

        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 2);

        Assert.False(first.IsLeader);
        Assert.False(second.IsLeader);
        Assert.Equal(LeadershipStatus.Candidate, first.Status);
        Assert.Equal(0, first.Term);
        Assert.True(first.LeadershipToken.IsCancellationRequested);
        Assert.Empty(changes);
        var warning = Assert.Single(
            logger.Entries,
            entry => entry.Event.Id == LeadershipEvents.Uncoordinated.Id
        );
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Contains("'scheduler'", warning.Message, StringComparison.Ordinal);
        Assert.Contains("Follow", warning.Message, StringComparison.Ordinal);

        var probe = LeadershipProbe.Describe(first);
        Assert.False(probe.Coordinated);
        Assert.Equal(LeadershipStatus.Candidate, probe.Status);
        Assert.Contains(
            probe.Lines,
            line => line.Contains("Leadership:WhenUncoordinated = Follow", StringComparison.Ordinal)
        );

        var guard = new LeaderScheduleGuard(first);
        Assert.False(guard.IsCoordinated);
        Assert.Null(
            await guard.TryClaimAsync(
                "catalog:rebuild",
                TimeSpan.FromMinutes(1),
                TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task Lead_makes_every_instance_lead_without_coordination_and_says_so()
    {
        var clock = new TestClock();
        await using var locks = Disabled();
        var logger = new RecordingLogger<LeaderElector>();
        await using var first = Elector(locks, clock, logger, UncoordinatedLeadership.Lead);
        await using var second = Elector(locks, clock, when: UncoordinatedLeadership.Lead);

        await first.StartAsync(TestContext.Current.CancellationToken);
        await second.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => first.IsLeader && second.IsLeader);

        Assert.Equal(1, first.Term);
        Assert.Equal(1, second.Term);
        Assert.False(first.IsCoordinated);
        Assert.False(first.LeadershipToken.IsCancellationRequested);
        var warning = Assert.Single(
            logger.Entries,
            entry => entry.Event.Id == LeadershipEvents.Uncoordinated.Id
        );
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Contains("Lead", warning.Message, StringComparison.Ordinal);
        var probe = LeadershipProbe.Describe(first);
        Assert.False(probe.Coordinated);
        Assert.Equal(LeadershipStatus.Leader, probe.Status);
        Assert.Contains(
            probe.Lines,
            line => line.Contains("Leadership:WhenUncoordinated = Lead", StringComparison.Ordinal)
        );

        var guard = new LeaderScheduleGuard(first);
        Assert.False(guard.IsCoordinated);
        var claim = await guard.TryClaimAsync(
            "catalog:rebuild",
            TimeSpan.FromMinutes(1),
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(claim);
        Assert.True(claim.IsHeld);

        // A placeholder renews for ever: two renewal periods later both still lead in term one,
        // and the warning was not repeated.
        for (var round = 0; round < 2; round++)
        {
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 2);
            clock.Advance(Renew);
        }

        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 2);
        Assert.True(first.IsLeader);
        Assert.True(second.IsLeader);
        Assert.Equal(1, first.Term);
        Assert.Single(logger.Entries, entry => entry.Event.Id == LeadershipEvents.Uncoordinated.Id);

        await first.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(LeadershipStatus.Stopped, first.Status);
        Assert.True(claim.LostToken.IsCancellationRequested);
    }

    [Theory]
    [InlineData(UncoordinatedLeadership.Follow, false)]
    [InlineData(UncoordinatedLeadership.Lead, true)]
    public async Task A_leader_guarded_schedule_over_a_disabled_lock_follows_the_configured_posture(
        UncoordinatedLeadership when,
        bool runs
    )
    {
        var clock = new TestClock();
        var ran = 0;
        using var host = new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton<TimeProvider>(clock);
                services.AddHostLoomLocking(locking =>
                {
                    locking.Namespace = "catalog";
                    locking.Enabled = false;
                });
                services
                    .AddHostLoomLeadership()
                    .AddRole(
                        "scheduler",
                        leadership =>
                        {
                            leadership.RetryInterval = Retry;
                            leadership.RetryJitter = TimeSpan.Zero;
                            leadership.WhenUncoordinated = when;
                        }
                    );
                services
                    .AddHostLoomScheduling()
                    .UseLeader("scheduler")
                    .AddSchedule(
                        "catalog:rebuild",
                        ScheduleTrigger.FixedRate(
                            TimeSpan.FromSeconds(10),
                            TimeSpan.FromSeconds(1)
                        ),
                        (_, _, _) =>
                        {
                            Interlocked.Increment(ref ran);
                            return ValueTask.CompletedTask;
                        },
                        options => options.Exclusive = true
                    );
            })
            .Build();

        await host.StartAsync(TestContext.Current.CancellationToken);
        var elector = host.Services.GetRequiredKeyedService<LeaderElector>("scheduler");
        var scheduler = host.Services.GetRequiredService<Scheduler>();
        Assert.False(elector.IsCoordinated);
        if (runs)
        {
            await SchedulingTests.WaitUntilAsync(() => elector.IsLeader);
        }

        await SchedulingTests.WaitUntilAsync(() =>
            scheduler.GetState("catalog:rebuild").NextDue is not null
        );
        clock.Advance(TimeSpan.FromSeconds(1));
        await SchedulingTests.WaitUntilAsync(() => scheduler.GetState("catalog:rebuild").Runs == 1);
        clock.Advance(TimeSpan.FromSeconds(10));
        await SchedulingTests.WaitUntilAsync(() =>
            scheduler.GetState("catalog:rebuild") is { Runs: 2, Running: false }
        );

        Assert.Equal(runs ? 2 : 0, Volatile.Read(ref ran));
        Assert.Equal(
            runs ? ScheduleRunOutcome.Succeeded : ScheduleRunOutcome.Skipped,
            scheduler.GetState("catalog:rebuild").LastOutcome
        );
        Assert.False(SchedulingProbe.Describe(scheduler).GuardCoordinated);
        Assert.Equal(runs, elector.IsLeader);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_scheduler_over_an_uncoordinated_lock_guard_warns_once_and_the_probe_reports_it()
    {
        var clock = new TestClock();
        await using var locks = Disabled();
        var guard = new DistributedLockScheduleGuard(locks);
        var logger = new RecordingLogger<Scheduler>();
        await using var scheduler = new Scheduler(
            new SchedulingOptions(),
            [
                new ScheduleDefinition(
                    "catalog:rebuild",
                    ScheduleTrigger.FixedRate(TimeSpan.FromMinutes(1)),
                    static (_, _) => ValueTask.CompletedTask,
                    new ScheduleOptions { Exclusive = true }
                ),
                new ScheduleDefinition(
                    "rates:refresh",
                    ScheduleTrigger.FixedRate(TimeSpan.FromMinutes(1)),
                    static (_, _) => ValueTask.CompletedTask
                ),
            ],
            guard,
            clock,
            logger
        );

        Assert.False(guard.IsCoordinated);
        var warning = Assert.Single(
            logger.Entries,
            entry => entry.Event.Id == SchedulingEvents.GuardUncoordinated.Id
        );
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Contains("DistributedLockScheduleGuard", warning.Message, StringComparison.Ordinal);
        Assert.Contains("1 exclusive", warning.Message, StringComparison.Ordinal);
        var probe = SchedulingProbe.Describe(scheduler);
        Assert.False(probe.GuardCoordinated);
        Assert.Equal("DistributedLockScheduleGuard", probe.Guard);
        Assert.Contains(
            probe.Lines,
            line =>
                line.Contains("uncoordinated", StringComparison.Ordinal)
                && line.Contains("Locking:Enabled = false", StringComparison.Ordinal)
        );

        // The lock's single-instance mode still grants the claim; what changed is that the
        // grant is no longer silent.
        var claim = await guard.TryClaimAsync(
            "catalog:rebuild",
            TimeSpan.FromMinutes(1),
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(claim);
        Assert.True(claim.IsHeld);
        Assert.False(claim.LostToken.CanBeCanceled);
        await claim.DisposeAsync();

        // Over a coordinated lock nothing is said and the probe reports coordination.
        var quiet = new RecordingLogger<Scheduler>();
        await using var coordinatedLock = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            new InMemoryLockProvider(clock),
            clock
        );
        await using var coordinated = new Scheduler(
            new SchedulingOptions(),
            [
                new ScheduleDefinition(
                    "catalog:rebuild",
                    ScheduleTrigger.FixedRate(TimeSpan.FromMinutes(1)),
                    static (_, _) => ValueTask.CompletedTask,
                    new ScheduleOptions { Exclusive = true }
                ),
            ],
            new DistributedLockScheduleGuard(coordinatedLock),
            clock,
            quiet
        );
        Assert.False(quiet.Has(SchedulingEvents.GuardUncoordinated));
        Assert.True(SchedulingProbe.Describe(coordinated).GuardCoordinated);
        Assert.Contains(
            "Guard = DistributedLockScheduleGuard",
            SchedulingProbe.Describe(coordinated).Lines
        );
    }

    private static DistributedLock Disabled() =>
        new(new LockingOptions { Namespace = "catalog", Enabled = false }, provider: null);

    private static LeaderElector Elector(
        IDistributedLock locks,
        TestClock clock,
        ILogger<LeaderElector>? logger = null,
        UncoordinatedLeadership when = UncoordinatedLeadership.Follow
    ) =>
        new(
            "scheduler",
            locks,
            new LeadershipOptions
            {
                Lease = TimeSpan.FromSeconds(15),
                RenewInterval = Renew,
                RetryInterval = Retry,
                RetryJitter = TimeSpan.Zero,
                WhenUncoordinated = when,
            },
            clock,
            logger
        );
}
