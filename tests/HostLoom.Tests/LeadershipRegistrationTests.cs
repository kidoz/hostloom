using HostLoom.Leadership;
using HostLoom.Leadership.DependencyInjection;
using HostLoom.Leadership.Testing;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Scheduling;
using HostLoom.Scheduling.DependencyInjection;
using HostLoom.Scheduling.Leadership;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.Tests;

public sealed class LeadershipRegistrationTests
{
    [Fact]
    public async Task AddRole_registers_a_keyed_elector_and_the_single_role_resolves_unkeyed()
    {
        var clock = new TestClock();
        var services = new ServiceCollection();
        services.AddSingleton<TimeProvider>(clock);
        services.AddHostLoomLocking(locking => locking.Namespace = "billing").UseInMemory();
        services
            .AddHostLoomLeadership()
            .AddRole("scheduler", leadership => leadership.Lease = TimeSpan.FromSeconds(20));
        await using var provider = services.BuildServiceProvider();

        var keyed = provider.GetRequiredKeyedService<ILeadership>("scheduler");
        var elector = provider.GetRequiredKeyedService<LeaderElector>("scheduler");

        Assert.Same(keyed, elector);
        Assert.Same(keyed, provider.GetRequiredService<ILeadership>());
        Assert.Equal(TimeSpan.FromSeconds(20), elector.Options.Lease);
        Assert.Equal(LeadershipStatus.Idle, elector.Status);
    }

    [Fact]
    public async Task Several_roles_need_a_key_and_a_repeated_role_is_refused()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking => locking.Namespace = "billing").UseInMemory();
        var builder = services.AddHostLoomLeadership().AddRole("scheduler").AddRole("reconciler");
        await using var provider = services.BuildServiceProvider();

        var failure = Assert.Throws<InvalidOperationException>(() =>
            provider.GetRequiredService<ILeadership>()
        );
        Assert.Contains("scheduler", failure.Message, StringComparison.Ordinal);
        Assert.Contains("reconciler", failure.Message, StringComparison.Ordinal);
        Assert.NotSame(
            provider.GetRequiredKeyedService<ILeadership>("scheduler"),
            provider.GetRequiredKeyedService<ILeadership>("reconciler")
        );
        var repeated = Assert.Throws<InvalidOperationException>(() => builder.AddRole("scheduler"));
        Assert.Contains("'scheduler'", repeated.Message, StringComparison.Ordinal);
        Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomLeadership().AddRole("reconciler")
        );
    }

    [Fact]
    public async Task Options_are_validated_per_role_naming_the_role_and_the_key()
    {
        var services = new ServiceCollection();
        services.AddHostLoomLocking(locking => locking.Namespace = "billing").UseInMemory();
        services
            .AddHostLoomLeadership()
            .AddRole("scheduler", leadership => leadership.RenewInterval = leadership.Lease);
        await using var provider = services.BuildServiceProvider();

        var failure = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<IOptionsMonitor<LeadershipOptions>>().Get("scheduler")
        );

        Assert.Contains("Role 'scheduler'", failure.Message, StringComparison.Ordinal);
        Assert.Contains("Leadership:RenewInterval", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task The_hosted_service_starts_electors_with_the_host_and_releases_on_stop()
    {
        var clock = new TestClock();
        var backend = new InMemoryLockProvider(clock);
        using var host = Build(clock, backend);
        var elector = host.Services.GetRequiredKeyedService<LeaderElector>("scheduler");

        await host.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => elector.IsLeader);
        Assert.Equal(1, backend.Count);

        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(LeadershipStatus.Stopped, elector.Status);
        Assert.Equal(0, backend.Count);
    }

    [Fact]
    public async Task UseLeader_runs_an_exclusive_schedule_on_the_leader_only_and_follows_a_hand_over()
    {
        var clock = new TestClock();
        var backend = new InMemoryLockProvider(clock);
        var ran = new List<string>();
        using var first = Build(clock, backend, ("first", ran));
        using var second = Build(clock, backend, ("second", ran));
        await first.StartAsync(TestContext.Current.CancellationToken);
        var firstElector = first.Services.GetRequiredKeyedService<LeaderElector>("scheduler");
        await SchedulingTests.WaitUntilAsync(() => firstElector.IsLeader);
        await second.StartAsync(TestContext.Current.CancellationToken);
        var firstScheduler = first.Services.GetRequiredService<Scheduler>();
        var secondScheduler = second.Services.GetRequiredService<Scheduler>();
        Assert.IsType<LeaderScheduleGuard>(firstScheduler.Guard);
        await SchedulingTests.WaitUntilAsync(() =>
            firstScheduler.GetState("catalog:rebuild").NextDue is not null
            && secondScheduler.GetState("catalog:rebuild").NextDue is not null
        );
        clock.Advance(TimeSpan.FromSeconds(1));

        await SchedulingTests.WaitUntilAsync(() =>
            firstScheduler.GetState("catalog:rebuild").Runs == 1
            && secondScheduler.GetState("catalog:rebuild").Runs == 1
        );
        Assert.Equal(["first"], ran);
        Assert.Equal(
            ScheduleRunOutcome.Succeeded,
            firstScheduler.GetState("catalog:rebuild").LastOutcome
        );
        Assert.Equal(
            ScheduleRunOutcome.Skipped,
            secondScheduler.GetState("catalog:rebuild").LastOutcome
        );

        // The leader stops; the other instance leads and runs the next occurrence.
        await first.StopAsync(TestContext.Current.CancellationToken);
        clock.Advance(TimeSpan.FromSeconds(2));
        var secondElector = second.Services.GetRequiredKeyedService<LeaderElector>("scheduler");
        await SchedulingTests.WaitUntilAsync(() => secondElector.IsLeader);
        clock.Advance(TimeSpan.FromSeconds(8));
        await SchedulingTests.WaitUntilAsync(() =>
            secondScheduler.GetState("catalog:rebuild").Runs == 2
            && !secondScheduler.GetState("catalog:rebuild").Running
        );

        Assert.Equal(["first", "second"], ran);
        Assert.Equal(
            ScheduleRunOutcome.Succeeded,
            secondScheduler.GetState("catalog:rebuild").LastOutcome
        );
        await second.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_manual_leadership_drives_a_leader_only_consumer()
    {
        using var leadership = new ManualLeadership("scheduler");
        var guard = new LeaderScheduleGuard(leadership);
        var changes = new List<LeadershipChangeReason>();
        using var _ = leadership.OnChange(change => changes.Add(change.Reason));

        Assert.Null(
            await guard.TryClaimAsync(
                "catalog:rebuild",
                TimeSpan.FromMinutes(1),
                TestContext.Current.CancellationToken
            )
        );
        var waiting = leadership.WaitForLeadershipAsync(TestContext.Current.CancellationToken);
        leadership.Acquire();
        await waiting;
        var claim = await guard.TryClaimAsync(
            "catalog:rebuild",
            TimeSpan.FromMinutes(1),
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(claim);
        Assert.True(claim.IsHeld);

        leadership.Lose();

        Assert.False(claim.IsHeld);
        Assert.True(claim.LostToken.IsCancellationRequested);
        Assert.Equal(1, leadership.Term);
        Assert.Equal([LeadershipChangeReason.Acquired, LeadershipChangeReason.Lost], changes);
        Assert.Equal("scheduler", guard.Role);
        leadership.Acquire();
        leadership.Resign();
        Assert.Equal(2, leadership.Term);
        Assert.Equal(4, leadership.Changes.Count);
    }

    private static IHost Build(
        TestClock clock,
        InMemoryLockProvider backend,
        (string Name, List<string> Ran)? schedule = null
    ) =>
        new HostBuilder()
            .ConfigureServices(services =>
            {
                services.AddSingleton<TimeProvider>(clock);
                services.AddSingleton(backend);
                services
                    .AddHostLoomLocking(locking =>
                    {
                        locking.Namespace = "billing";
                        locking.DefaultLease = TimeSpan.FromSeconds(15);
                    })
                    .UseProvider<InMemoryLockProvider>("Shared");
                services
                    .AddHostLoomLeadership()
                    .AddRole(
                        "scheduler",
                        leadership =>
                        {
                            leadership.Lease = TimeSpan.FromSeconds(15);
                            leadership.RenewInterval = TimeSpan.FromSeconds(5);
                            leadership.RetryInterval = TimeSpan.FromSeconds(2);
                            leadership.RetryJitter = TimeSpan.Zero;
                        }
                    );
                if (schedule is { } s)
                {
                    services
                        .AddHostLoomScheduling()
                        .UseLeader("scheduler")
                        .AddSchedule(
                            "catalog:rebuild",
                            // One second of initial delay: the first occurrence must not race the
                            // elector's first acquisition on the thread pool.
                            ScheduleTrigger.FixedRate(
                                TimeSpan.FromSeconds(10),
                                TimeSpan.FromSeconds(1)
                            ),
                            (_, _, _) =>
                            {
                                lock (s.Ran)
                                {
                                    s.Ran.Add(s.Name);
                                }

                                return ValueTask.CompletedTask;
                            },
                            options => options.Exclusive = true
                        );
                }
            })
            .Build();
}
