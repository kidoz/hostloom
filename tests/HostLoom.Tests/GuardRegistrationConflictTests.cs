using HostLoom.Leadership;
using HostLoom.Leadership.DependencyInjection;
using HostLoom.Leadership.Testing;
using HostLoom.Scheduling;
using HostLoom.Scheduling.DependencyInjection;
using HostLoom.Scheduling.Leadership;
using HostLoom.Scheduling.Locking;
using HostLoom.Scheduling.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A guard or a leadership registered before the builder would have won over the one the
/// builder chooses while the probe reported the chosen one; the builder refuses it and names
/// both, and the deliberate ways to stand in for a chosen guard keep working.
/// </summary>
public sealed class GuardRegistrationConflictTests
{
    [Fact]
    public void A_guard_registered_before_the_builder_is_refused_naming_both()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IScheduleGuard>(new ManualScheduleGuard());
        var builder = services.AddHostLoomScheduling();

        var failure = Assert.Throws<InvalidOperationException>(() => builder.UseDistributedLock());

        Assert.Contains("'DistributedLock'", failure.Message, StringComparison.Ordinal);
        Assert.Contains("ManualScheduleGuard", failure.Message, StringComparison.Ordinal);
        Assert.Contains("(an instance)", failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(
            services,
            descriptor => descriptor.ServiceType == typeof(DistributedLockScheduleGuard)
        );
        // Nothing was chosen, so the next choice is judged on its own and refused the same way.
        var again = Assert.Throws<InvalidOperationException>(() => builder.UseLeader("scheduler"));
        Assert.Contains("'Leader:scheduler'", again.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_guard_registered_through_a_factory_is_named_as_one()
    {
        var services = new ServiceCollection();
        services.AddSingleton<IScheduleGuard>(_ => new ManualScheduleGuard());

        var failure = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomScheduling().UseDistributedLock()
        );

        Assert.Contains("a factory registration", failure.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task A_double_registered_after_the_builder_replaces_the_chosen_guard_deliberately()
    {
        var services = new ServiceCollection();
        var manual = new ManualScheduleGuard();
        services.AddHostLoomScheduling().UseDistributedLock();
        services.AddSingleton<IScheduleGuard>(manual);
        await using var provider = services.BuildServiceProvider();

        Assert.Same(manual, provider.GetRequiredService<IScheduleGuard>());
    }

    [Fact]
    public async Task The_guard_type_registered_first_is_the_one_UseGuard_chooses()
    {
        var services = new ServiceCollection();
        var manual = new ManualScheduleGuard();
        services.AddSingleton(manual);
        services.AddHostLoomScheduling().UseGuard<ManualScheduleGuard>("Manual");
        await using var provider = services.BuildServiceProvider();

        Assert.Same(manual, provider.GetRequiredService<IScheduleGuard>());
    }

    [Fact]
    public void An_ILeadership_registered_before_AddHostLoomLeadership_is_refused_naming_it()
    {
        var services = new ServiceCollection();
        using var manual = new ManualLeadership("scheduler");
        services.AddSingleton<ILeadership>(manual);

        var failure = Assert.Throws<InvalidOperationException>(() =>
            services.AddHostLoomLeadership()
        );

        Assert.Contains("ILeadership", failure.Message, StringComparison.Ordinal);
        Assert.Contains("ManualLeadership", failure.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(
            services,
            descriptor => descriptor.ServiceType.Name == "LeadershipRegistration"
        );
    }

    [Fact]
    public async Task A_keyed_double_is_refused_for_a_role_that_is_added_and_serves_one_that_is_not()
    {
        var services = new ServiceCollection();
        using var manual = new ManualLeadership("reconciler");
        services.AddKeyedSingleton<ILeadership>("reconciler", manual);
        var builder = services.AddHostLoomLeadership();

        var failure = Assert.Throws<InvalidOperationException>(() => builder.AddRole("reconciler"));

        Assert.Contains("'reconciler'", failure.Message, StringComparison.Ordinal);
        Assert.Contains("ManualLeadership", failure.Message, StringComparison.Ordinal);

        // Another role is added as usual, a repeated AddHostLoomLeadership is fine, and the
        // double drives a leader-guarded schedule for its role without an elector.
        builder.AddRole("scheduler");
        services.AddHostLoomLeadership();
        services.AddHostLoomScheduling().UseLeader("reconciler");
        await using var provider = services.BuildServiceProvider();

        Assert.Same(manual, provider.GetRequiredKeyedService<ILeadership>("reconciler"));
        var guard = Assert.IsType<LeaderScheduleGuard>(
            provider.GetRequiredService<IScheduleGuard>()
        );
        Assert.Equal("reconciler", guard.Role);
        Assert.True(guard.IsCoordinated);
        manual.IsCoordinated = false;
        Assert.False(guard.IsCoordinated);
    }
}
