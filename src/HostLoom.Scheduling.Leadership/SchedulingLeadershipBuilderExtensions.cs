using HostLoom.Leadership;
using HostLoom.Scheduling.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.Scheduling.Leadership;

/// <summary>Chooses the elected leader as the schedule guard.</summary>
public static class SchedulingLeadershipBuilderExtensions
{
    /// <summary>
    /// Runs exclusive schedules on the instance that leads <paramref name="role"/>, registered
    /// by <c>AddHostLoomLeadership().AddRole(role)</c>. Followers skip every occurrence; a run in
    /// progress is cancelled when leadership ends.
    /// </summary>
    /// <exception cref="InvalidOperationException">A guard was already chosen; the message names it.</exception>
    public static SchedulingBuilder UseLeader(this SchedulingBuilder builder, string role)
    {
        ArgumentNullException.ThrowIfNull(builder);
        LeadershipRole.Validate(role);
        builder.Services.TryAddSingleton(provider => new LeaderScheduleGuard(
            provider.GetRequiredKeyedService<ILeadership>(role)
        ));
        return builder.UseGuard<LeaderScheduleGuard>("Leader:" + role);
    }
}
