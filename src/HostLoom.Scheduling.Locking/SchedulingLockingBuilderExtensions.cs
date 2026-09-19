using HostLoom.Scheduling.DependencyInjection;

namespace HostLoom.Scheduling.Locking;

/// <summary>Chooses the distributed lock as the schedule guard.</summary>
public static class SchedulingLockingBuilderExtensions
{
    /// <summary>
    /// Guards exclusive schedules with the <c>IDistributedLock</c> registered by
    /// <c>AddHostLoomLocking</c>. Each run claims <c>schedule:{name}</c> skip-if-busy for the
    /// schedule's lease; the lock's namespace, provider, and <c>Locking:MaxHold</c> apply.
    /// </summary>
    /// <exception cref="InvalidOperationException">A guard was already chosen; the message names it.</exception>
    public static SchedulingBuilder UseDistributedLock(this SchedulingBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.UseGuard<DistributedLockScheduleGuard>("DistributedLock");
    }
}
