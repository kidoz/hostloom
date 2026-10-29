using HostLoom.Locking;
using HostLoom.Scheduling.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Options;

namespace HostLoom.Scheduling.Locking;

/// <summary>Chooses the distributed lock as the schedule guard.</summary>
public static class SchedulingLockingBuilderExtensions
{
    /// <summary>
    /// Guards exclusive schedules with the default <c>IDistributedLock</c> or the one keyed by
    /// <paramref name="lockProfile"/>. Each run claims <c>schedule:{name}</c> skip-if-busy for the
    /// schedule's lease; the selected lock's namespace, provider, and <c>Locking:MaxHold</c> apply.
    /// </summary>
    /// <param name="builder">The scheduling composition.</param>
    /// <param name="lockProfile">The keyed lock profile, or null for the default unkeyed lock.</param>
    /// <exception cref="InvalidOperationException">A guard was already chosen; the message names it.</exception>
    public static SchedulingBuilder UseDistributedLock(
        this SchedulingBuilder builder,
        string? lockProfile = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        if (lockProfile is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(lockProfile);
        }

        builder.UseGuard<DistributedLockScheduleGuard>("DistributedLock");
        if (lockProfile is not null)
        {
            builder.Services.Replace(
                ServiceDescriptor.Singleton(provider => new DistributedLockScheduleGuard(
                    provider.GetKeyedService<IDistributedLock>(lockProfile)
                        ?? throw new InvalidOperationException(
                            $"Distributed-lock schedule guards require lock profile '{lockProfile}', but that profile is not registered."
                        )
                ))
            );
        }

        builder.Services.AddSingleton<IValidateOptions<SchedulingOptions>>(
            provider => new DistributedLockSchedulingOptionsValidator(provider, lockProfile)
        );
        return builder;
    }
}
