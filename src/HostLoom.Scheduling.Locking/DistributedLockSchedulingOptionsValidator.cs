using HostLoom.Locking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace HostLoom.Scheduling.Locking;

internal sealed class DistributedLockSchedulingOptionsValidator(
    IServiceProvider services,
    string? lockProfile
) : IValidateOptions<SchedulingOptions>
{
    public ValidateOptionsResult Validate(string? name, SchedulingOptions options)
    {
        IDistributedLock? locks;
        try
        {
            locks = lockProfile is null
                ? services.GetService<IDistributedLock>()
                : services.GetKeyedService<IDistributedLock>(lockProfile);
        }
        catch (OptionsValidationException)
        {
            return ValidateOptionsResult.Success;
        }

        var schedules = services
            .GetServices<ScheduleDefinition>()
            .Where(static definition => definition.Options.Exclusive)
            .ToArray();
        List<string> problems = [];
        if (lockProfile is not null && locks is null)
        {
            foreach (var schedule in schedules)
            {
                problems.Add(
                    $"Schedule '{schedule.Name}' requires lock profile '{lockProfile}', but that profile is not registered."
                );
            }
        }

        if (locks is DistributedLock composed)
        {
            foreach (var schedule in schedules)
            {
                var lease = schedule.Options.Lease ?? options.DefaultLease;
                if (composed.Enabled && lease > composed.Describe().MaxLease)
                {
                    problems.Add(
                        $"Schedule '{schedule.Name}': Lease ({lease}) must be at most Locking:MaxLease ({composed.Describe().MaxLease}) for its bound lock."
                    );
                }

                var keyLength = DistributedLockScheduleGuard.KeyFor(schedule.Name).Length;
                if (keyLength > composed.MaxKeyLength)
                {
                    problems.Add(
                        $"Schedule '{schedule.Name}': The generated lock key exceeds Locking:MaxKeyLength ({composed.MaxKeyLength}) for its bound lock."
                    );
                }
            }
        }

        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(problems);
    }
}
