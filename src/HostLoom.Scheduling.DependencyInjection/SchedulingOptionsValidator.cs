using Microsoft.Extensions.Options;

namespace HostLoom.Scheduling.DependencyInjection;

/// <summary>
/// Runs <see cref="SchedulingOptions.Validate"/> at startup and adds the one rule only the
/// container knows: an exclusive schedule needs a guard.
/// </summary>
internal sealed class SchedulingOptionsValidator(SchedulingRegistration registration)
    : IValidateOptions<SchedulingOptions>
{
    public ValidateOptionsResult Validate(string? name, SchedulingOptions options)
    {
        List<string> problems = [.. options.Validate()];
        if (registration.ExclusiveSchedules.Count > 0 && registration.GuardName is null)
        {
            problems.Add(
                $"Schedule(s) {string.Join(", ", registration.ExclusiveSchedules.Select(static s => $"'{s}'"))} "
                    + "are exclusive but no guard was chosen. Call UseDistributedLock() from "
                    + "HostLoom.Scheduling.Locking or UseGuard<TGuard>(name) on the builder returned by "
                    + "AddHostLoomScheduling, or register the schedule without Exclusive."
            );
        }

        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(problems);
    }
}
