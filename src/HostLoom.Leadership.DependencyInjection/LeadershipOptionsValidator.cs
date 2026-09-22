using HostLoom.Locking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace HostLoom.Leadership.DependencyInjection;

/// <summary>
/// Runs <see cref="LeadershipOptions.Validate"/> for every role's named options when the host
/// starts, and adds the one rule only the container knows: the role's lease must fit within
/// <c>Locking:MaxLease</c> of the lock the electors run over, which would otherwise cap it silently.
/// </summary>
internal sealed class LeadershipOptionsValidator(IServiceProvider services)
    : IValidateOptions<LeadershipOptions>
{
    public ValidateOptionsResult Validate(string? name, LeadershipOptions options)
    {
        List<string> problems = [.. options.Validate()];
        if (MaxLease() is { } maxLease && options.Lease > maxLease)
        {
            problems.Add(
                $"Leadership:Lease ({options.Lease}) must be at most Locking:MaxLease ({maxLease}); "
                    + "the lock would cap the lease, and the elector would lead on a shorter lease "
                    + "than it was configured with."
            );
        }

        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(
                problems.Select(problem =>
                    name is null or "" ? problem : $"Role '{name}': {problem}"
                )
            );
    }

    /// <summary>
    /// <c>Locking:MaxLease</c> of the registered lock when it is a coordinating
    /// <see cref="DistributedLock"/>; <see langword="null"/> for any other lock, for single-instance
    /// mode, which takes no lease, and when no lock is registered, which the elector reports itself.
    /// </summary>
    private TimeSpan? MaxLease()
    {
        IDistributedLock? locks;
        try
        {
            locks = services.GetService<IDistributedLock>();
        }
        catch (OptionsValidationException)
        {
            // The lock's own options are invalid; their validation reports that, not this one.
            return null;
        }

        return locks is DistributedLock { Enabled: true } composed
            ? composed.Describe().MaxLease
            : null;
    }
}
