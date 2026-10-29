using HostLoom.Locking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace HostLoom.Leadership.DependencyInjection;

/// <summary>
/// Runs <see cref="LeadershipOptions.Validate(string, int)"/> for every role's named options when the host
/// starts, checking resolved keys and lease limits against that role's exact lock profile.
/// </summary>
internal sealed class LeadershipOptionsValidator(
    IServiceProvider services,
    LeadershipRegistration registration
) : IValidateOptions<LeadershipOptions>
{
    public ValidateOptionsResult Validate(string? name, LeadershipOptions options)
    {
        var locks = ComposedLock(name, out var missingProfile);
        List<string> problems =
        [
            .. name is null or ""
                ? options.Validate()
                : options.Validate(name, (locks as DistributedLock)?.MaxKeyLength ?? int.MaxValue),
        ];
        if (missingProfile is not null)
        {
            problems.Add(
                $"Leadership role '{name}' requires lock profile '{missingProfile}', but that profile is not registered."
            );
        }

        if (
            locks is DistributedLock { Enabled: true } composed
            && composed.Describe().MaxLease is var maxLease
            && options.Lease > maxLease
        )
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
    /// The registered HostLoom lock, when available. Invalid lock options report their own errors.
    /// </summary>
    private IDistributedLock? ComposedLock(string? name, out string? missingProfile)
    {
        missingProfile = null;
        string? profile = null;
        if (
            name is not null
            && registration.LockProfiles.TryGetValue(name, out var configuredProfile)
        )
        {
            profile = configuredProfile;
        }

        try
        {
            var locks = profile is null
                ? services.GetService<IDistributedLock>()
                : services.GetKeyedService<IDistributedLock>(profile);
            if (profile is not null && locks is null)
            {
                missingProfile = profile;
            }

            return locks;
        }
        catch (OptionsValidationException)
        {
            // The lock's own options are invalid; their validation reports that, not this one.
            return null;
        }
    }
}
