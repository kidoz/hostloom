using HostLoom.Locking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace HostLoom.NodeLeasing.DependencyInjection;

/// <summary>
/// Runs <see cref="NodeLeasingOptions.Validate"/> for every pool's named options when the host
/// starts, checking generated keys and lease limits against that pool's exact lock profile.
/// </summary>
internal sealed class NodeLeasingOptionsValidator(
    IServiceProvider services,
    NodeLeasingRegistration registration
) : IValidateOptions<NodeLeasingOptions>
{
    public ValidateOptionsResult Validate(string? name, NodeLeasingOptions options)
    {
        List<string> problems = [.. options.Validate()];
        var locks = Lock(name, out var missingProfile);
        var composed = locks as DistributedLock;
        if (missingProfile is not null)
        {
            problems.Add(
                $"Node-leasing pool '{name}' requires lock profile '{missingProfile}', but that profile is not registered."
            );
        }

        if (composed is { Enabled: true } && options.Lease > composed.Describe().MaxLease)
        {
            problems.Add(
                $"NodeLeasing:Lease ({options.Lease}) must be at most Locking:MaxLease ({composed.Describe().MaxLease}); "
                    + "the lock would cap the lease, and the elector would hold its ID on a shorter lease "
                    + "than it was configured with."
            );
        }
        if (
            composed is not null
            && name is not null
            && $"nodeid:{name}:{(Math.Max(1, options.PoolSize) - 1).ToString(System.Globalization.CultureInfo.InvariantCulture)}".Length
                > composed.MaxKeyLength
        )
        {
            problems.Add(
                $"The generated node-ID key exceeds Locking:MaxKeyLength ({composed.MaxKeyLength})."
            );
        }

        return problems.Count == 0
            ? ValidateOptionsResult.Success
            : ValidateOptionsResult.Fail(
                problems.Select(problem =>
                    name is null or "" ? problem : $"Pool '{name}': {problem}"
                )
            );
    }

    private IDistributedLock? Lock(string? name, out string? missingProfile)
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
