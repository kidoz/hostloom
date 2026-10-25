using HostLoom.Locking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace HostLoom.NodeLeasing.DependencyInjection;

/// <summary>
/// Runs <see cref="NodeLeasingOptions.Validate"/> for every pool's named options when the host
/// starts, and adds the one rule only the container knows: the pool's lease must fit within
/// <c>Locking:MaxLease</c> of the lock the electors run over, which would otherwise cap it
/// silently.
/// </summary>
internal sealed class NodeLeasingOptionsValidator(IServiceProvider services)
    : IValidateOptions<NodeLeasingOptions>
{
    public ValidateOptionsResult Validate(string? name, NodeLeasingOptions options)
    {
        List<string> problems = [.. options.Validate()];
        var limits = Limits();
        if (limits is { } lockLimits && options.Lease > lockLimits.MaxLease)
        {
            problems.Add(
                $"NodeLeasing:Lease ({options.Lease}) must be at most Locking:MaxLease ({lockLimits.MaxLease}); "
                    + "the lock would cap the lease, and the elector would hold its ID on a shorter lease "
                    + "than it was configured with."
            );
        }
        if (
            limits is { } keyLimits
            && name is not null
            && $"nodeid:{name}:{(Math.Max(1, options.PoolSize) - 1).ToString(System.Globalization.CultureInfo.InvariantCulture)}".Length
                > keyLimits.MaxKeyLength
        )
        {
            problems.Add(
                $"The generated node-ID key exceeds Locking:MaxKeyLength ({keyLimits.MaxKeyLength})."
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

    /// <summary>
    /// <c>Locking:MaxLease</c> of the registered lock when it is a coordinating
    /// <see cref="DistributedLock"/>; <see langword="null"/> for any other lock, for
    /// single-instance mode, which takes no lease, and when no lock is registered, which the
    /// elector reports itself.
    /// </summary>
    private (TimeSpan MaxLease, int MaxKeyLength)? Limits()
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
            ? (composed.Describe().MaxLease, composed.MaxKeyLength)
            : null;
    }
}
