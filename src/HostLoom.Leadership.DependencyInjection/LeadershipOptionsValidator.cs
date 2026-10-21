using HostLoom.Locking;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Options;

namespace HostLoom.Leadership.DependencyInjection;

/// <summary>
/// Runs <see cref="LeadershipOptions.Validate(string, int)"/> for every role's named options when the host
/// starts, checking resolved keys against <c>Locking:MaxKeyLength</c>. The role's lease must fit within
/// <c>Locking:MaxLease</c> of the lock the electors run over, which would otherwise cap it silently.
/// </summary>
internal sealed class LeadershipOptionsValidator(IServiceProvider services)
    : IValidateOptions<LeadershipOptions>
{
    public ValidateOptionsResult Validate(string? name, LeadershipOptions options)
    {
        var locks = ComposedLock();
        List<string> problems =
        [
            .. name is null or ""
                ? options.Validate()
                : options.Validate(name, locks?.MaxKeyLength ?? int.MaxValue),
        ];
        if (
            locks is { Enabled: true }
            && locks.Describe().MaxLease is var maxLease
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
    private DistributedLock? ComposedLock()
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

        return locks as DistributedLock;
    }
}
