namespace HostLoom.Locking;

/// <summary>
/// Execution-free description of a lock composition, in the spirit of <c>HostLoomProbe</c>. Safe
/// to call from a health or debug endpoint on every request; it never contacts a provider.
/// </summary>
public static class LockingProbe
{
    /// <summary>
    /// Describes <paramref name="lockService"/>. A <see cref="DistributedLock"/> reports its
    /// namespace, provider, enabled state, leases, and retry shape, each line naming the option
    /// key that decided it; any other implementation is reported by type with nothing else known.
    /// </summary>
    public static LockDescription Describe(IDistributedLock lockService)
    {
        ArgumentNullException.ThrowIfNull(lockService);
        if (lockService is DistributedLock composed)
        {
            return composed.Describe();
        }

        var type = lockService.GetType().FullName ?? lockService.GetType().Name;
        return new LockDescription(
            "",
            type,
            true,
            TimeSpan.Zero,
            TimeSpan.Zero,
            "",
            TimeSpan.Zero,
            [$"Provider = {type}: not a HostLoom composition, nothing else is known"]
        );
    }
}

/// <summary>What <see cref="LockingProbe.Describe"/> reports.</summary>
/// <param name="Namespace"><see cref="LockingOptions.Namespace"/>.</param>
/// <param name="Provider">The provider type name, or <c>(disabled)</c> in single-instance mode.</param>
/// <param name="Enabled"><see cref="LockingOptions.Enabled"/>.</param>
/// <param name="DefaultLease"><see cref="LockingOptions.DefaultLease"/>.</param>
/// <param name="MaxLease"><see cref="LockingOptions.MaxLease"/>.</param>
/// <param name="Retry"><see cref="LockRetryPolicy.Description"/> of the default policy.</param>
/// <param name="MaxWait">The derived maximum wait of the default policy.</param>
/// <param name="Lines">Human-readable lines, each naming the option key that decided it.</param>
public sealed record LockDescription(
    string Namespace,
    string Provider,
    bool Enabled,
    TimeSpan DefaultLease,
    TimeSpan MaxLease,
    string Retry,
    TimeSpan MaxWait,
    IReadOnlyList<string> Lines
);
