namespace HostLoom.Locking;

/// <summary>
/// Per-call overrides. A <see langword="null"/> member falls back to <see cref="LockingOptions"/>.
/// </summary>
public sealed class LockOptions
{
    /// <summary>Lease length, capped by <see cref="LockingOptions.MaxLease"/>. Default: <see cref="LockingOptions.DefaultLease"/>.</summary>
    public TimeSpan? Lease { get; set; }

    /// <summary>
    /// Hard wall-clock bound on acquisition: no attempt starts on or after it, no delay reaches it,
    /// and a provider call still running at it is cancelled, so a backend that never answers cannot
    /// stretch the wait. <see cref="TimeSpan.Zero"/> makes exactly one attempt (skip-if-busy),
    /// bounded only by the caller's token. <see langword="null"/> bounds acquisition by the retry
    /// policy alone.
    /// </summary>
    public TimeSpan? MaxWait { get; set; }

    /// <summary>Retry shape between attempts. Default: <see cref="LockingOptions.Retry"/>.</summary>
    public LockRetryPolicy? Retry { get; set; }

    /// <summary>Heartbeat the lease at half its length until <see cref="LockingOptions.MaxHold"/>. Default: <see cref="LockingOptions.AutoExtend"/>.</summary>
    public bool? AutoExtend { get; set; }

    /// <summary>What happens to the action when the lease is lost.</summary>
    public LostLeaseBehavior OnLost { get; set; } = LostLeaseBehavior.Observe;
}
