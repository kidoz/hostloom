using Microsoft.Extensions.Logging;

namespace HostLoom.Locking;

/// <summary>
/// Stable event ids for every log line the locking kernel writes, so a logging pipeline can
/// filter or alert on one condition without matching message text.
/// </summary>
public static class LockingEvents
{
    /// <summary>Warning at construction: <c>Locking:Enabled</c> is false, single-instance mode.</summary>
    public static readonly EventId Disabled = new(3100, "LockingDisabled");

    /// <summary>Information at construction: the retry policy and its derived maximum wait.</summary>
    public static readonly EventId DerivedMaxWait = new(3101, "LockDerivedMaxWait");

    /// <summary>Warning: a lease expired or the provider refused the owner; exclusivity is gone.</summary>
    public static readonly EventId LeaseLost = new(3102, "LockLeaseLost");

    /// <summary>Warning: a hold passed 80 % of its lease without release or extension.</summary>
    public static readonly EventId HoldThreshold = new(3103, "LockHoldThreshold");

    /// <summary>Warning: the provider threw while releasing; the lease expires on its own.</summary>
    public static readonly EventId ReleaseFailed = new(3104, "LockReleaseFailed");

    /// <summary>Warning: the provider threw while extending; the lease keeps its previous end.</summary>
    public static readonly EventId ExtendFailed = new(3105, "LockExtendFailed");

    /// <summary>Information: automatic extension stopped at <c>Locking:MaxHold</c>.</summary>
    public static readonly EventId AutoExtendStopped = new(3106, "LockAutoExtendStopped");
}
