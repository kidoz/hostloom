using Microsoft.Extensions.Logging;

namespace HostLoom.NodeLeasing;

/// <summary>Stable event ids for every log line the elector writes.</summary>
public static class NodeLeasingEvents
{
    /// <summary>Information: the elector started as a candidate.</summary>
    public static readonly EventId Started = new(3500, "NodeLeasingStarted");

    /// <summary>Information: this instance acquired an ID.</summary>
    public static readonly EventId Acquired = new(3501, "NodeLeaseAcquired");

    /// <summary>Warning: a renewal was refused or the lease ran out; this instance stepped back to candidate.</summary>
    public static readonly EventId Lost = new(3502, "NodeLeaseLost");

    /// <summary>Information: this instance released its ID on request.</summary>
    public static readonly EventId Released = new(3503, "NodeLeaseReleased");

    /// <summary>Warning, once per outage: the lock provider is unreachable; no ID can be assigned until it answers.</summary>
    public static readonly EventId ProviderUnavailable = new(
        3504,
        "NodeLeasingProviderUnavailable"
    );

    /// <summary>Information: the provider answered again after an outage.</summary>
    public static readonly EventId ProviderRecovered = new(3505, "NodeLeasingProviderRecovered");

    /// <summary>Information: the elector stopped and released the lease it held.</summary>
    public static readonly EventId Stopped = new(3506, "NodeLeasingStopped");

    /// <summary>Warning: a change listener threw; the elector continues.</summary>
    public static readonly EventId ListenerFailed = new(3507, "NodeLeasingListenerFailed");

    /// <summary>Warning: an assignment token cancellation callback threw; cleanup and leasing continue.</summary>
    public static readonly EventId CancellationCallbackFailed = new(
        3508,
        "NodeLeasingCancellationCallbackFailed"
    );

    /// <summary>
    /// Warning, once per pool: the lock does not coordinate across instances
    /// (<c>Locking:Enabled = false</c>); the elector follows <c>NodeLeasing:WhenUncoordinated</c>.
    /// </summary>
    public static readonly EventId Uncoordinated = new(3509, "NodeLeasingUncoordinated");

    /// <summary>Error: the elector loop threw something other than a stop; it backed off one retry interval and continues.</summary>
    public static readonly EventId LoopFaulted = new(3510, "NodeLeasingLoopFaulted");
}
