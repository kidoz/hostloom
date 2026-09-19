using Microsoft.Extensions.Logging;

namespace HostLoom.Leadership;

/// <summary>Stable event ids for every log line the elector writes.</summary>
public static class LeadershipEvents
{
    /// <summary>Information: the elector started as a candidate.</summary>
    public static readonly EventId Started = new(3400, "LeadershipStarted");

    /// <summary>Information: this instance became leader.</summary>
    public static readonly EventId Acquired = new(3401, "LeadershipAcquired");

    /// <summary>Warning: a renewal failed or the lease expired; this instance stepped back to candidate.</summary>
    public static readonly EventId Lost = new(3402, "LeadershipLost");

    /// <summary>Information: this instance resigned on request.</summary>
    public static readonly EventId Resigned = new(3403, "LeadershipResigned");

    /// <summary>Warning, once per outage: the lock provider is unreachable; nobody can become leader.</summary>
    public static readonly EventId ProviderUnavailable = new(3404, "LeadershipProviderUnavailable");

    /// <summary>Information: the provider answered again after an outage.</summary>
    public static readonly EventId ProviderRecovered = new(3405, "LeadershipProviderRecovered");

    /// <summary>Information: the elector stopped and released the lease it held.</summary>
    public static readonly EventId Stopped = new(3406, "LeadershipStopped");

    /// <summary>Warning: a change listener threw; the elector continues.</summary>
    public static readonly EventId ListenerFailed = new(3407, "LeadershipListenerFailed");

    /// <summary>Warning: a leadership token cancellation callback threw; cleanup and election continue.</summary>
    public static readonly EventId CancellationCallbackFailed = new(
        3408,
        "LeadershipCancellationCallbackFailed"
    );
}
