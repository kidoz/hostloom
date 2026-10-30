using System.Threading.RateLimiting;

namespace HostLoom.RateLimiting;

/// <summary>Additional lease metadata supplied by HostLoom rate limiters.</summary>
public static class RateLimitMetadata
{
    /// <summary>Name of <see cref="Unavailable"/>.</summary>
    public const string UnavailableName = "HostLoom.RateLimiting.Unavailable";

    /// <summary>Name of <see cref="Degraded"/>.</summary>
    public const string DegradedName = "HostLoom.RateLimiting.Degraded";

    /// <summary>Name of <see cref="RemainingPermits"/>.</summary>
    public const string RemainingPermitsName = "HostLoom.RateLimiting.RemainingPermits";

    /// <summary>True when acquisition failed because no store decision could be obtained.</summary>
    public static readonly MetadataName<bool> Unavailable = new(UnavailableName);

    /// <summary>True when work was explicitly allowed without a confirmed store decision.</summary>
    public static readonly MetadataName<bool> Degraded = new(DegradedName);

    /// <summary>Whole permits remaining after the store decision, when known.</summary>
    public static readonly MetadataName<int> RemainingPermits = new(RemainingPermitsName);
}
