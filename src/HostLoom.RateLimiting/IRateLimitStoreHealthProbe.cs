namespace HostLoom.RateLimiting;

/// <summary>Optional backend reachability probe used only by readiness.</summary>
public interface IRateLimitStoreHealthProbe
{
    /// <summary>Reports backend health without throwing for an unreachable backend.</summary>
    ValueTask<RateLimitStoreHealth> CheckHealthAsync(CancellationToken cancellationToken);
}

/// <summary>Outcome of <see cref="IRateLimitStoreHealthProbe.CheckHealthAsync"/>.</summary>
public sealed record RateLimitStoreHealth(bool IsHealthy, string Description)
{
    /// <summary>A usable backend.</summary>
    public static RateLimitStoreHealth Healthy(
        string description = "Rate-limit store is reachable."
    ) => new(true, description);

    /// <summary>An unusable backend, with a sanitized reason.</summary>
    public static RateLimitStoreHealth Unhealthy(string description) => new(false, description);
}
