namespace HostLoom.Caching;

/// <summary>
/// Optional capability a distributed store implements so readiness can report backend
/// reachability. A store without it is treated as reachable, because "cannot tell" must not read
/// as "broken". Liveness never touches it.
/// </summary>
public interface ICacheStoreHealthProbe
{
    /// <summary>Reports whether the backend is usable. Never throws; returns a failed result instead.</summary>
    ValueTask<CacheStoreHealth> CheckHealthAsync(CancellationToken cancellationToken);
}

/// <summary>Outcome of <see cref="ICacheStoreHealthProbe.CheckHealthAsync"/>.</summary>
/// <param name="IsHealthy">Whether the backend is usable right now.</param>
/// <param name="Description">Human-readable detail, surfaced in the health report.</param>
public sealed record CacheStoreHealth(bool IsHealthy, string Description)
{
    /// <summary>A healthy result.</summary>
    public static CacheStoreHealth Healthy(string description = "Cache store is reachable.") =>
        new(true, description);

    /// <summary>An unhealthy result.</summary>
    public static CacheStoreHealth Unhealthy(string description) => new(false, description);
}
