namespace HostLoom.Locking;

/// <summary>
/// Optional capability a lock provider may implement so readiness can report backend
/// reachability. A provider that does not implement it is treated as reachable, because
/// "cannot tell" must not read as "broken". Liveness never consults it.
/// </summary>
public interface ILockProviderHealthProbe
{
    /// <summary>
    /// Reports whether the backend is usable right now. Must not throw for an unreachable
    /// backend; return an unhealthy result instead.
    /// </summary>
    ValueTask<LockProviderHealth> CheckHealthAsync(CancellationToken cancellationToken);
}

/// <summary>Outcome of <see cref="ILockProviderHealthProbe.CheckHealthAsync"/>.</summary>
/// <param name="IsHealthy">Whether the backend is usable right now.</param>
/// <param name="Description">Human-readable detail, surfaced in the health report.</param>
public sealed record LockProviderHealth(bool IsHealthy, string Description)
{
    /// <summary>A usable backend.</summary>
    public static LockProviderHealth Healthy(string description = "Lock provider is reachable.") =>
        new(true, description);

    /// <summary>An unusable backend, with the reason.</summary>
    public static LockProviderHealth Unhealthy(string description) => new(false, description);
}
