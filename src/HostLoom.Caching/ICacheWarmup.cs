namespace HostLoom.Caching;

/// <summary>
/// A unit of startup warmup: writes what a service needs hot before it takes traffic. Registered
/// through the dependency-injection package and run once after the store is usable.
/// </summary>
public interface ICacheWarmup
{
    /// <summary>Fills <paramref name="cache"/>. Fail-open: a store failure is logged, not thrown.</summary>
    ValueTask WarmupAsync(ICache cache, CancellationToken cancellationToken);
}
