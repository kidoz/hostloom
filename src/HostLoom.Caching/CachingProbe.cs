namespace HostLoom.Caching;

/// <summary>
/// Execution-free description of one cache: what was composed and which option decided it.
/// Safe to call from a debug endpoint on every request.
/// </summary>
/// <param name="Namespace">The key prefix.</param>
/// <param name="Store">The distributed store type, or <c>InMemory</c> when there is none.</param>
/// <param name="L1Enabled">Whether the in-process tier is on.</param>
/// <param name="Serializer">The serializer type, or null when there is no distributed tier.</param>
/// <param name="Invalidation">How other instances learn about evictions.</param>
/// <param name="Warmups">Registered warmup type names.</param>
/// <param name="Lines">Human-readable lines, each naming the option key that decided it.</param>
public sealed record CacheDescription(
    string Namespace,
    string Store,
    bool L1Enabled,
    string? Serializer,
    string Invalidation,
    IReadOnlyList<string> Warmups,
    IReadOnlyList<string> Lines
);

/// <summary>Describes an <see cref="ICache"/> without executing anything.</summary>
public static class CachingProbe
{
    /// <summary>
    /// Describes <paramref name="cache"/>. Warmups are supplied by the caller because the cache
    /// itself does not know what a composition root registered around it.
    /// </summary>
    public static CacheDescription Describe(ICache cache, IEnumerable<ICacheWarmup>? warmups = null)
    {
        ArgumentNullException.ThrowIfNull(cache);
        var warmupNames = warmups?.Select(static warmup => warmup.GetType().Name).ToList() ?? [];
        if (cache is TieredCache tiered)
        {
            return tiered.Describe(warmupNames);
        }

        var typeName = cache.GetType().Name;
        return new CacheDescription(
            Namespace: "(unknown)",
            Store: typeName,
            L1Enabled: false,
            Serializer: null,
            Invalidation: "(unknown)",
            Warmups: warmupNames,
            Lines: [$"Cache = {typeName} (not a HostLoom tiered cache; nothing else is known)."]
        );
    }
}
