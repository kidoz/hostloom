namespace HostLoom.Caching;

/// <summary>Which tier answered a lookup.</summary>
public enum CacheTier
{
    /// <summary>The key was not found in any tier.</summary>
    None,

    /// <summary>The in-process tier.</summary>
    L1,

    /// <summary>The distributed tier.</summary>
    L2,
}

/// <summary>
/// Result of <see cref="ICache.TryGetAsync{T}"/>: whether the key was found, its value, the
/// tier that answered, and whether the distributed tier was unavailable during the lookup.
/// </summary>
/// <remarks>
/// Unlike <see cref="ICache.GetAsync{T}"/>, this distinguishes a cached <c>default(T)</c> from a
/// miss, which matters for value types.
/// </remarks>
public readonly record struct CacheLookup<T>(bool Found, T? Value, CacheTier Tier, bool Degraded);

/// <summary>Factories for <see cref="CacheLookup{T}"/>.</summary>
public static class CacheLookup
{
    /// <summary>A miss, optionally recorded while the distributed tier was unavailable.</summary>
    public static CacheLookup<T> Miss<T>(bool degraded = false) =>
        new(false, default, CacheTier.None, degraded);

    /// <summary>A hit from <paramref name="tier"/>.</summary>
    public static CacheLookup<T> Hit<T>(T value, CacheTier tier, bool degraded = false) =>
        new(true, value, tier, degraded);
}
