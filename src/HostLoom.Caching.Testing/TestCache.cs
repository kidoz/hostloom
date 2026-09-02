namespace HostLoom.Caching.Testing;

/// <summary>
/// Composes a <see cref="TieredCache"/> for a test without a container, with the defaults a test
/// wants: a namespace, no jitter, and whatever clock the test controls.
/// </summary>
/// <remarks>
/// The kernel's constructors are public, so this adds nothing a test could not write itself; it
/// removes the boilerplate and keeps every test on one composition. Substituting
/// <see cref="ICache"/> is the other option and a worse one: a substitute returns what the test
/// told it to, so the test passes whether or not the consumer would have been served by a real
/// cache.
/// </remarks>
public static class TestCache
{
    /// <summary>The namespace every test cache uses unless the options say otherwise.</summary>
    public const string Namespace = "test";

    /// <summary>An in-process-only cache: the in-process tier is the only tier.</summary>
    public static TieredCache InMemory(
        Action<CachingOptions>? configure = null,
        TimeProvider? timeProvider = null
    ) => new(Options(configure), timeProvider: timeProvider);

    /// <summary>
    /// A two-tier cache over <paramref name="store"/>. Two caches over one
    /// <see cref="InMemoryDistributedCacheStore"/> behave like two service instances sharing a
    /// backend: they share payloads and invalidate each other.
    /// </summary>
    public static TieredCache Tiered(
        IDistributedCacheStore store,
        ICacheValueSerializer serializer,
        Action<CachingOptions>? configure = null,
        TimeProvider? timeProvider = null,
        ICacheInvalidationChannel? channel = null
    )
    {
        ArgumentNullException.ThrowIfNull(store);
        ArgumentNullException.ThrowIfNull(serializer);
        return new TieredCache(Options(configure), store, serializer, channel, timeProvider);
    }

    /// <summary>Options with the test namespace and every jitter and pause turned off.</summary>
    public static CachingOptions Options(Action<CachingOptions>? configure = null)
    {
        var options = new CachingOptions { Namespace = Namespace };
        options.L1.ExpirationJitter = TimeSpan.Zero;
        options.Stampede.WaitBeforeFallback = TimeSpan.Zero;
        configure?.Invoke(options);
        return options;
    }
}
