namespace HostLoom.Caching;

/// <summary>
/// The cache a consumer injects: an in-process tier in front of an optional distributed tier,
/// with per-key single-flight and fail-open behaviour when the distributed store misbehaves.
/// </summary>
/// <remarks>
/// Every member is asynchronous, thread-safe, and takes a trailing optional
/// <see cref="CancellationToken"/>. A distributed-store failure never surfaces as an exception
/// from a read or a get-or-create: the cache degrades to the factory, keeps the in-process tier,
/// records a metric, and logs at most one warning per key per
/// <see cref="CacheDiagnosticsOptions.DegradedLogInterval"/>. The only members that can throw for
/// a store failure are <see cref="SetIfAbsentAsync{T}(string, T, CacheEntryOptions, CancellationToken)"/>
/// under <see cref="UnavailableBehavior.Throw"/>, and cancellation.
/// </remarks>
public interface ICache
{
    /// <summary>
    /// Returns the cached value for <paramref name="key"/>, or runs <paramref name="factory"/>
    /// once per key per process, stores its non-null result in both tiers, and returns it.
    /// </summary>
    /// <remarks>
    /// Lookup order is the in-process tier, then the distributed tier, then the single-flight
    /// guard, then the factory. A distributed hit repopulates the in-process tier with the
    /// remaining distributed time to live. A null factory result, or a non-positive
    /// <see cref="CacheEntryOptions.Expiration"/>, is returned without being stored. A factory
    /// exception propagates unchanged and nothing is stored.
    /// </remarks>
    ValueTask<T?> GetOrCreateAsync<T>(
        string key,
        Func<CancellationToken, ValueTask<T>> factory,
        CacheEntryOptions options,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// <see cref="GetOrCreateAsync{T}(string, Func{CancellationToken, ValueTask{T}}, CacheEntryOptions, CancellationToken)"/>
    /// with only an absolute expiration.
    /// </summary>
    ValueTask<T?> GetOrCreateAsync<T>(
        string key,
        Func<CancellationToken, ValueTask<T>> factory,
        TimeSpan expiration,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// The state-carrying form of get-or-create. The factory receives <paramref name="state"/>
    /// rather than closing over it, so an in-process hit allocates nothing on the caller's side.
    /// </summary>
    ValueTask<T?> GetOrCreateAsync<TState, T>(
        string key,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        CacheEntryOptions options,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Reads <paramref name="key"/> from the in-process tier, then the distributed tier.
    /// </summary>
    /// <remarks>
    /// Returns <c>default(T)</c> on a miss or when the distributed store is unavailable. For a
    /// value type that makes a cached <c>0</c>, <c>false</c>, or default-valued struct
    /// indistinguishable from a miss; use <see cref="TryGetAsync{T}"/> when the difference
    /// matters. This member exists for call sites written against that contract.
    /// </remarks>
    ValueTask<T?> GetAsync<T>(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads <paramref name="key"/> and reports whether it was found, in which tier, and whether
    /// the distributed tier was unavailable during the lookup.
    /// </summary>
    ValueTask<CacheLookup<T>> TryGetAsync<T>(
        string key,
        CancellationToken cancellationToken = default
    );

    /// <summary>Writes <paramref name="value"/> to the distributed tier, then the in-process tier.</summary>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null; null is never cached.</exception>
    ValueTask SetAsync<T>(
        string key,
        T value,
        CacheEntryOptions options,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Writes <paramref name="value"/> only when <paramref name="key"/> is absent. Atomic in the
    /// distributed tier; atomic in the in-process tier when there is no distributed tier.
    /// </summary>
    /// <returns>
    /// <see langword="true"/> when the value was written; <see langword="false"/> when a value
    /// was already present, or when the distributed store was unavailable and
    /// <see cref="CacheEntryOptions.OnUnavailable"/> is <see cref="UnavailableBehavior.ReturnFalse"/>.
    /// </returns>
    /// <exception cref="CacheUnavailableException">
    /// The distributed store was unavailable and <see cref="CacheEntryOptions.OnUnavailable"/> is
    /// <see cref="UnavailableBehavior.Throw"/>.
    /// </exception>
    ValueTask<bool> SetIfAbsentAsync<T>(
        string key,
        T value,
        CacheEntryOptions options,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// <see cref="SetIfAbsentAsync{T}(string, T, CacheEntryOptions, CancellationToken)"/> with only
    /// an absolute expiration and the default <see cref="UnavailableBehavior.ReturnFalse"/>.
    /// </summary>
    ValueTask<bool> SetIfAbsentAsync<T>(
        string key,
        T value,
        TimeSpan expiration,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Evicts <paramref name="key"/> from the in-process tier first, then the distributed tier,
    /// then publishes the invalidation to every other instance.
    /// </summary>
    ValueTask RemoveAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>
    /// Evicts every key in one distributed-store call and one invalidation message, in the same
    /// tier order as <see cref="RemoveAsync(string, CancellationToken)"/>.
    /// </summary>
    ValueTask RemoveAsync(IEnumerable<string> keys, CancellationToken cancellationToken = default);

    /// <summary>Evicts every entry written with <paramref name="tag"/>, on every instance.</summary>
    ValueTask RemoveByTagAsync(string tag, CancellationToken cancellationToken = default);

    /// <summary>
    /// Reads every key, in-process tier first and then the distributed tier in one batched call,
    /// and returns only the entries that were found. Never throws for a store failure; the result
    /// is then partial.
    /// </summary>
    ValueTask<IReadOnlyDictionary<string, T>> GetManyAsync<T>(
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Writes <paramref name="entries"/> to both tiers in batches of
    /// <see cref="CacheWarmupOptions.BatchSize"/>, reporting progress after each batch. Fail-open:
    /// a distributed-store failure is logged and the method returns.
    /// </summary>
    ValueTask WarmupAsync<T>(
        IReadOnlyDictionary<string, T> entries,
        TimeSpan expiration,
        IProgress<CacheWarmupProgress>? progress = null,
        CancellationToken cancellationToken = default
    );
}

/// <summary>Progress of one <see cref="ICache.WarmupAsync{T}"/> call.</summary>
/// <param name="Written">Entries written so far.</param>
/// <param name="Total">Entries in the call.</param>
public readonly record struct CacheWarmupProgress(int Written, int Total);
