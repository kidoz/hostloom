namespace HostLoom.Caching;

/// <summary>
/// What <see cref="ICache.SetIfAbsentAsync{T}(string, T, CacheEntryOptions, CancellationToken)"/>
/// does when the distributed store cannot answer.
/// </summary>
public enum UnavailableBehavior
{
    /// <summary>Report "not written", so a rate limiter treats the outage as "deny".</summary>
    ReturnFalse,

    /// <summary>Throw <see cref="CacheUnavailableException"/>, so the caller decides.</summary>
    Throw,
}

/// <summary>Per-call settings for one cache entry. Defaults come from <see cref="CachingOptions"/>.</summary>
public sealed class CacheEntryOptions
{
    /// <summary>Creates options with an absolute expiration.</summary>
    public CacheEntryOptions(TimeSpan expiration) => Expiration = expiration;

    /// <summary>
    /// Absolute time to live in both tiers. Non-positive means "do not store": a get-or-create
    /// returns the factory result and writes nothing.
    /// </summary>
    public TimeSpan Expiration { get; init; }

    /// <summary>
    /// Shorter time to live for the in-process tier alone, so instances refresh from the
    /// distributed tier sooner. Must not exceed <see cref="Expiration"/>.
    /// </summary>
    public TimeSpan? LocalExpiration { get; init; }

    /// <summary>Tags attached at write time; <see cref="ICache.RemoveByTagAsync"/> evicts by them.</summary>
    public IReadOnlyCollection<string>? Tags { get; init; }

    /// <summary>
    /// Approximate size in bytes of the value, used by the in-process byte bound when the value
    /// did not arrive serialized from the distributed tier.
    /// </summary>
    public long? Size { get; init; }

    /// <summary>Behaviour of set-if-absent when the distributed store is unavailable.</summary>
    public UnavailableBehavior OnUnavailable { get; init; } = UnavailableBehavior.ReturnFalse;

    /// <summary>
    /// Negative caching: how long a null factory result is remembered in both tiers, so a lookup
    /// for something that does not exist stops reaching the source on every call. Unset or
    /// non-positive means a null result is returned and not stored. A remembered null is a hit:
    /// <see cref="ICache.TryGetAsync{T}"/> reports <c>Found</c> with a null <c>Value</c>, and
    /// get-or-create returns null without running the factory. <see cref="LocalExpiration"/>
    /// bounds the in-process copy as it does for a value. <see cref="ICache.SetAsync{T}"/> still
    /// rejects null; only a factory result is remembered.
    /// </summary>
    public TimeSpan? NullExpiration { get; init; }

    /// <summary>
    /// Stale-while-revalidate for an outage: how long past its in-process expiry an entry stays
    /// available to serve while the distributed store is unavailable. When a get-or-create
    /// misses the in-process tier, the distributed read fails, and a stale copy is inside the
    /// grace window, one caller refreshes through the factory and every other caller receives the
    /// stale copy at once. While the distributed store answers, an expired entry is an ordinary
    /// miss. Unset or non-positive disables it. Has no effect without a distributed store.
    /// </summary>
    public TimeSpan? StaleGrace { get; init; }

    internal TimeSpan? EffectiveStaleGrace =>
        StaleGrace is { } grace && grace > TimeSpan.Zero ? grace : null;

    internal bool CachesNull => NullExpiration is { } ttl && ttl > TimeSpan.Zero;

    internal void Validate(string parameterName)
    {
        if (LocalExpiration is { } local && local > Expiration)
        {
            throw new ArgumentException(
                $"{nameof(LocalExpiration)} ({local}) must not exceed {nameof(Expiration)} ({Expiration}).",
                parameterName
            );
        }

        if (Tags is not null)
        {
            foreach (var tag in Tags)
            {
                CacheKey.Validate(tag, int.MaxValue, nameof(Tags));
            }
        }
    }
}
