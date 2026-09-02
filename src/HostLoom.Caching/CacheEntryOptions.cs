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
