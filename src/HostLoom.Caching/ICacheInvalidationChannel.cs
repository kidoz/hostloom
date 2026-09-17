namespace HostLoom.Caching;

/// <summary>One invalidation message: consumer keys and tags to evict from every in-process tier.</summary>
/// <param name="Keys">Consumer keys, without the namespace prefix.</param>
/// <param name="Tags">Tag names, without the namespace prefix.</param>
public sealed record CacheInvalidation(
    IReadOnlyCollection<string> Keys,
    IReadOnlyCollection<string> Tags
)
{
    /// <summary>
    /// A message that clears the whole in-process tier. A backend channel raises it to its own
    /// subscribers after a reconnect, because invalidations published during the outage were never
    /// delivered; every entry could be stale, so every entry is dropped.
    /// </summary>
    public static CacheInvalidation Flush { get; } = new([], []) { FlushAll = true };

    /// <summary>
    /// Whether the message clears every entry rather than the listed keys and tags. The Redis and
    /// Valkey channels carry it on the wire; an instance on an earlier package version ignores or
    /// rejects it, so a published flush reaches every instance only once the deploy is complete.
    /// </summary>
    public bool FlushAll { get; init; }
}

/// <summary>
/// Fan-out of invalidations between instances that share one distributed store. A store without a
/// channel degrades to time-to-live-only staleness, and the probe says so.
/// </summary>
/// <remarks>
/// Keys and tags travel unprefixed because a channel is scoped to one namespace. Delivery is
/// best-effort; a lost message costs at most one in-process expiry.
/// </remarks>
public interface ICacheInvalidationChannel
{
    /// <summary>Publishes one message to every subscriber, including this instance.</summary>
    ValueTask PublishAsync(
        CacheInvalidation invalidation,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Delivers every published message to <paramref name="handler"/> until the returned
    /// subscription is disposed. The handler must not block; the cache queues the work.
    /// </summary>
    IDisposable Subscribe(Action<CacheInvalidation> handler);
}
