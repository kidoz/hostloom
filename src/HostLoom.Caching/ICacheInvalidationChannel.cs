namespace HostLoom.Caching;

/// <summary>One invalidation message: consumer keys and tags to evict from every in-process tier.</summary>
/// <param name="Keys">Consumer keys, without the namespace prefix.</param>
/// <param name="Tags">Tag names, without the namespace prefix.</param>
public sealed record CacheInvalidation(
    IReadOnlyCollection<string> Keys,
    IReadOnlyCollection<string> Tags
);

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
