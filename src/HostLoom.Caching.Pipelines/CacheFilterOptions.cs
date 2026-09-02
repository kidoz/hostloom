using HostLoom.Pipelines;

namespace HostLoom.Caching.Pipelines;

/// <summary>
/// What a <see cref="CacheFilter{TContext, TPayload}"/> needs beyond the cache: how a context
/// maps to a key, and how long the payload it produces stays cached. Register one as a singleton
/// when the filter is resolved from the container.
/// </summary>
public sealed class CacheFilterOptions<TContext, TPayload>
    where TContext : class, IPipeContext
    where TPayload : class
{
    /// <summary>Derives the cache key from the context. Runs on every send.</summary>
    public required Func<TContext, string> KeySelector { get; init; }

    /// <summary>Expiration, tags, and the other per-entry settings applied on a miss.</summary>
    public required CacheEntryOptions Entry { get; init; }
}

/// <summary>
/// What a <see cref="DeduplicationFilter{TContext}"/> needs beyond the cache: how a context maps
/// to the identity being deduplicated, and how long a seen identity is remembered.
/// </summary>
public sealed class DeduplicationFilterOptions<TContext>
    where TContext : class, IPipeContext
{
    /// <summary>Derives the identity from the context, for example a message id.</summary>
    public required Func<TContext, string> IdSelector { get; init; }

    /// <summary>How long a seen identity suppresses further runs.</summary>
    public required TimeSpan Window { get; init; }
}
