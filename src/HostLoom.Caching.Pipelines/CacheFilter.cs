using HostLoom.Pipelines;

namespace HostLoom.Caching.Pipelines;

/// <summary>
/// What the cache filter decided for one send, left on the context so a later filter or the
/// caller can tell a served-from-cache run from a computed one.
/// </summary>
/// <param name="Key">The key the selector produced.</param>
/// <param name="Hit">Whether the payload came from the cache instead of the downstream pipe.</param>
/// <param name="Tier">Which tier answered, or <see cref="CacheTier.None"/> on a miss.</param>
/// <param name="Degraded">Whether the distributed tier was unavailable during the lookup.</param>
public sealed record CacheFilterResult(string Key, bool Hit, CacheTier Tier, bool Degraded);

/// <summary>
/// Get-or-create around the downstream pipe. On a hit the cached <typeparamref name="TPayload"/>
/// is placed on the context and the rest of the pipe does not run; on a miss the rest of the
/// pipe runs, and whatever <typeparamref name="TPayload"/> the context holds afterwards is
/// written to the cache.
/// </summary>
/// <remarks>
/// The lookup is <see cref="ICache.TryGetAsync{T}"/> and the write is
/// <see cref="ICache.SetAsync{T}"/> rather than a get-or-create call, because the "factory" here
/// is the downstream pipe, which has to run with the context rather than inside a delegate.
/// Two concurrent misses for one key therefore both run downstream; use a distributed-lock
/// filter ahead of this one when a single computation per key matters. The cache is fail-open,
/// so a store failure never surfaces here; the result records <c>Degraded</c> instead.
/// </remarks>
public sealed class CacheFilter<TContext, TPayload> : IFilter<TContext>
    where TContext : class, IPipeContext
    where TPayload : class
{
    private readonly ICache _cache;
    private readonly CacheFilterOptions<TContext, TPayload> _options;

    /// <summary>Creates the filter over <paramref name="cache"/> with <paramref name="options"/>.</summary>
    public CacheFilter(ICache cache, CacheFilterOptions<TContext, TPayload> options)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(options);
        _cache = cache;
        _options = options;
    }

    /// <inheritdoc />
    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        var key = _options.KeySelector(context);
        var lookup = await _cache
            .TryGetAsync<TPayload>(key, context.CancellationToken)
            .ConfigureAwait(false);
        if (lookup.Found)
        {
            var cached = lookup.Value!;
            context.AddOrUpdatePayload(() => cached, _ => cached);
            var hit = new CacheFilterResult(key, true, lookup.Tier, lookup.Degraded);
            context.AddOrUpdatePayload(() => hit, _ => hit);
            return;
        }

        var miss = new CacheFilterResult(key, false, CacheTier.None, lookup.Degraded);
        context.AddOrUpdatePayload(() => miss, _ => miss);
        await next.SendAsync(context).ConfigureAwait(false);

        if (context.TryGetPayload<TPayload>(out var produced) && produced is not null)
        {
            await _cache
                .SetAsync(key, produced, _options.Entry, context.CancellationToken)
                .ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public void Probe(IProbeContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var scope = context.CreateScope("cache");
        scope.Set("payload", typeof(TPayload).Name);
        scope.Set("expiration", _options.Entry.Expiration);
        scope.Set("localExpiration", _options.Entry.LocalExpiration);
        scope.Set("tags", _options.Entry.Tags?.Count ?? 0);
    }
}
