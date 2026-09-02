using HostLoom.Pipelines;

namespace HostLoom.Caching.Pipelines;

/// <summary>Adds the cache and deduplication filters to a pipe composed without a container.</summary>
public static class CachingPipeBuilderExtensions
{
    /// <summary>
    /// Serves a cached <typeparamref name="TPayload"/> for the key <paramref name="keySelector"/>
    /// derives, or runs the rest of the pipe and caches the <typeparamref name="TPayload"/> it
    /// leaves on the context.
    /// </summary>
    public static PipeBuilder<TContext> UseCache<TContext, TPayload>(
        this PipeBuilder<TContext> builder,
        ICache cache,
        Func<TContext, string> keySelector,
        CacheEntryOptions options
    )
        where TContext : class, IPipeContext
        where TPayload : class
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(keySelector);
        ArgumentNullException.ThrowIfNull(options);
        return builder.Use(
            new CacheFilter<TContext, TPayload>(
                cache,
                new CacheFilterOptions<TContext, TPayload>
                {
                    KeySelector = keySelector,
                    Entry = options,
                }
            )
        );
    }

    /// <summary>
    /// Runs the rest of the pipe at most once per identity <paramref name="idSelector"/> derives,
    /// inside <paramref name="window"/>. Runs anyway when the store cannot answer.
    /// </summary>
    public static PipeBuilder<TContext> UseDeduplication<TContext>(
        this PipeBuilder<TContext> builder,
        ICache cache,
        Func<TContext, string> idSelector,
        TimeSpan window
    )
        where TContext : class, IPipeContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(idSelector);
        return builder.Use(
            new DeduplicationFilter<TContext>(
                cache,
                new DeduplicationFilterOptions<TContext>
                {
                    IdSelector = idSelector,
                    Window = window,
                }
            )
        );
    }
}
