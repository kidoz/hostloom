using HostLoom.Pipelines;

namespace HostLoom.Caching.Pipelines;

/// <summary>Left on the context when a send was suppressed because its identity was seen inside the window.</summary>
/// <param name="Id">The identity the selector produced.</param>
public sealed record Deduplicated(string Id);

/// <summary>
/// Left on the context when the store could not say whether the identity was seen, so the send
/// ran anyway. Processing twice is recoverable; dropping a message on an outage is not.
/// </summary>
/// <param name="Id">The identity the selector produced.</param>
/// <param name="Kind">Why the store could not answer.</param>
public sealed record DeduplicationSkipped(string Id, CacheFailureKind Kind);

/// <summary>
/// Runs the downstream pipe at most once per identity inside a window, by claiming the identity
/// with an atomic set-if-absent before running. A claim that succeeds runs the pipe; a claim
/// that finds the identity present adds <see cref="Deduplicated"/> and stops.
/// </summary>
/// <remarks>
/// <para>
/// The claim happens before processing, so a run that fails after claiming is not repeated
/// inside the window; put a retry filter after this one when a failed run should be retried.
/// </para>
/// <para>
/// When the store is unavailable the claim cannot be made, and the pipe runs anyway with
/// <see cref="DeduplicationSkipped"/> on the context: at-least-once is the safe side of an
/// outage. This filter is offered for generic pipelines; deduplication on the messaging
/// receive pipeline is not provided by HostLoom until the decision deferring idempotent consumer
/// storage is revisited.
/// </para>
/// </remarks>
public sealed class DeduplicationFilter<TContext> : IFilter<TContext>
    where TContext : class, IPipeContext
{
    private const string KeyPrefix = "dedup:";

    private readonly ICache _cache;
    private readonly DeduplicationFilterOptions<TContext> _options;

    /// <summary>Creates the filter over <paramref name="cache"/> with <paramref name="options"/>.</summary>
    public DeduplicationFilter(ICache cache, DeduplicationFilterOptions<TContext> options)
    {
        ArgumentNullException.ThrowIfNull(cache);
        ArgumentNullException.ThrowIfNull(options);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(options.Window, TimeSpan.Zero);
        _cache = cache;
        _options = options;
    }

    /// <inheritdoc />
    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        var id = _options.IdSelector(context);
        var entry = new CacheEntryOptions(_options.Window)
        {
            OnUnavailable = UnavailableBehavior.Throw,
        };
        bool claimed;
        try
        {
            // The marker is the id itself: a string, which every serializer can write.
            claimed = await _cache
                .SetIfAbsentAsync(KeyPrefix + id, id, entry, context.CancellationToken)
                .ConfigureAwait(false);
        }
        catch (CacheUnavailableException exception)
        {
            var skipped = new DeduplicationSkipped(id, exception.Kind);
            context.AddOrUpdatePayload(() => skipped, _ => skipped);
            await next.SendAsync(context).ConfigureAwait(false);
            return;
        }

        if (!claimed)
        {
            var duplicate = new Deduplicated(id);
            context.AddOrUpdatePayload(() => duplicate, _ => duplicate);
            return;
        }

        await next.SendAsync(context).ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Probe(IProbeContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var scope = context.CreateScope("deduplication");
        scope.Set("window", _options.Window);
        scope.Set("onUnavailable", "run");
    }
}
