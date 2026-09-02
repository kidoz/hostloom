using HostLoom.Pipelines;

namespace HostLoom.Locking.Pipelines;

/// <summary>
/// What a <see cref="DistributedLockFilter{TContext}"/> needs beyond the lock: how a context maps
/// to the key being serialised, and the per-acquisition options. Register one as a singleton when
/// the filter is resolved from the container.
/// </summary>
public sealed class DistributedLockFilterOptions<TContext>
    where TContext : class, IPipeContext
{
    /// <summary>Derives the lock key from the context. Runs on every send.</summary>
    public required Func<TContext, string> KeySelector { get; init; }

    /// <summary>Lease, wait bound, retry, and lost-lease behaviour; the lock's defaults when null.</summary>
    public LockOptions? Lock { get; init; }
}

/// <summary>
/// The lock the downstream pipe runs under, left on the context so a later filter can observe
/// it. The context's own cancellation token cannot be replaced, so a filter that wants to stop
/// when the lease is lost watches <see cref="CancellationToken"/>, which the lock cancels when
/// <see cref="LockOptions.OnLost"/> is <see cref="LostLeaseBehavior.Cancel"/>.
/// </summary>
public sealed class HeldLock
{
    internal HeldLock(string key, CancellationToken cancellationToken)
    {
        Key = key;
        CancellationToken = cancellationToken;
    }

    /// <summary>The consumer key the selector produced.</summary>
    public string Key { get; }

    /// <summary>
    /// The token the lock handed to the run: the caller's token, linked to the lost-lease token
    /// when the options ask for cancellation on loss.
    /// </summary>
    public CancellationToken CancellationToken { get; }
}

/// <summary>
/// Serialises runs per key: the rest of the pipe executes inside
/// <see cref="IDistributedLock.ExecuteWithLockAsync(string, Func{CancellationToken, ValueTask}, LockOptions?, CancellationToken)"/>,
/// so two contexts with the same key never run downstream at once, across every instance sharing
/// the lock's provider.
/// </summary>
/// <remarks>
/// <see cref="LockNotAcquiredException"/> and <see cref="LockProviderUnavailableException"/>
/// propagate unchanged; a retry filter ahead of this one is the place to decide whether to try
/// again. The lock is coordination, not correctness, for persisted state.
/// </remarks>
public sealed class DistributedLockFilter<TContext> : IFilter<TContext>
    where TContext : class, IPipeContext
{
    private readonly IDistributedLock _lock;
    private readonly DistributedLockFilterOptions<TContext> _options;

    /// <summary>Creates the filter over <paramref name="lockService"/> with <paramref name="options"/>.</summary>
    public DistributedLockFilter(
        IDistributedLock lockService,
        DistributedLockFilterOptions<TContext> options
    )
    {
        ArgumentNullException.ThrowIfNull(lockService);
        ArgumentNullException.ThrowIfNull(options);
        _lock = lockService;
        _options = options;
    }

    /// <inheritdoc />
    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        ArgumentNullException.ThrowIfNull(context);
        ArgumentNullException.ThrowIfNull(next);

        var key = _options.KeySelector(context);
        await _lock
            .ExecuteWithLockAsync(
                key,
                async token =>
                {
                    var held = new HeldLock(key, token);
                    context.AddOrUpdatePayload(() => held, _ => held);
                    await next.SendAsync(context).ConfigureAwait(false);
                },
                _options.Lock,
                context.CancellationToken
            )
            .ConfigureAwait(false);
    }

    /// <inheritdoc />
    public void Probe(IProbeContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        var scope = context.CreateScope("distributedLock");
        scope.Set("lease", _options.Lock?.Lease);
        scope.Set("maxWait", _options.Lock?.MaxWait);
        scope.Set("retry", _options.Lock?.Retry?.Description);
        scope.Set("onLost", (_options.Lock?.OnLost ?? LostLeaseBehavior.Observe).ToString());
    }
}
