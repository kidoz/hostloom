namespace HostLoom.Caching.Testing;

/// <summary>
/// Decorates a store so a scenario can make the next <c>n</c> calls, or every call, fail with a
/// chosen <see cref="CacheFailureKind"/>. Forwards the inner store's invalidation channel when it
/// has one, so two caches over one faulting store still invalidate each other.
/// </summary>
public sealed class FaultingCacheStore(
    IDistributedCacheStore inner,
    ICacheInvalidationChannel? channel = null
) : IDistributedCacheStore, ISlidingExpirationCacheStore, ICacheInvalidationChannel
{
    private readonly Lock _gate = new();
    private CacheFailureKind _kind;
    private int _remaining;
    private bool _all;
    private string? _failedOperation;

    /// <summary>The wrapped store.</summary>
    public IDistributedCacheStore Inner { get; } = inner;

    /// <summary>Calls that reached the inner store.</summary>
    public int Calls { get; private set; }

    /// <summary>Calls that were failed by this decorator.</summary>
    public int Faulted { get; private set; }

    /// <inheritdoc />
    public CacheStoreCapabilities Capabilities => Inner.Capabilities;

    /// <summary>Fails the next <paramref name="count"/> calls with <paramref name="kind"/>.</summary>
    public void Fail(CacheFailureKind kind, int count) => FailOperation(kind, null, count);

    /// <summary>Fails the next <paramref name="count"/> calls to one named operation.</summary>
    public void FailOperation(CacheFailureKind kind, string? operation, int count = 1)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        lock (_gate)
        {
            _kind = kind;
            _remaining = count;
            _all = false;
            _failedOperation = operation;
        }
    }

    /// <summary>Fails every call with <paramref name="kind"/> until <see cref="Heal"/>.</summary>
    public void FailAll(CacheFailureKind kind)
    {
        lock (_gate)
        {
            _kind = kind;
            _all = true;
            _failedOperation = null;
        }
    }

    /// <summary>Stops failing calls.</summary>
    public void Heal()
    {
        lock (_gate)
        {
            _all = false;
            _remaining = 0;
            _failedOperation = null;
        }
    }

    /// <inheritdoc />
    public ValueTask<CacheStoreEntry?> GetAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        Gate("get");
        return Inner.GetAsync(key, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    )
    {
        Gate("set");
        return Inner.SetAsync(key, payload, timeToLive, tagKeys, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> SetIfAbsentAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    )
    {
        Gate("set-if-absent");
        return Inner.SetIfAbsentAsync(key, payload, timeToLive, tagKeys, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask RemoveAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        Gate("remove");
        return Inner.RemoveAsync(keys, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        Gate("get-many");
        return Inner.GetManyAsync(keys, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetManyAsync(
        IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>> entries,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default
    )
    {
        Gate("set-many");
        return Inner.SetManyAsync(entries, timeToLive, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask RemoveByTagAsync(string tagKey, CancellationToken cancellationToken = default)
    {
        Gate("remove-by-tag");
        return Inner.RemoveByTagAsync(tagKey, cancellationToken);
    }

    /// <summary>Runs after a completed read and before the wrapped conditional refresh.</summary>
    public Func<CancellationToken, ValueTask>? BeforeRefresh { get; set; }

    /// <inheritdoc />
    public async ValueTask<CacheRefreshOutcome> RefreshAsync(
        string key,
        ReadOnlyMemory<byte> expectedPayload,
        TimeSpan slidingWindow,
        DateTimeOffset? absoluteExpiration,
        CancellationToken cancellationToken = default
    )
    {
        Gate("refresh");
        if (BeforeRefresh is { } beforeRefresh)
        {
            await beforeRefresh(cancellationToken).ConfigureAwait(false);
        }

        return Inner is ISlidingExpirationCacheStore sliding
            ? await sliding
                .RefreshAsync(
                    key,
                    expectedPayload,
                    slidingWindow,
                    absoluteExpiration,
                    cancellationToken
                )
                .ConfigureAwait(false)
            : throw new NotSupportedException(
                "The wrapped store does not support sliding expiration."
            );
    }

    /// <summary>
    /// The channel messages travel on: the one given to the constructor, else the inner store's
    /// own, else none. Without one this store fans out nothing, like a backend store without a
    /// channel, and <see cref="ToString"/> says so for the cache probe.
    /// </summary>
    public ICacheInvalidationChannel? Channel { get; } =
        channel ?? inner as ICacheInvalidationChannel;

    /// <inheritdoc />
    public ValueTask PublishAsync(
        CacheInvalidation invalidation,
        CancellationToken cancellationToken = default
    ) => Channel?.PublishAsync(invalidation, cancellationToken) ?? ValueTask.CompletedTask;

    /// <inheritdoc />
    public IDisposable Subscribe(Action<CacheInvalidation> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        return Channel?.Subscribe(handler) ?? NoSubscription.Instance;
    }

    /// <summary>The wrapped store and whether invalidations have somewhere to travel.</summary>
    public override string ToString() =>
        Channel is null
            ? $"{nameof(FaultingCacheStore)} over {Inner} (no channel: TTL-only)"
            : $"{nameof(FaultingCacheStore)} over {Channel}";

    private sealed class NoSubscription : IDisposable
    {
        public static readonly NoSubscription Instance = new();

        public void Dispose() { }
    }

    private void Gate(string operation)
    {
        lock (_gate)
        {
            if (
                _all
                || (_remaining > 0 && (_failedOperation is null || _failedOperation == operation))
            )
            {
                if (!_all)
                {
                    _remaining--;
                }

                Faulted++;
                throw new CacheStoreException(
                    _kind,
                    $"Injected {_kind} failure during {operation}."
                );
            }

            Calls++;
        }
    }
}
