namespace HostLoom.Caching.Testing;

/// <summary>
/// Decorates a store so a scenario can make the next <c>n</c> calls, or every call, fail with a
/// chosen <see cref="CacheFailureKind"/>. Forwards the inner store's invalidation channel when it
/// has one, so two caches over one faulting store still invalidate each other.
/// </summary>
public sealed class FaultingCacheStore(
    IDistributedCacheStore inner,
    ICacheInvalidationChannel? channel = null
) : IDistributedCacheStore, ICacheInvalidationChannel
{
    private readonly Lock _gate = new();
    private CacheFailureKind _kind;
    private int _remaining;
    private bool _all;

    /// <summary>The wrapped store.</summary>
    public IDistributedCacheStore Inner { get; } = inner;

    /// <summary>Calls that reached the inner store.</summary>
    public int Calls { get; private set; }

    /// <summary>Calls that were failed by this decorator.</summary>
    public int Faulted { get; private set; }

    /// <inheritdoc />
    public CacheStoreCapabilities Capabilities => Inner.Capabilities;

    /// <summary>Fails the next <paramref name="count"/> calls with <paramref name="kind"/>.</summary>
    public void Fail(CacheFailureKind kind, int count)
    {
        lock (_gate)
        {
            _kind = kind;
            _remaining = count;
            _all = false;
        }
    }

    /// <summary>Fails every call with <paramref name="kind"/> until <see cref="Heal"/>.</summary>
    public void FailAll(CacheFailureKind kind)
    {
        lock (_gate)
        {
            _kind = kind;
            _all = true;
        }
    }

    /// <summary>Stops failing calls.</summary>
    public void Heal()
    {
        lock (_gate)
        {
            _all = false;
            _remaining = 0;
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

    /// <inheritdoc />
    public ValueTask PublishAsync(
        CacheInvalidation invalidation,
        CancellationToken cancellationToken = default
    ) => Channel.PublishAsync(invalidation, cancellationToken);

    /// <inheritdoc />
    public IDisposable Subscribe(Action<CacheInvalidation> handler) => Channel.Subscribe(handler);

    private ICacheInvalidationChannel Channel =>
        channel
        ?? Inner as ICacheInvalidationChannel
        ?? throw new InvalidOperationException("The inner store offers no invalidation channel.");

    private void Gate(string operation)
    {
        lock (_gate)
        {
            if (_all || _remaining > 0)
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
