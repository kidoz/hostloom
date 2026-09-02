namespace HostLoom.Caching.Testing;

/// <summary>One call a <see cref="RecordingCacheStore"/> saw.</summary>
/// <param name="Operation">The store member, for example <c>get</c> or <c>set-if-absent</c>.</param>
/// <param name="Key">The fully prefixed key, or the first key of a bulk call.</param>
public readonly record struct RecordedCacheCall(string Operation, string Key);

/// <summary>
/// Decorates a store and records every call, so a test asserts what the cache asked the
/// distributed tier for: one lease per stampede, one batched read for a bulk lookup, no write
/// after a null factory result. Forwards the inner store's invalidation channel when it has one.
/// </summary>
public sealed class RecordingCacheStore(
    IDistributedCacheStore inner,
    ICacheInvalidationChannel? channel = null
) : IDistributedCacheStore, ICacheInvalidationChannel
{
    private readonly Lock _gate = new();
    private readonly List<RecordedCacheCall> _calls = [];

    /// <summary>The wrapped store.</summary>
    public IDistributedCacheStore Inner { get; } = inner;

    /// <summary>Every call so far, in order.</summary>
    public IReadOnlyList<RecordedCacheCall> Calls
    {
        get
        {
            lock (_gate)
            {
                return [.. _calls];
            }
        }
    }

    /// <inheritdoc />
    public CacheStoreCapabilities Capabilities => Inner.Capabilities;

    /// <summary>Forgets the calls recorded so far.</summary>
    public void Reset()
    {
        lock (_gate)
        {
            _calls.Clear();
        }
    }

    /// <summary>How many calls named <paramref name="operation"/> were recorded.</summary>
    public int Count(string operation)
    {
        lock (_gate)
        {
            return _calls.Count(call => call.Operation == operation);
        }
    }

    /// <inheritdoc />
    public ValueTask<CacheStoreEntry?> GetAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        Record("get", key);
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
        Record("set", key);
        return Inner.SetAsync(key, payload, timeToLive, tagKeys, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<bool> SetIfAbsentAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default
    )
    {
        Record("set-if-absent", key);
        return Inner.SetIfAbsentAsync(key, payload, timeToLive, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask RemoveAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        Record("remove", keys.FirstOrDefault() ?? "");
        return Inner.RemoveAsync(keys, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        Record("get-many", keys.FirstOrDefault() ?? "");
        return Inner.GetManyAsync(keys, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask SetManyAsync(
        IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>> entries,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default
    )
    {
        Record("set-many", entries.FirstOrDefault().Key ?? "");
        return Inner.SetManyAsync(entries, timeToLive, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask RemoveByTagAsync(string tagKey, CancellationToken cancellationToken = default)
    {
        Record("remove-by-tag", tagKey);
        return Inner.RemoveByTagAsync(tagKey, cancellationToken);
    }

    /// <inheritdoc />
    public ValueTask PublishAsync(
        CacheInvalidation invalidation,
        CancellationToken cancellationToken = default
    )
    {
        Record(
            "publish",
            invalidation.Keys.FirstOrDefault() ?? invalidation.Tags.FirstOrDefault() ?? ""
        );
        return Channel.PublishAsync(invalidation, cancellationToken);
    }

    /// <inheritdoc />
    public IDisposable Subscribe(Action<CacheInvalidation> handler) => Channel.Subscribe(handler);

    private ICacheInvalidationChannel Channel =>
        channel
        ?? Inner as ICacheInvalidationChannel
        ?? throw new InvalidOperationException("The inner store offers no invalidation channel.");

    private void Record(string operation, string key)
    {
        lock (_gate)
        {
            _calls.Add(new RecordedCacheCall(operation, key));
        }
    }
}
