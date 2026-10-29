using System.Collections.Concurrent;

namespace HostLoom.Caching;

/// <summary>
/// An <see cref="IDistributedCacheStore"/> and <see cref="ICacheInvalidationChannel"/> held in
/// process memory. It gives the tiered composition, the serializer, invalidation, the
/// conformance suite, and a Native AOT sample a serialized second tier without a backend.
/// </summary>
/// <remarks>
/// Two <see cref="TieredCache"/> instances over one store instance behave like two service
/// instances over one backend: they share payloads and receive each other's invalidations.
/// Nothing crosses a process boundary.
/// </remarks>
public sealed class InMemoryDistributedCacheStore
    : IDistributedCacheStore,
        ISlidingExpirationCacheStore,
        ICacheInvalidationChannel
{
    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly ConcurrentDictionary<string, ConcurrentDictionary<string, byte>> _tags = new(
        StringComparer.Ordinal
    );
    private readonly ConcurrentDictionary<Subscription, byte> _subscriptions = new();
    private readonly TimeProvider _time;
    private readonly Lock _gate = new();
    private long _nextCleanupTicks;

    /// <summary>Creates the store on <paramref name="timeProvider"/> or the system clock.</summary>
    public InMemoryDistributedCacheStore(TimeProvider? timeProvider = null) =>
        _time = timeProvider ?? TimeProvider.System;

    /// <inheritdoc />
    public CacheStoreCapabilities Capabilities =>
        CacheStoreCapabilities.Tags
        | CacheStoreCapabilities.InvalidationChannel
        | CacheStoreCapabilities.SlidingExpiration;

    /// <summary>Entries currently held after reclaiming expired values and unused tag memberships.</summary>
    public int Count
    {
        get
        {
            lock (_gate)
            {
                Maintain(force: true);
                return _entries.Count;
            }
        }
    }

    internal int TagCount
    {
        get
        {
            lock (_gate)
            {
                Maintain(force: true);
                return _tags.Count;
            }
        }
    }

    /// <inheritdoc />
    public ValueTask<CacheStoreEntry?> GetAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        lock (_gate)
        {
            Maintain();
            cancellationToken.ThrowIfCancellationRequested();
            return ValueTask.FromResult(TryRead(key));
        }
    }

    /// <inheritdoc />
    public ValueTask<CacheRefreshOutcome> RefreshAsync(
        string key,
        ReadOnlyMemory<byte> expectedPayload,
        TimeSpan slidingWindow,
        DateTimeOffset? absoluteExpiration,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(slidingWindow, TimeSpan.Zero);
        lock (_gate)
        {
            Maintain();
            cancellationToken.ThrowIfCancellationRequested();
            var now = _time.GetUtcNow().UtcTicks;
            var expiration = absoluteExpiration?.UtcTicks;
            if (!_entries.TryGetValue(key, out var entry))
            {
                return ValueTask.FromResult(
                    expiration <= now ? CacheRefreshOutcome.Expired : CacheRefreshOutcome.Missing
                );
            }

            if (entry.ExpiresAt <= now)
            {
                _entries.TryRemove(key, out _);
                return ValueTask.FromResult(
                    expiration <= now ? CacheRefreshOutcome.Expired : CacheRefreshOutcome.Missing
                );
            }

            if (expiration <= now)
            {
                return ValueTask.FromResult(CacheRefreshOutcome.Expired);
            }

            if (!entry.Payload.AsSpan().SequenceEqual(expectedPayload.Span))
            {
                return ValueTask.FromResult(CacheRefreshOutcome.GenerationMismatch);
            }

            var timeToLive = slidingWindow.Ticks;
            if (expiration is { } cap && cap - now < timeToLive)
            {
                timeToLive = cap - now;
            }

            if (timeToLive <= 0)
            {
                return ValueTask.FromResult(CacheRefreshOutcome.Expired);
            }

            _entries[key] = entry with { ExpiresAt = now + timeToLive };
            return ValueTask.FromResult(CacheRefreshOutcome.Applied);
        }
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
        lock (_gate)
        {
            Maintain();
            cancellationToken.ThrowIfCancellationRequested();
            Write(key, payload, timeToLive);
            Index(key, tagKeys);
            return ValueTask.CompletedTask;
        }
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
        lock (_gate)
        {
            Maintain();
            cancellationToken.ThrowIfCancellationRequested();
            var now = _time.GetUtcNow().UtcTicks;
            var entry = new Entry(payload.ToArray(), now + timeToLive.Ticks);
            while (true)
            {
                if (_entries.TryAdd(key, entry))
                {
                    Index(key, tagKeys);
                    return ValueTask.FromResult(true);
                }

                if (_entries.TryGetValue(key, out var existing))
                {
                    if (existing.ExpiresAt > now)
                    {
                        return ValueTask.FromResult(false);
                    }

                    if (_entries.TryUpdate(key, entry, existing))
                    {
                        Index(key, tagKeys);
                        return ValueTask.FromResult(true);
                    }
                }
            }
        }
    }

    /// <inheritdoc />
    public ValueTask RemoveAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        lock (_gate)
        {
            Maintain();
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var key in keys)
            {
                _entries.TryRemove(key, out _);
            }

            return ValueTask.CompletedTask;
        }
    }

    /// <inheritdoc />
    public ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        lock (_gate)
        {
            Maintain();
            cancellationToken.ThrowIfCancellationRequested();
            var found = new Dictionary<string, CacheStoreEntry>(StringComparer.Ordinal);
            foreach (var key in keys)
            {
                if (TryRead(key) is { } entry)
                {
                    found[key] = entry;
                }
            }

            return ValueTask.FromResult<IReadOnlyDictionary<string, CacheStoreEntry>>(found);
        }
    }

    /// <inheritdoc />
    public ValueTask SetManyAsync(
        IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>> entries,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default
    )
    {
        lock (_gate)
        {
            Maintain();
            cancellationToken.ThrowIfCancellationRequested();
            foreach (var (key, payload) in entries)
            {
                Write(key, payload, timeToLive);
            }

            return ValueTask.CompletedTask;
        }
    }

    /// <inheritdoc />
    public ValueTask RemoveByTagAsync(string tagKey, CancellationToken cancellationToken = default)
    {
        lock (_gate)
        {
            Maintain();
            cancellationToken.ThrowIfCancellationRequested();
            if (_tags.TryRemove(tagKey, out var members))
            {
                foreach (var key in members.Keys)
                {
                    _entries.TryRemove(key, out _);
                }
            }

            return ValueTask.CompletedTask;
        }
    }

    /// <inheritdoc />
    public ValueTask PublishAsync(
        CacheInvalidation invalidation,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(invalidation);
        cancellationToken.ThrowIfCancellationRequested();
        foreach (var subscription in _subscriptions.Keys)
        {
            subscription.Handler(invalidation);
        }

        return ValueTask.CompletedTask;
    }

    /// <inheritdoc />
    public IDisposable Subscribe(Action<CacheInvalidation> handler)
    {
        ArgumentNullException.ThrowIfNull(handler);
        var subscription = new Subscription(this, handler);
        _subscriptions[subscription] = 0;
        return subscription;
    }

    /// <inheritdoc />
    public override string ToString() => nameof(InMemoryDistributedCacheStore) + " (in-process)";

    private void Maintain(bool force = false)
    {
        var now = _time.GetUtcNow().UtcTicks;
        if (!force && now < _nextCleanupTicks)
            return;
        _nextCleanupTicks = now + TimeSpan.TicksPerSecond;
        foreach (var pair in _entries)
            if (pair.Value.ExpiresAt <= now)
                _entries.TryRemove(pair.Key, out _);
        foreach (var pair in _tags)
        {
            foreach (var key in pair.Value.Keys)
                if (!_entries.ContainsKey(key))
                    pair.Value.TryRemove(key, out _);
            if (pair.Value.IsEmpty)
                _tags.TryRemove(pair.Key, out _);
        }
    }

    private CacheStoreEntry? TryRead(string key)
    {
        if (!_entries.TryGetValue(key, out var entry))
        {
            return null;
        }

        var remaining = entry.ExpiresAt - _time.GetUtcNow().UtcTicks;
        if (remaining <= 0)
        {
            _entries.TryRemove(new KeyValuePair<string, Entry>(key, entry));
            return null;
        }

        return new CacheStoreEntry(entry.Payload, TimeSpan.FromTicks(remaining));
    }

    private void Index(string key, IReadOnlyCollection<string>? tagKeys)
    {
        if (tagKeys is null)
        {
            return;
        }

        foreach (var tagKey in tagKeys)
        {
            _tags.GetOrAdd(tagKey, static _ => new ConcurrentDictionary<string, byte>())[key] = 0;
        }
    }

    private void Write(string key, ReadOnlyMemory<byte> payload, TimeSpan timeToLive)
    {
        if (timeToLive <= TimeSpan.Zero)
        {
            _entries.TryRemove(key, out _);
            return;
        }

        // Copied: the caller's buffer is borrowed only until this call returns.
        _entries[key] = new Entry(payload.ToArray(), _time.GetUtcNow().UtcTicks + timeToLive.Ticks);
    }

    private sealed record Entry(byte[] Payload, long ExpiresAt);

    private sealed class Subscription(
        InMemoryDistributedCacheStore owner,
        Action<CacheInvalidation> handler
    ) : IDisposable
    {
        public Action<CacheInvalidation> Handler { get; } = handler;

        public void Dispose() => owner._subscriptions.TryRemove(this, out _);
    }
}
