using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.Caching;

/// <summary>
/// The in-process tier: a bounded, lock-free-on-read dictionary of typed values with absolute
/// expiry. It has no <c>IMemoryCache</c> dependency and never stores null.
/// </summary>
/// <remarks>
/// Above <see cref="CacheL1Options.MaxEntries"/> a sampled least-recently-accessed
/// <see cref="CacheL1Options.EvictionFraction"/> is evicted; at 150 % of capacity everything is
/// cleared and a warning logged. A cleanup timer on the <see cref="TimeProvider"/> removes
/// expired entries every <see cref="CacheL1Options.CleanupInterval"/> and stops on dispose.
/// </remarks>
public sealed class LocalCacheStore : IDisposable
{
    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly CacheL1Options _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly ITimer? _cleanup;
    private long _approximateBytes;
    private int _evicting;

    /// <summary>Creates the tier with <paramref name="options"/>.</summary>
    public LocalCacheStore(
        CacheL1Options options,
        TimeProvider? timeProvider = null,
        ILogger<LocalCacheStore>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        var problems = new List<string>();
        options.Validate(problems);
        if (problems.Count > 0)
        {
            throw new ArgumentException(string.Join(" ", problems), nameof(options));
        }

        _options = options;
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<LocalCacheStore>.Instance;
        _cleanup = _time.CreateTimer(
            static state => ((LocalCacheStore)state!).RemoveExpired(),
            this,
            options.CleanupInterval,
            options.CleanupInterval
        );
    }

    /// <summary>Entries currently held, including ones that expired but were not yet reclaimed.</summary>
    public int Count => _entries.Count;

    /// <summary>Approximate bytes held, when sizes are known.</summary>
    public long ApproximateBytes => Interlocked.Read(ref _approximateBytes);

    /// <summary>
    /// Reads <paramref name="key"/>. An expired entry, or one holding a value of another type,
    /// is a miss and is evicted.
    /// </summary>
    public bool TryGet<T>(string key, out T value)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_entries.TryGetValue(key, out var entry))
        {
            var now = _time.GetUtcNow().UtcTicks;
            if (entry.ExpiresAt > now)
            {
                if (entry.Value is T typed)
                {
                    Volatile.Write(ref entry.LastAccess, now);
                    value = typed;
                    return true;
                }
            }

            Evict(key, entry);
        }

        value = default!;
        return false;
    }

    /// <summary>
    /// Writes <paramref name="value"/> with an absolute <paramref name="timeToLive"/>. A
    /// non-positive time to live writes nothing.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    public void Set<T>(
        string key,
        T value,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tags = null,
        long? size = null
    )
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        timeToLive = ApplyJitter(timeToLive);
        if (timeToLive <= TimeSpan.Zero)
        {
            return;
        }

        var now = _time.GetUtcNow().UtcTicks;
        var entry = new Entry(value, now + timeToLive.Ticks, now, size ?? 0, tags);
        if (_entries.TryGetValue(key, out var previous))
        {
            Interlocked.Add(ref _approximateBytes, -previous.Size);
        }

        _entries[key] = entry;
        Interlocked.Add(ref _approximateBytes, entry.Size);
        EnforceBounds();
    }

    /// <summary>Writes <paramref name="value"/> only when <paramref name="key"/> is absent or expired.</summary>
    public bool SetIfAbsent<T>(
        string key,
        T value,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tags = null,
        long? size = null
    )
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        timeToLive = ApplyJitter(timeToLive);
        if (timeToLive <= TimeSpan.Zero)
        {
            return false;
        }

        var now = _time.GetUtcNow().UtcTicks;
        var entry = new Entry(value, now + timeToLive.Ticks, now, size ?? 0, tags);
        while (true)
        {
            if (_entries.TryAdd(key, entry))
            {
                Interlocked.Add(ref _approximateBytes, entry.Size);
                EnforceBounds();
                return true;
            }

            if (_entries.TryGetValue(key, out var existing))
            {
                if (existing.ExpiresAt > now)
                {
                    return false;
                }

                if (_entries.TryUpdate(key, entry, existing))
                {
                    Interlocked.Add(ref _approximateBytes, entry.Size - existing.Size);
                    EnforceBounds();
                    return true;
                }
            }
        }
    }

    /// <summary>Removes <paramref name="key"/>.</summary>
    public bool Remove(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_entries.TryRemove(key, out var entry))
        {
            Interlocked.Add(ref _approximateBytes, -entry.Size);
            return true;
        }

        return false;
    }

    /// <summary>Removes every key.</summary>
    public void Remove(IEnumerable<string> keys)
    {
        ArgumentNullException.ThrowIfNull(keys);
        foreach (var key in keys)
        {
            Remove(key);
        }
    }

    /// <summary>Removes every entry written with <paramref name="tag"/>.</summary>
    public void RemoveByTag(string tag)
    {
        ArgumentNullException.ThrowIfNull(tag);
        foreach (var (key, entry) in _entries)
        {
            if (entry.HasTag(tag))
            {
                Evict(key, entry);
            }
        }
    }

    /// <summary>Removes everything.</summary>
    public void Clear()
    {
        _entries.Clear();
        Interlocked.Exchange(ref _approximateBytes, 0);
    }

    /// <summary>Removes expired entries now, as the cleanup timer would.</summary>
    public void RemoveExpired()
    {
        var now = _time.GetUtcNow().UtcTicks;
        foreach (var (key, entry) in _entries)
        {
            if (entry.ExpiresAt <= now)
            {
                Evict(key, entry);
            }
        }
    }

    /// <inheritdoc />
    public void Dispose() => _cleanup?.Dispose();

    private TimeSpan ApplyJitter(TimeSpan timeToLive)
    {
        if (_options.ExpirationJitter <= TimeSpan.Zero || timeToLive <= TimeSpan.Zero)
        {
            return timeToLive;
        }

        // CA5394: jitter decorrelates expiry between instances; it is not a security boundary.
#pragma warning disable CA5394
        var jitter = TimeSpan.FromTicks(
            (long)(Random.Shared.NextDouble() * _options.ExpirationJitter.Ticks)
        );
#pragma warning restore CA5394
        var jittered = timeToLive - jitter;
        return jittered > TimeSpan.Zero ? jittered : TimeSpan.FromTicks(1);
    }

    private void Evict(string key, Entry expected)
    {
        if (_entries.TryRemove(new KeyValuePair<string, Entry>(key, expected)))
        {
            Interlocked.Add(ref _approximateBytes, -expected.Size);
        }
    }

    private void EnforceBounds()
    {
        var count = _entries.Count;
        var overBytes = _options.MaxBytes is { } maxBytes && ApproximateBytes > maxBytes;
        if (count <= _options.MaxEntries && !overBytes)
        {
            return;
        }

        if (Interlocked.CompareExchange(ref _evicting, 1, 0) != 0)
        {
            return;
        }

        try
        {
            if (count >= _options.MaxEntries * 1.5)
            {
                Clear();
                _logger.LogWarning(
                    new EventId(1101, "CacheL1Cleared"),
                    "In-process cache exceeded 150 % of Caching:L1:MaxEntries ({MaxEntries}) with {Count} entries and was cleared.",
                    _options.MaxEntries,
                    count
                );
                return;
            }

            RemoveExpired();
            EvictLeastRecentlyAccessed(overBytes);
        }
        finally
        {
            Volatile.Write(ref _evicting, 0);
        }
    }

    private void EvictLeastRecentlyAccessed(bool overBytes)
    {
        var target = (int)Math.Ceiling(_options.MaxEntries * _options.EvictionFraction);
        if (_entries.Count <= _options.MaxEntries && !overBytes)
        {
            return;
        }

        // Sample rather than sort the whole dictionary: a few times the eviction target is enough
        // to find old entries, and it keeps the cost of a write bounded under a full cache.
        var sampleSize = Math.Min(_entries.Count, Math.Max(target * 4, 64));
        var sample = new List<KeyValuePair<string, Entry>>(sampleSize);
        foreach (var pair in _entries)
        {
            sample.Add(pair);
            if (sample.Count >= sampleSize)
            {
                break;
            }
        }

        sample.Sort(
            static (left, right) =>
                Volatile
                    .Read(ref left.Value.LastAccess)
                    .CompareTo(Volatile.Read(ref right.Value.LastAccess))
        );
        var evicted = 0;
        foreach (var (key, entry) in sample)
        {
            if (evicted >= target && !(overBytes && ApproximateBytes > _options.MaxBytes))
            {
                break;
            }

            Evict(key, entry);
            evicted++;
        }
    }

    private sealed class Entry(
        object value,
        long expiresAt,
        long lastAccess,
        long size,
        IReadOnlyCollection<string>? tags
    )
    {
        public readonly object Value = value;
        public readonly long ExpiresAt = expiresAt;
        public readonly long Size = size;
        public long LastAccess = lastAccess;
        private readonly string[]? _tags = tags is { Count: > 0 } ? [.. tags] : null;

        public bool HasTag(string tag)
        {
            if (_tags is null)
            {
                return false;
            }

            foreach (var candidate in _tags)
            {
                if (string.Equals(candidate, tag, StringComparison.Ordinal))
                {
                    return true;
                }
            }

            return false;
        }
    }
}
