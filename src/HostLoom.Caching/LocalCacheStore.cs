using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.Caching;

/// <summary>
/// The in-process tier: a bounded, lock-free-on-read dictionary of typed values with absolute
/// expiry. It has no <c>IMemoryCache</c> dependency. A null value is stored only through
/// <see cref="SetNull"/>, as a remembered absence.
/// </summary>
/// <remarks>
/// Above <see cref="CacheL1Options.MaxEntries"/> a sampled least-recently-accessed
/// <see cref="CacheL1Options.EvictionFraction"/> is evicted; at 150 % of capacity everything is
/// cleared and a warning logged. A cleanup timer on the <see cref="TimeProvider"/> removes
/// expired entries every <see cref="CacheL1Options.CleanupInterval"/> and stops on dispose. An
/// entry written with a stale grace is kept, but not returned by <see cref="TryGet{T}"/>, for
/// that long after it expires; <see cref="TryGetWithinGrace{T}"/> is the read that accepts it.
/// </remarks>
public sealed class LocalCacheStore : IDisposable
{
    private static readonly object NullSentinel = new();

    private readonly ConcurrentDictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly CacheL1Options _options;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly ITimer? _cleanup;
    private readonly Lock _mutation = new();
    private long _approximateBytes;

    // Mutated with the entries under _mutation; avoids dictionary-wide Count locks per write.
    private int _count;
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
    public int Count => Volatile.Read(ref _count);

    /// <summary>Approximate bytes held, when sizes are known.</summary>
    public long ApproximateBytes => Interlocked.Read(ref _approximateBytes);

    /// <summary>
    /// Reads <paramref name="key"/>. An expired entry, or one holding a value of another type,
    /// is a miss and is evicted, except that an expired entry still inside its stale grace is
    /// kept for <see cref="TryGetWithinGrace{T}"/>. A remembered null is found with a null
    /// <paramref name="value"/> when <typeparamref name="T"/> can hold null, and is otherwise a
    /// miss that evicts.
    /// </summary>
    public bool TryGet<T>(string key, out T? value)
    {
        ArgumentNullException.ThrowIfNull(key);
        if (_entries.TryGetValue(key, out var entry))
        {
            var now = _time.GetUtcNow().UtcTicks;
            if (entry.ExpiresAt > now)
            {
                if (TryUnwrap(entry, now, out value))
                {
                    return true;
                }
            }
            else if (entry.StaleUntil > now)
            {
                value = default;
                return false;
            }

            Evict(key, entry);
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Reads <paramref name="key"/> accepting an expired entry inside its stale grace as well as
    /// a fresh one. <paramref name="stale"/> reports which it was.
    /// </summary>
    public bool TryGetWithinGrace<T>(string key, out T? value, out bool stale)
    {
        ArgumentNullException.ThrowIfNull(key);
        stale = false;
        if (_entries.TryGetValue(key, out var entry))
        {
            var now = _time.GetUtcNow().UtcTicks;
            if (entry.StaleUntil > now && TryUnwrap(entry, now, out value))
            {
                stale = entry.ExpiresAt <= now;
                return true;
            }

            Evict(key, entry);
        }

        value = default;
        return false;
    }

    /// <summary>
    /// Writes <paramref name="value"/> with an absolute <paramref name="timeToLive"/>. A
    /// non-positive time to live writes nothing.
    /// </summary>
    /// <param name="staleGrace">
    /// How long past expiry the entry stays readable through <see cref="TryGetWithinGrace{T}"/>.
    /// </param>
    /// <param name="notIfWrittenAfter">
    /// A <see cref="TimeProvider.GetTimestamp"/> value: when the key already holds an entry
    /// written after it, nothing is written. A value read from the distributed tier passes the
    /// timestamp its read started at, so it cannot replace what a later write put here.
    /// </param>
    /// <exception cref="ArgumentNullException"><paramref name="value"/> is null.</exception>
    public void Set<T>(
        string key,
        T value,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tags = null,
        long? size = null,
        TimeSpan? staleGrace = null,
        long? notIfWrittenAfter = null
    )
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentNullException.ThrowIfNull(value);
        Write(key, value, timeToLive, tags, size, staleGrace, notIfWrittenAfter);
    }

    /// <summary>
    /// Remembers that <paramref name="key"/> has no value, for <paramref name="timeToLive"/>, so a
    /// lookup finds a null instead of missing. A non-positive time to live writes nothing.
    /// </summary>
    public void SetNull(
        string key,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tags = null,
        TimeSpan? staleGrace = null,
        long? notIfWrittenAfter = null
    )
    {
        ArgumentNullException.ThrowIfNull(key);
        Write(key, NullSentinel, timeToLive, tags, size: 0, staleGrace, notIfWrittenAfter);
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
        var expiresAt = now + timeToLive.Ticks;
        var entry = new Entry(
            value,
            expiresAt,
            expiresAt,
            now,
            size ?? 0,
            tags,
            _time.GetTimestamp()
        );
        lock (_mutation)
        {
            if (_entries.TryGetValue(key, out var existing) && existing.ExpiresAt > now)
                return false;
            _entries[key] = entry;
            if (existing is null)
                Interlocked.Increment(ref _count);
            Interlocked.Add(ref _approximateBytes, entry.Size - (existing?.Size ?? 0));
        }
        EnforceBounds();
        return true;
    }

    /// <summary>Removes <paramref name="key"/>.</summary>
    public bool Remove(string key)
    {
        ArgumentNullException.ThrowIfNull(key);
        lock (_mutation)
        {
            if (_entries.TryRemove(key, out var entry))
            {
                Interlocked.Decrement(ref _count);
                Interlocked.Add(ref _approximateBytes, -entry.Size);
                return true;
            }
            return false;
        }
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
        lock (_mutation)
        {
            _entries.Clear();
            Interlocked.Exchange(ref _count, 0);
            Interlocked.Exchange(ref _approximateBytes, 0);
        }
    }

    /// <summary>
    /// Removes expired entries now, as the cleanup timer would. An entry inside its stale grace
    /// is kept until the grace ends.
    /// </summary>
    public void RemoveExpired()
    {
        var now = _time.GetUtcNow().UtcTicks;
        foreach (var (key, entry) in _entries)
        {
            if (entry.StaleUntil <= now)
            {
                Evict(key, entry);
            }
        }
    }

    /// <inheritdoc />
    public void Dispose() => _cleanup?.Dispose();

    private void Write(
        string key,
        object value,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tags,
        long? size,
        TimeSpan? staleGrace,
        long? notIfWrittenAfter = null
    )
    {
        timeToLive = ApplyJitter(timeToLive);
        if (timeToLive <= TimeSpan.Zero)
        {
            return;
        }

        var now = _time.GetUtcNow().UtcTicks;
        var expiresAt = now + timeToLive.Ticks;
        var staleUntil =
            staleGrace is { } grace && grace > TimeSpan.Zero ? expiresAt + grace.Ticks : expiresAt;
        var entry = new Entry(
            value,
            expiresAt,
            staleUntil,
            now,
            size ?? 0,
            tags,
            _time.GetTimestamp()
        );
        lock (_mutation)
        {
            _entries.TryGetValue(key, out var previous);
            if (
                notIfWrittenAfter is { } since
                && previous is not null
                && previous.WrittenAt > since
            )
            {
                // The distributed read that produced this value started before the entry was
                // written here, so the entry is the newer of the two.
                return;
            }

            _entries[key] = entry;
            if (previous is null)
                Interlocked.Increment(ref _count);
            Interlocked.Add(ref _approximateBytes, entry.Size - (previous?.Size ?? 0));
        }
        EnforceBounds();
    }

    private static bool TryUnwrap<T>(Entry entry, long now, out T? value)
    {
        if (ReferenceEquals(entry.Value, NullSentinel))
        {
            // A remembered absence can only be handed back as null; a non-nullable value type
            // has no null, so for it the entry is a value of another type.
            if (default(T) is null)
            {
                Volatile.Write(ref entry.LastAccess, now);
                value = default;
                return true;
            }
        }
        else if (entry.Value is T typed)
        {
            Volatile.Write(ref entry.LastAccess, now);
            value = typed;
            return true;
        }

        value = default;
        return false;
    }

    private TimeSpan ApplyJitter(TimeSpan timeToLive)
    {
        if (_options.ExpirationJitter <= TimeSpan.Zero || timeToLive <= TimeSpan.Zero)
        {
            return timeToLive;
        }

        // The jitter only decorrelates expiry between instances; it must never turn a short
        // time to live into no caching at all, so it takes at most half of the entry's life.
        var bound = Math.Min(_options.ExpirationJitter.Ticks, timeToLive.Ticks / 2);
        if (bound <= 0)
        {
            return timeToLive;
        }

        // CA5394: jitter decorrelates expiry between instances; it is not a security boundary.
#pragma warning disable CA5394
        var jitter = (long)(Random.Shared.NextDouble() * bound);
#pragma warning restore CA5394
        return TimeSpan.FromTicks(timeToLive.Ticks - jitter);
    }

    private void Evict(string key, Entry expected)
    {
        lock (_mutation)
        {
            if (_entries.TryRemove(new KeyValuePair<string, Entry>(key, expected)))
            {
                Interlocked.Decrement(ref _count);
                Interlocked.Add(ref _approximateBytes, -expected.Size);
            }
        }
    }

    private void EnforceBounds()
    {
        var count = Count;
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
        if (Count <= _options.MaxEntries && !overBytes)
        {
            return;
        }

        // Sample rather than sort the whole dictionary: a few times the eviction target is enough
        // to find old entries, and it keeps the cost of a write bounded under a full cache.
        var sampleSize = Math.Min(Count, Math.Max(target * 4, 64));
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
        long staleUntil,
        long lastAccess,
        long size,
        IReadOnlyCollection<string>? tags,
        long writtenAt
    )
    {
        public readonly object Value = value;
        public readonly long ExpiresAt = expiresAt;
        public readonly long StaleUntil = staleUntil;
        public readonly long Size = size;
        public readonly long WrittenAt = writtenAt;
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
