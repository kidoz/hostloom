using Microsoft.Extensions.Logging;

namespace HostLoom.Caching.Internal;

/// <summary>
/// The in-process tier kept coherent with invalidations. An operation captures generations before
/// it starts work that can overlap an invalidation, and inserts only through this type, which
/// compares the capture and inserts under the same lock that an invalidation holds while it moves
/// generations and evicts. An overlapping invalidation therefore either keeps the operation's
/// value out of the tier or evicts it afterwards; nothing inserts without the comparison.
/// </summary>
/// <remarks>
/// <para>
/// A generation per key stripe conservatively rejects every single-key fill overlapping an
/// invalidation of that key, and a generation per tag stripe rejects every fill declaring a tag
/// invalidated meanwhile. A fill is compared against its key stripe and one stripe per tag it
/// declares, so an untagged fill ignores tag-only invalidations and a fill on another tag or key
/// stripe proceeds. That relies on every writer of a key declaring the same tags.
/// </para>
/// <para>
/// A distributed read learns its tags from the payload only after the read, so its promotion is
/// compared against its key stripe and, for a tagged payload, the single tag generation that any
/// tag message moves. Bulk reads and warmup batches are compared against the whole generation,
/// which every invalidation moves. A flush moves every stripe of both kinds.
/// </para>
/// </remarks>
internal sealed class CoherentLocalTier : IDisposable
{
    private const int GenerationStripes = 1024;

    private readonly LocalCacheStore? _store;
    private readonly Lock _gate = new();
    private readonly long[] _generations = new long[GenerationStripes];
    private readonly long[] _tagGenerations = new long[GenerationStripes];
    private long _wholeGeneration;
    private long _tagGeneration;

    /// <summary>
    /// Creates the tier. With <see cref="CacheL1Options.Enabled"/> off it holds nothing but still
    /// tracks generations, so a write overlapped by an invalidation is still abandoned.
    /// </summary>
    /// <param name="options">The in-process tier's bounds.</param>
    /// <param name="time">Clock for expiry.</param>
    /// <param name="logger">Where the tier reports its capacity clear.</param>
    public CoherentLocalTier(CacheL1Options options, TimeProvider time, ILogger logger)
    {
        ArgumentNullException.ThrowIfNull(options);
        // The composing cache's maintenance timer owns the expired-entry sweep.
        _store = options.Enabled
            ? new LocalCacheStore(options, time, logger, sweepsExpired: false)
            : null;
    }

    /// <summary>Whether the tier holds entries at all.</summary>
    public bool Enabled => _store is not null;

    /// <summary>Entries held, including expired ones not yet swept.</summary>
    public long Count => _store?.Count ?? 0;

    /// <summary>The generation stripe of <paramref name="key"/>.</summary>
    public static int StripeOf(string key) =>
        (int)((uint)string.GetHashCode(key, StringComparison.Ordinal) % GenerationStripes);

    /// <summary>The tag generation stripe of <paramref name="tag"/>.</summary>
    public static int TagStripeOf(string tag) =>
        (int)((uint)string.GetHashCode(tag, StringComparison.Ordinal) % GenerationStripes);

    /// <summary>Reads <paramref name="key"/>; see <see cref="LocalCacheStore.TryGet{T}"/>.</summary>
    public bool TryGet<T>(string key, out T? value)
    {
        if (_store is null)
        {
            value = default;
            return false;
        }

        return _store.TryGet(key, out value);
    }

    /// <summary>Reads <paramref name="key"/> accepting a stale copy; see <see cref="LocalCacheStore.TryGetWithinGrace{T}"/>.</summary>
    public bool TryGetWithinGrace<T>(string key, out T? value, out bool stale)
    {
        if (_store is null)
        {
            value = default;
            stale = false;
            return false;
        }

        return _store.TryGetWithinGrace(key, out value, out stale);
    }

    /// <summary>
    /// Captures what a fill or write of <paramref name="key"/> declaring <paramref name="tags"/>
    /// is compared against: its key stripe and one tag stripe per declared tag.
    /// </summary>
    public FillCapture Capture(string key, IReadOnlyCollection<string>? tags)
    {
        var keyStripe = StripeOf(key);
        var keyGeneration = Volatile.Read(ref _generations[keyStripe]);
        if (tags is not { Count: > 0 })
        {
            return new FillCapture(keyStripe, keyGeneration, null);
        }

        var tagGenerations = new TagGeneration[tags.Count];
        var index = 0;
        foreach (var tag in tags)
        {
            var stripe = TagStripeOf(tag);
            tagGenerations[index++] = new TagGeneration(
                stripe,
                Volatile.Read(ref _tagGenerations[stripe])
            );
        }

        return new FillCapture(keyStripe, keyGeneration, tagGenerations);
    }

    /// <summary>
    /// Captures what the promotion of a distributed read of <paramref name="key"/> is compared
    /// against: its key stripe and the tag generation.
    /// </summary>
    public ReadCapture CaptureRead(string key)
    {
        var stripe = StripeOf(key);
        return new ReadCapture(
            stripe,
            Volatile.Read(ref _generations[stripe]),
            Interlocked.Read(ref _tagGeneration)
        );
    }

    /// <summary>Captures the whole generation, which a bulk read or a warmup batch is compared against.</summary>
    public ReadCapture CaptureBulk() => new(Interlocked.Read(ref _wholeGeneration));

    /// <summary>
    /// Whether no invalidation has moved a generation <paramref name="captured"/> holds. Read
    /// without the lock, so a write overlapped by an invalidation can be abandoned before it
    /// reaches the distributed tier; a commit compares again under the lock.
    /// </summary>
    public bool IsCurrent(in FillCapture captured)
    {
        if (captured.Key != Volatile.Read(ref _generations[captured.KeyStripe]))
        {
            return false;
        }

        if (captured.Tags is null)
        {
            return true;
        }

        foreach (var (stripe, generation) in captured.Tags)
        {
            if (generation != Volatile.Read(ref _tagGenerations[stripe]))
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>Inserts a written or computed value when <paramref name="captured"/> is still current.</summary>
    /// <returns>False when an invalidation overlapped and nothing was inserted.</returns>
    public bool TryCommit<T>(
        in FillCapture captured,
        string key,
        T value,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tags,
        long? size,
        TimeSpan? staleGrace
    )
    {
        lock (_gate)
        {
            if (!IsCurrent(captured))
            {
                return false;
            }

            _store?.Set(key, value, timeToLive, tags, size, staleGrace);
            return true;
        }
    }

    /// <summary>Remembers a null factory result when <paramref name="captured"/> is still current.</summary>
    /// <returns>False when an invalidation overlapped and nothing was inserted.</returns>
    public bool TryCommitNull(
        in FillCapture captured,
        string key,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tags,
        TimeSpan? staleGrace
    )
    {
        lock (_gate)
        {
            if (!IsCurrent(captured))
            {
                return false;
            }

            _store?.SetNull(key, timeToLive, tags, staleGrace);
            return true;
        }
    }

    /// <summary>
    /// Promotes a value a distributed read returned when <paramref name="captured"/> is still
    /// current for a payload carrying <paramref name="tags"/>. An entry written here after
    /// <paramref name="readStart"/> is newer than the read and is kept.
    /// </summary>
    /// <returns>False when an invalidation overlapped and nothing was inserted.</returns>
    public bool TryPromote<T>(
        in ReadCapture captured,
        string key,
        string[]? tags,
        T value,
        TimeSpan timeToLive,
        long size,
        TimeSpan? staleGrace,
        long readStart
    )
    {
        lock (_gate)
        {
            if (!MayPromote(captured, tags))
            {
                return false;
            }

            _store?.Set(
                key,
                value,
                timeToLive,
                tags,
                size,
                staleGrace,
                notIfWrittenAfter: readStart
            );
            return true;
        }
    }

    /// <summary>Promotes a remembered absence a distributed read returned; see <see cref="TryPromote{T}"/>.</summary>
    /// <returns>False when an invalidation overlapped and nothing was inserted.</returns>
    public bool TryPromoteNull(
        in ReadCapture captured,
        string key,
        string[]? tags,
        TimeSpan timeToLive,
        TimeSpan? staleGrace,
        long readStart
    )
    {
        lock (_gate)
        {
            if (!MayPromote(captured, tags))
            {
                return false;
            }

            _store?.SetNull(key, timeToLive, tags, staleGrace, notIfWrittenAfter: readStart);
            return true;
        }
    }

    /// <summary>Inserts a warmup batch when no invalidation has moved the whole generation since <paramref name="captured"/>.</summary>
    /// <returns>False when an invalidation overlapped and nothing was inserted.</returns>
    public bool TryCommitBatch<T>(
        in ReadCapture captured,
        IReadOnlyList<KeyValuePair<string, T>> batch,
        TimeSpan expiration
    )
    {
        lock (_gate)
        {
            if (captured.Whole != _wholeGeneration)
            {
                return false;
            }

            if (_store is not null)
            {
                foreach (var (key, value) in batch)
                {
                    _store.Set(key, value!, expiration);
                }
            }

            return true;
        }
    }

    /// <summary>
    /// Inserts <paramref name="value"/> unless a live entry holds <paramref name="key"/>, for a
    /// cache without a distributed tier. Nothing is awaited between deciding and inserting, so
    /// the insert only has to be atomic with evictions.
    /// </summary>
    public bool SetIfAbsent<T>(
        string key,
        T value,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tags,
        long? size
    )
    {
        lock (_gate)
        {
            return _store!.SetIfAbsent(key, value, timeToLive, tags, size);
        }
    }

    /// <summary>
    /// Applies an invalidation: moves the generations it covers, so overlapping operations do
    /// not insert, and evicts what it names, atomically with respect to every insert.
    /// </summary>
    public void Apply(CacheInvalidation invalidation)
    {
        lock (_gate)
        {
            Interlocked.Increment(ref _wholeGeneration);
            if (invalidation.FlushAll)
            {
                // A flush covers every key and every tag.
                Interlocked.Increment(ref _tagGeneration);
                for (var stripe = 0; stripe < GenerationStripes; stripe++)
                {
                    _generations[stripe]++;
                    _tagGenerations[stripe]++;
                }

                _store?.Clear();
                return;
            }

            foreach (var key in invalidation.Keys)
            {
                _generations[StripeOf(key)]++;
            }

            if (invalidation.Tags.Count > 0)
            {
                // The keys a tag covers are not known here; a fill declaring the tag notices its
                // stripe move, and a distributed read notices the tag generation move.
                Interlocked.Increment(ref _tagGeneration);
                foreach (var tag in invalidation.Tags)
                {
                    _tagGenerations[TagStripeOf(tag)]++;
                }
            }

            _store?.Remove(invalidation.Keys);
            foreach (var tag in invalidation.Tags)
            {
                _store?.RemoveByTag(tag);
            }
        }
    }

    /// <summary>Sweeps expired entries; the composing cache calls it from its maintenance timer.</summary>
    public void RemoveExpired() => _store?.RemoveExpired();

    /// <inheritdoc />
    public void Dispose() => _store?.Dispose();

    /// <summary>Read under the lock: whether a read that captured <paramref name="captured"/> may promote a payload carrying <paramref name="tags"/>.</summary>
    private bool MayPromote(in ReadCapture captured, string[]? tags) =>
        captured.IsBulk
            ? captured.Whole == _wholeGeneration
            : captured.Key == _generations[captured.KeyStripe]
                && (tags is not { Length: > 0 } || captured.Tag == _tagGeneration);
}

/// <summary>
/// The generations a single-key fill or write started against: its key stripe and one tag
/// stripe per tag it declares. Any of them moving means an invalidation overlapped it.
/// </summary>
internal readonly struct FillCapture(int keyStripe, long key, TagGeneration[]? tags)
{
    public int KeyStripe { get; } = keyStripe;

    public long Key { get; } = key;

    public TagGeneration[]? Tags { get; } = tags;
}

/// <summary>One declared tag's stripe and the generation it had when captured.</summary>
internal readonly record struct TagGeneration(int Stripe, long Generation);

/// <summary>
/// What a distributed read captured before it started: its key stripe and the tag generation
/// for a single key, or the whole generation for a bulk read or a warmup batch.
/// </summary>
internal readonly struct ReadCapture
{
    public ReadCapture(int keyStripe, long key, long tag)
    {
        KeyStripe = keyStripe;
        Key = key;
        Tag = tag;
    }

    public ReadCapture(long whole)
    {
        Whole = whole;
        IsBulk = true;
    }

    public int KeyStripe { get; }

    public long Key { get; }

    public long Tag { get; }

    public long Whole { get; }

    public bool IsBulk { get; }
}
