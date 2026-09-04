namespace HostLoom.Caching;

/// <summary>What a distributed store can do beyond get, set, and remove.</summary>
[Flags]
public enum CacheStoreCapabilities
{
    /// <summary>Get, set, set-if-absent, remove, and bulk operations only.</summary>
    None = 0,

    /// <summary>Tag indexes and <see cref="IDistributedCacheStore.RemoveByTagAsync"/>.</summary>
    Tags = 1,

    /// <summary>The store also offers an <see cref="ICacheInvalidationChannel"/>.</summary>
    InvalidationChannel = 2,

    /// <summary>The backend can push invalidations for keys a client has read.</summary>
    ServerAssistedTracking = 4,
}

/// <summary>One entry read from a distributed store.</summary>
/// <param name="Payload">The bytes exactly as they were written.</param>
/// <param name="RemainingTimeToLive">Time left before the backend expires the entry, when known.</param>
public readonly record struct CacheStoreEntry(
    ReadOnlyMemory<byte> Payload,
    TimeSpan? RemainingTimeToLive
);

/// <summary>
/// The backend contract behind the distributed tier. A store sees opaque, fully prefixed keys and
/// byte payloads; it never sees CLR types, namespaces, or serializers.
/// </summary>
/// <remarks>
/// <para>
/// Failures are reported as <see cref="CacheStoreException"/> carrying a
/// <see cref="CacheFailureKind"/>; cancellation propagates as
/// <see cref="OperationCanceledException"/>. The composed cache maps kinds to behaviour so a
/// consumer never sees a backend exception type.
/// </para>
/// <para>
/// Buffer ownership: memory passed to a write member is borrowed until the returned
/// <see cref="ValueTask"/> completes, after which the caller may reuse it; a store that retains
/// bytes longer copies them. Memory returned by a read member is not touched by the store again.
/// </para>
/// </remarks>
public interface IDistributedCacheStore
{
    /// <summary>What this store supports.</summary>
    CacheStoreCapabilities Capabilities { get; }

    /// <summary>Reads one entry, or null when absent.</summary>
    ValueTask<CacheStoreEntry?> GetAsync(string key, CancellationToken cancellationToken = default);

    /// <summary>Writes one entry with an absolute time to live and optional tag-index keys.</summary>
    /// <remarks>
    /// A tag index gains the key and never loses it before the index itself is removed, so
    /// rewriting an entry under different tags leaves the earlier memberships in place and
    /// <see cref="RemoveByTagAsync"/> may remove more entries than currently carry the tag. For a
    /// cache that costs a refill, never a wrong value.
    /// </remarks>
    ValueTask SetAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Writes one entry only when the key is absent; atomic in the backend. Tag indexes gain the
    /// key only when the write happened, and follow the same rule as <see cref="SetAsync"/>.
    /// </summary>
    ValueTask<bool> SetIfAbsentAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>Removes every key in one backend call.</summary>
    ValueTask RemoveAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    );

    /// <summary>Reads many keys in at most two round trips; absent keys are omitted.</summary>
    ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    );

    /// <summary>Writes many entries with one time to live in one backend call.</summary>
    ValueTask SetManyAsync(
        IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>> entries,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Removes every member of the tag index <paramref name="tagKey"/>, then the index itself.
    /// Membership is monotonic, so the members may include entries rewritten under other tags.
    /// </summary>
    ValueTask RemoveByTagAsync(string tagKey, CancellationToken cancellationToken = default);
}
