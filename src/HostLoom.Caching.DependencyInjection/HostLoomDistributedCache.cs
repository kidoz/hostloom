using System.Buffers;
using System.Buffers.Binary;
using System.Security.Cryptography;
using HostLoom.Caching.Internal;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.Logging;

namespace HostLoom.Caching.DependencyInjection;

/// <summary>
/// <see cref="IDistributedCache"/> and <see cref="IBufferDistributedCache"/> over the registered
/// <see cref="IDistributedCacheStore"/>, so <c>HybridCache</c> and other asynchronous Microsoft
/// consumers can use a HostLoom store. Entries live under <c>{namespace}:cache:external:{key}</c>,
/// apart from the tiered cache's own enveloped entries.
/// </summary>
/// <remarks>
/// The synchronous members throw <see cref="NotSupportedException"/>: the store contract is
/// asynchronous and blocking over it is exactly what the HostLoom analyzers forbid. Store failures
/// are fail-open: a read returns null, a write or explicit refresh returns, and a failed sliding
/// renewal returns the value from the completed read. Failures are counted on
/// <c>hostloom.cache.errors</c> and logged once per key per
/// <c>Caching:Diagnostics:DegradedLogInterval</c>. Keys follow the kernel's rules and payloads are
/// bounded by <c>Caching:MaxPayloadBytes</c>. With sliding expiration opted in, entries use a
/// versioned binary envelope under <c>{namespace}:cache:external-v2:{key}</c>; legacy raw entries
/// remain readable until a successful enveloped write retires them, and removal covers both
/// domains. Legacy entries cannot gain sliding behavior, and readers without the opt-in miss
/// enveloped entries.
/// </remarks>
internal sealed class HostLoomDistributedCache(
    IDistributedCacheStore store,
    CachingOptions options,
    TimeSpan defaultExpiration,
    TimeProvider time,
    ILogger<HostLoomDistributedCache> logger,
    bool enableSlidingExpiration = false
) : IDistributedCache, IBufferDistributedCache
{
    private const string NotSupported =
        "The HostLoom distributed cache adapter is asynchronous only; use the *Async member.";
    private const int HeaderLength = 40;
    private const byte FormatVersion = 1;
    private const byte AbsoluteExpirationFlag = 1;

    private readonly string _legacyPrefix = options.Namespace + ":cache:external:";
    private readonly string _envelopedPrefix = options.Namespace + ":cache:external-v2:";
    private readonly int _maxKeyLength = options.MaxKeyLength;
    private readonly long _maxPayloadBytes = options.MaxPayloadBytes;
    private readonly ISlidingExpirationCacheStore? _slidingStore =
        store as ISlidingExpirationCacheStore;
    private readonly DegradedLogThrottle _throttle = new(
        time,
        options.Diagnostics.DegradedLogInterval
    );

    /// <inheritdoc />
    public byte[]? Get(string key) => throw new NotSupportedException(NotSupported);

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    {
        CacheKey.Validate(key, _maxKeyLength, nameof(key));
        var entry = await ReadAsync(key, legacyFallback: true, renew: true, token)
            .ConfigureAwait(false);
        return entry?.Payload.ToArray();
    }

    /// <inheritdoc />
    public void Set(string key, byte[] value, DistributedCacheEntryOptions options) =>
        throw new NotSupportedException(NotSupported);

    /// <inheritdoc />
    public Task SetAsync(
        string key,
        byte[] value,
        DistributedCacheEntryOptions options,
        CancellationToken token = default
    )
    {
        ArgumentNullException.ThrowIfNull(value);
        return WriteAsync(key, value, options, token).AsTask();
    }

    /// <inheritdoc />
    public void Refresh(string key) => throw new NotSupportedException(NotSupported);

    /// <inheritdoc />
    public async Task RefreshAsync(string key, CancellationToken token = default)
    {
        CacheKey.Validate(key, _maxKeyLength, nameof(key));
        if (!enableSlidingExpiration)
        {
            return;
        }

        var entry = await ReadAsync(key, legacyFallback: false, renew: false, token)
            .ConfigureAwait(false);
        if (entry is { } found && found.SlidingWindow > TimeSpan.Zero)
        {
            await RenewAsync(key, found, token).ConfigureAwait(false);
        }
    }

    /// <inheritdoc />
    public void Remove(string key) => throw new NotSupportedException(NotSupported);

    /// <inheritdoc />
    public async Task RemoveAsync(string key, CancellationToken token = default)
    {
        CacheKey.Validate(key, _maxKeyLength, nameof(key));
        try
        {
            await store.RemoveAsync(KeyDomains(key), token).ConfigureAwait(false);
        }
        catch (Exception exception) when (!IsCallerCancellation(exception, token))
        {
            Degraded(exception, "remove", key);
        }
    }

    /// <inheritdoc />
    public bool TryGet(string key, IBufferWriter<byte> destination) =>
        throw new NotSupportedException(NotSupported);

    /// <inheritdoc />
    public async ValueTask<bool> TryGetAsync(
        string key,
        IBufferWriter<byte> destination,
        CancellationToken token = default
    )
    {
        CacheKey.Validate(key, _maxKeyLength, nameof(key));
        ArgumentNullException.ThrowIfNull(destination);
        var entry = await ReadAsync(key, legacyFallback: true, renew: true, token)
            .ConfigureAwait(false);
        if (entry is not { } found)
        {
            return false;
        }

        destination.Write(found.Payload.Span);
        return true;
    }

    /// <inheritdoc />
    public void Set(
        string key,
        ReadOnlySequence<byte> value,
        DistributedCacheEntryOptions options
    ) => throw new NotSupportedException(NotSupported);

    /// <inheritdoc />
    public ValueTask SetAsync(
        string key,
        ReadOnlySequence<byte> value,
        DistributedCacheEntryOptions options,
        CancellationToken token = default
    ) => WriteAsync(key, value.IsSingleSegment ? value.First : value.ToArray(), options, token);

    private async ValueTask<AdapterEntry?> ReadAsync(
        string key,
        bool legacyFallback,
        bool renew,
        CancellationToken token
    )
    {
        CacheStoreEntry? stored;
        var legacy = !enableSlidingExpiration;
        try
        {
            stored = await store.GetAsync(StorageKey(key, legacy), token).ConfigureAwait(false);
            if (stored is null && enableSlidingExpiration && legacyFallback)
            {
                legacy = true;
                stored = await store.GetAsync(StorageKey(key, legacy), token).ConfigureAwait(false);
            }
        }
        catch (Exception exception) when (!IsCallerCancellation(exception, token))
        {
            Degraded(exception, "read", key);
            return null;
        }

        if (stored is not { } found)
        {
            return null;
        }

        AdapterEntry entry;
        if (legacy)
        {
            entry = new AdapterEntry(found.Payload, TimeSpan.Zero, null, found.Payload);
        }
        else if (!TryDecode(found.Payload, out entry))
        {
            InvalidPayload(key);
            return null;
        }

        return renew && entry.SlidingWindow > TimeSpan.Zero
            ? await RenewAsync(key, entry, token).ConfigureAwait(false)
            : entry;
    }

    private async ValueTask<AdapterEntry?> RenewAsync(
        string key,
        AdapterEntry entry,
        CancellationToken token
    )
    {
        try
        {
            var outcome = await _slidingStore!
                .RefreshAsync(
                    StorageKey(key, legacy: false),
                    entry.StoredPayload,
                    entry.SlidingWindow,
                    entry.AbsoluteExpiration,
                    token
                )
                .ConfigureAwait(false);
            return outcome == CacheRefreshOutcome.Expired ? null : entry;
        }
        catch (Exception exception) when (!IsCallerCancellation(exception, token))
        {
            Degraded(exception, "refresh", key);
            return entry;
        }
    }

    private async ValueTask WriteAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        DistributedCacheEntryOptions options,
        CancellationToken token
    )
    {
        CacheKey.Validate(key, _maxKeyLength, nameof(key));
        ArgumentNullException.ThrowIfNull(options);
        try
        {
            if (!enableSlidingExpiration)
            {
                if (payload.Length > _maxPayloadBytes)
                {
                    TooLarge(key, payload.Length);
                    return;
                }

                var timeToLive = TimeToLive(options);
                if (timeToLive <= TimeSpan.Zero)
                {
                    await store.RemoveAsync(KeyDomains(key), token).ConfigureAwait(false);
                    return;
                }

                await store
                    .SetAsync(StorageKey(key, legacy: true), payload, timeToLive, null, token)
                    .ConfigureAwait(false);
                return;
            }

            var policy = SlidingPolicy(options);
            if (policy.TimeToLive <= TimeSpan.Zero)
            {
                await store.RemoveAsync(KeyDomains(key), token).ConfigureAwait(false);
                return;
            }

            var storedBytes = HeaderLength + (long)payload.Length;
            if (storedBytes > _maxPayloadBytes)
            {
                TooLarge(key, storedBytes);
                return;
            }

            var enveloped = Encode(payload, policy);
            await store
                .SetAsync(StorageKey(key, legacy: false), enveloped, policy.TimeToLive, null, token)
                .ConfigureAwait(false);
            await store.RemoveAsync([StorageKey(key, legacy: true)], token).ConfigureAwait(false);
        }
        catch (Exception exception)
            when (!IsCallerCancellation(exception, token) && exception is not NotSupportedException)
        {
            Degraded(exception, "write", key);
        }
    }

    private TimeSpan TimeToLive(DistributedCacheEntryOptions options)
    {
        if (options.SlidingExpiration is not null)
        {
            throw new NotSupportedException(
                "SlidingExpiration requires AddDistributedCacheAdapter(enableSlidingExpiration: true)."
            );
        }

        if (options.AbsoluteExpirationRelativeToNow is { } relative)
        {
            return relative;
        }

        if (options.AbsoluteExpiration is { } absolute)
        {
            return absolute - time.GetUtcNow();
        }

        return defaultExpiration;
    }

    private ExpirationPolicy SlidingPolicy(DistributedCacheEntryOptions options)
    {
        var sliding = options.SlidingExpiration ?? TimeSpan.Zero;
        DateTimeOffset? deadline = null;
        TimeSpan timeToLive;
        if (options.AbsoluteExpirationRelativeToNow is { } relative)
        {
            deadline = time.GetUtcNow() + relative;
            timeToLive = relative;
        }
        else if (options.AbsoluteExpiration is { } absolute)
        {
            deadline = absolute;
            timeToLive = absolute - time.GetUtcNow();
        }
        else if (sliding > TimeSpan.Zero)
        {
            timeToLive = sliding;
        }
        else
        {
            deadline = time.GetUtcNow() + defaultExpiration;
            timeToLive = defaultExpiration;
        }

        if (sliding > TimeSpan.Zero && (timeToLive <= TimeSpan.Zero || sliding < timeToLive))
        {
            timeToLive = sliding;
        }

        return new ExpirationPolicy(timeToLive, sliding, deadline);
    }

    private string[] KeyDomains(string key) =>
        enableSlidingExpiration
            ? [StorageKey(key, legacy: false), StorageKey(key, legacy: true)]
            : [StorageKey(key, legacy: true)];

    private string StorageKey(string key, bool legacy) =>
        (legacy ? _legacyPrefix : _envelopedPrefix) + key;

    private static byte[] Encode(ReadOnlyMemory<byte> payload, ExpirationPolicy policy)
    {
        var stored = new byte[HeaderLength + payload.Length];
        var span = stored.AsSpan();
        span[0] = (byte)'H';
        span[1] = (byte)'L';
        span[2] = (byte)'D';
        span[3] = (byte)'C';
        span[4] = FormatVersion;
        span[5] = policy.AbsoluteExpiration is null ? (byte)0 : AbsoluteExpirationFlag;
        BinaryPrimitives.WriteInt64LittleEndian(span[8..], policy.SlidingWindow.Ticks);
        BinaryPrimitives.WriteInt64LittleEndian(
            span[16..],
            policy.AbsoluteExpiration?.UtcTicks ?? 0
        );
        RandomNumberGenerator.Fill(span[24..HeaderLength]);
        payload.Span.CopyTo(span[HeaderLength..]);
        return stored;
    }

    private static bool TryDecode(ReadOnlyMemory<byte> stored, out AdapterEntry entry)
    {
        entry = default;
        var span = stored.Span;
        if (
            span.Length < HeaderLength
            || span[0] != (byte)'H'
            || span[1] != (byte)'L'
            || span[2] != (byte)'D'
            || span[3] != (byte)'C'
            || span[4] != FormatVersion
            || (span[5] & ~AbsoluteExpirationFlag) != 0
            || span[6] != 0
            || span[7] != 0
        )
        {
            return false;
        }

        var slidingTicks = BinaryPrimitives.ReadInt64LittleEndian(span[8..]);
        var deadlineTicks = BinaryPrimitives.ReadInt64LittleEndian(span[16..]);
        var hasDeadline = (span[5] & AbsoluteExpirationFlag) != 0;
        if (
            slidingTicks < 0
            || deadlineTicks < 0
            || deadlineTicks > DateTimeOffset.MaxValue.UtcTicks
            || (!hasDeadline && deadlineTicks != 0)
        )
        {
            return false;
        }

        entry = new AdapterEntry(
            stored[HeaderLength..],
            TimeSpan.FromTicks(slidingTicks),
            hasDeadline ? new DateTimeOffset(deadlineTicks, TimeSpan.Zero) : null,
            stored
        );
        return true;
    }

    private void TooLarge(string key, long bytes)
    {
        CachingDiagnostics.Errors.Add(
            1,
            new KeyValuePair<string, object?>(CachingDiagnostics.NamespaceTag, options.Namespace),
            new KeyValuePair<string, object?>(CachingDiagnostics.KindTag, "payload")
        );
        if (!_throttle.ShouldLog(key))
        {
            return;
        }

        logger.LogError(
            new EventId(1008, "DistributedCacheAdapterPayloadTooLarge"),
            "Payload for '{Key}' in namespace '{Namespace}' is {Bytes} bytes, above Caching:MaxPayloadBytes ({MaxPayloadBytes}); it was not written. Further errors for this key are suppressed for {Interval}.",
            key,
            options.Namespace,
            bytes,
            _maxPayloadBytes,
            options.Diagnostics.DegradedLogInterval
        );
    }

    private void InvalidPayload(string key)
    {
        CachingDiagnostics.Errors.Add(
            1,
            new KeyValuePair<string, object?>(CachingDiagnostics.NamespaceTag, options.Namespace),
            new KeyValuePair<string, object?>(CachingDiagnostics.KindTag, "payload")
        );
        if (!_throttle.ShouldLog(key))
        {
            return;
        }

        logger.LogError(
            new EventId(1009, "DistributedCacheAdapterPayloadInvalid"),
            "The enveloped payload for '{Key}' in namespace '{Namespace}' is malformed or has an unsupported version; the adapter answered as a miss. Further errors for this key are suppressed for {Interval}.",
            key,
            options.Namespace,
            options.Diagnostics.DegradedLogInterval
        );
    }

    private void Degraded(Exception exception, string operation, string key)
    {
        var kind = exception is CacheStoreException store ? store.Kind : CacheFailureKind.Other;
        CachingDiagnostics.RecordStoreFailure(options.Namespace, kind);
        if (!_throttle.ShouldLog(key))
        {
            return;
        }

        logger.LogWarning(
            new EventId(1007, "DistributedCacheAdapterDegraded"),
            exception,
            "The distributed cache store failed ({Kind}) during {Operation} of '{Key}' in namespace '{Namespace}'; the adapter used its fail-open outcome. Further warnings for this key are suppressed for {Interval}.",
            kind,
            operation,
            key,
            options.Namespace,
            options.Diagnostics.DegradedLogInterval
        );
    }

    private static bool IsCallerCancellation(Exception exception, CancellationToken token) =>
        exception is OperationCanceledException && token.IsCancellationRequested;

    private readonly record struct ExpirationPolicy(
        TimeSpan TimeToLive,
        TimeSpan SlidingWindow,
        DateTimeOffset? AbsoluteExpiration
    );

    private readonly record struct AdapterEntry(
        ReadOnlyMemory<byte> Payload,
        TimeSpan SlidingWindow,
        DateTimeOffset? AbsoluteExpiration,
        ReadOnlyMemory<byte> StoredPayload
    );
}
