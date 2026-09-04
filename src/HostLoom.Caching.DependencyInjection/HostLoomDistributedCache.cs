using System.Buffers;
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
/// asynchronous and blocking over it is exactly what the HostLoom analyzers forbid.
/// <c>RefreshAsync</c> completes as a no-op because the store has no touch operation, so the
/// adapter offers no sliding expiration. Store failures are fail-open here as everywhere: a read
/// returns null and a write returns, each counted on <c>hostloom.cache.errors</c> and logged once
/// per key per <c>Caching:Diagnostics:DegradedLogInterval</c>, because an outage otherwise turns
/// every request into a log line.
/// </remarks>
internal sealed class HostLoomDistributedCache(
    IDistributedCacheStore store,
    CachingOptions options,
    TimeSpan defaultExpiration,
    TimeProvider time,
    ILogger<HostLoomDistributedCache> logger
) : IDistributedCache, IBufferDistributedCache
{
    private const string NotSupported =
        "The HostLoom distributed cache adapter is asynchronous only; use the *Async member.";

    private readonly string _prefix = options.Namespace + ":cache:external:";
    private readonly DegradedLogThrottle _throttle = new(
        time,
        options.Diagnostics.DegradedLogInterval
    );

    /// <inheritdoc />
    public byte[]? Get(string key) => throw new NotSupportedException(NotSupported);

    /// <inheritdoc />
    public async Task<byte[]?> GetAsync(string key, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        try
        {
            var entry = await store.GetAsync(_prefix + key, token).ConfigureAwait(false);
            return entry?.Payload.ToArray();
        }
        catch (Exception exception) when (!IsCallerCancellation(exception, token))
        {
            Degraded(exception, "read", key);
            return null;
        }
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
    public Task RefreshAsync(string key, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        // The store contract has no touch operation, so there is no sliding window to reset.
        return Task.CompletedTask;
    }

    /// <inheritdoc />
    public void Remove(string key) => throw new NotSupportedException(NotSupported);

    /// <inheritdoc />
    public async Task RemoveAsync(string key, CancellationToken token = default)
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        try
        {
            await store.RemoveAsync([_prefix + key], token).ConfigureAwait(false);
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
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(destination);
        try
        {
            var entry = await store.GetAsync(_prefix + key, token).ConfigureAwait(false);
            if (entry is not { } found)
            {
                return false;
            }

            destination.Write(found.Payload.Span);
            return true;
        }
        catch (Exception exception) when (!IsCallerCancellation(exception, token))
        {
            Degraded(exception, "read", key);
            return false;
        }
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

    private async ValueTask WriteAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        DistributedCacheEntryOptions options,
        CancellationToken token
    )
    {
        ArgumentException.ThrowIfNullOrEmpty(key);
        ArgumentNullException.ThrowIfNull(options);
        var timeToLive = TimeToLive(options);
        try
        {
            if (timeToLive <= TimeSpan.Zero)
            {
                await store.RemoveAsync([_prefix + key], token).ConfigureAwait(false);
                return;
            }

            await store
                .SetAsync(_prefix + key, payload, timeToLive, null, token)
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (!IsCallerCancellation(exception, token))
        {
            Degraded(exception, "write", key);
        }
    }

    private TimeSpan TimeToLive(DistributedCacheEntryOptions options)
    {
        if (options.AbsoluteExpirationRelativeToNow is { } relative)
        {
            return relative;
        }

        if (options.AbsoluteExpiration is { } absolute)
        {
            return absolute - time.GetUtcNow();
        }

        // Sliding expiration cannot be honoured without a touch operation; the window becomes an
        // absolute time to live so the entry still expires.
        return options.SlidingExpiration ?? defaultExpiration;
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
            "The distributed cache store failed ({Kind}) during {Operation} of '{Key}' in namespace '{Namespace}'; the adapter answered as a miss. Further warnings for this key are suppressed for {Interval}.",
            kind,
            operation,
            key,
            options.Namespace,
            options.Diagnostics.DegradedLogInterval
        );
    }

    private static bool IsCallerCancellation(Exception exception, CancellationToken token) =>
        exception is OperationCanceledException && token.IsCancellationRequested;
}
