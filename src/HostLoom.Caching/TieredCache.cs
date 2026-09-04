using System.Diagnostics;
using System.Threading.Channels;
using HostLoom.Caching.Internal;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;

namespace HostLoom.Caching;

/// <summary>
/// The composed cache: an in-process tier, an optional distributed tier, per-key single-flight,
/// a best-effort cluster-wide lease, cross-instance invalidation, and fail-open behaviour.
/// </summary>
/// <remarks>
/// Constructible without a container. Every collaborator is optional except the options: with no
/// store the cache is in-process only; with a store a serializer is required; a store that also
/// implements <see cref="ICacheInvalidationChannel"/> is subscribed automatically.
/// </remarks>
public sealed class TieredCache : ICache, IAsyncDisposable
{
    private const string LeaseSegment = ":cache:lease:";
    private const string DataSegment = ":cache:data:";
    private const string TagSegment = ":cache:tag:";

    /// <summary>Throttle key for the invalidation queue; a validated cache key never has a space.</summary>
    private const string InvalidationThrottleKey = "invalidation queue";
    private static readonly ReadOnlyMemory<byte> LeasePayload = new byte[] { 1 };

    private readonly CachingOptions _options;
    private readonly IDistributedCacheStore? _store;
    private readonly ICacheValueSerializer? _serializer;
    private readonly ICacheInvalidationChannel? _channel;
    private readonly TimeProvider _time;
    private readonly ILogger _logger;
    private readonly LocalCacheStore? _local;
    private readonly KeyedAsyncGuard _guard;
    private readonly DegradedLogThrottle _throttle;
    private readonly string _dataPrefix;
    private readonly string _leasePrefix;
    private readonly string _tagPrefix;
    private readonly string _versionSuffix;
    private readonly KeyValuePair<string, object?> _namespaceTag;
    private readonly IDisposable? _subscription;
    private readonly Channel<CacheInvalidation>? _pending;
    private readonly Task? _invalidationLoop;
    private readonly CancellationTokenSource _disposal = new();
    private readonly ITimer _maintenance;
    private int _disposed;

    /// <summary>Composes a cache.</summary>
    /// <param name="options">Validated with <see cref="CachingOptions.Validate"/>.</param>
    /// <param name="store">The distributed tier, or null for an in-process-only cache.</param>
    /// <param name="serializer">Required when <paramref name="store"/> is given.</param>
    /// <param name="channel">
    /// Invalidation fan-out. When null and <paramref name="store"/> implements
    /// <see cref="ICacheInvalidationChannel"/>, the store's channel is used.
    /// </param>
    /// <param name="timeProvider">Clock for expiry, leases, and timers; the system clock when null.</param>
    /// <param name="logger">Where degraded paths are reported; a null logger when absent.</param>
    public TieredCache(
        CachingOptions options,
        IDistributedCacheStore? store = null,
        ICacheValueSerializer? serializer = null,
        ICacheInvalidationChannel? channel = null,
        TimeProvider? timeProvider = null,
        ILogger<TieredCache>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        options.ThrowIfInvalid(nameof(options));
        if (store is not null && serializer is null)
        {
            throw new ArgumentException(
                "A distributed store needs an ICacheValueSerializer to turn values into payloads.",
                nameof(serializer)
            );
        }

        if (store is null && !options.L1.Enabled)
        {
            throw new ArgumentException(
                "Caching:L1:Enabled is false and there is no distributed store, so nothing would be cached.",
                nameof(options)
            );
        }

        _options = options;
        _store = store;
        _serializer = serializer;
        _channel = channel ?? store as ICacheInvalidationChannel;
        _time = timeProvider ?? TimeProvider.System;
        _logger = logger ?? NullLogger<TieredCache>.Instance;
        _local = options.L1.Enabled ? new LocalCacheStore(options.L1, _time) : null;
        _guard = new KeyedAsyncGuard(_time);
        _throttle = new DegradedLogThrottle(_time, options.Diagnostics.DegradedLogInterval);
        _dataPrefix = options.Namespace + DataSegment;
        _leasePrefix = options.Namespace + LeaseSegment;
        _tagPrefix = options.Namespace + TagSegment;
        _versionSuffix = options.PayloadVersion is { } version ? ":" + version : "";
        _namespaceTag = new KeyValuePair<string, object?>(
            CachingDiagnostics.NamespaceTag,
            options.Namespace
        );

        if (_channel is not null && _local is not null)
        {
            _pending = Channel.CreateBounded<CacheInvalidation>(
                new BoundedChannelOptions(options.Invalidation.MaxPending)
                {
                    FullMode = BoundedChannelFullMode.DropWrite,
                    SingleReader = true,
                },
                // DropWrite discards the item and still reports the write as successful, so this
                // callback is the only place the loss is observable.
                itemDropped: _ => ReportDroppedInvalidation()
            );
            _invalidationLoop = Task.Run(() => ApplyInvalidationsAsync(_disposal.Token));
            _subscription = _channel.Subscribe(OnInvalidation);
        }

        _maintenance = _time.CreateTimer(
            static state => ((TieredCache)state!).Maintain(),
            this,
            options.L1.CleanupInterval,
            options.L1.CleanupInterval
        );
        CachingDiagnostics.Register(this);
    }

    /// <summary>The key prefix.</summary>
    public string Namespace => _options.Namespace;

    internal long LocalEntryCount => _local?.Count ?? 0;

    internal long ActiveGuardCount => _guard.ActiveCount;

    /// <inheritdoc />
    public ValueTask<T?> GetOrCreateAsync<T>(
        string key,
        Func<CancellationToken, ValueTask<T>> factory,
        CacheEntryOptions options,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(factory);
        return GetOrCreateAsync(
            key,
            factory,
            static (state, token) => state(token),
            options,
            cancellationToken
        );
    }

    /// <inheritdoc />
    public ValueTask<T?> GetOrCreateAsync<T>(
        string key,
        Func<CancellationToken, ValueTask<T>> factory,
        TimeSpan expiration,
        CancellationToken cancellationToken = default
    ) => GetOrCreateAsync(key, factory, new CacheEntryOptions(expiration), cancellationToken);

    /// <inheritdoc />
    public ValueTask<T?> GetOrCreateAsync<TState, T>(
        string key,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        CacheEntryOptions options,
        CancellationToken cancellationToken = default
    )
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(factory);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate(nameof(options));
        ThrowIfDisposed();

        var start = Stopwatch.GetTimestamp();
        if (_local is not null && _local.TryGet<T>(key, out var hit))
        {
            RecordOperation("get_or_create", "hit_l1", start);
            return new ValueTask<T?>(hit);
        }

        return GetOrCreateSlowAsync(key, state, factory, options, start, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask<T?> GetAsync<T>(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        var lookup = await TryGetAsync<T>(key, cancellationToken).ConfigureAwait(false);
        return lookup.Value;
    }

    /// <inheritdoc />
    public ValueTask<CacheLookup<T>> TryGetAsync<T>(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        ValidateKey(key);
        ThrowIfDisposed();
        var start = Stopwatch.GetTimestamp();
        if (_local is not null && _local.TryGet<T>(key, out var hit))
        {
            RecordOperation("get", "hit_l1", start);
            return new ValueTask<CacheLookup<T>>(CacheLookup.Hit(hit, CacheTier.L1));
        }

        if (_store is null)
        {
            RecordOperation("get", "miss", start);
            return new ValueTask<CacheLookup<T>>(CacheLookup.Miss<T>());
        }

        return GetSlowAsync<T>(key, start, cancellationToken);
    }

    /// <inheritdoc />
    public async ValueTask SetAsync<T>(
        string key,
        T value,
        CacheEntryOptions options,
        CancellationToken cancellationToken = default
    )
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate(nameof(options));
        ThrowIfDisposed();
        if (options.Expiration <= TimeSpan.Zero)
        {
            return;
        }

        using var activity = StartActivity("cache.set", key);
        var start = Stopwatch.GetTimestamp();
        var degraded = await WriteAsync(key, value, options, cancellationToken)
            .ConfigureAwait(false);
        activity?.SetTag("hostloom.cache.degraded", degraded);
        RecordOperation("set", degraded ? "degraded" : "miss", start);
    }

    /// <inheritdoc />
    public ValueTask<bool> SetIfAbsentAsync<T>(
        string key,
        T value,
        TimeSpan expiration,
        CancellationToken cancellationToken = default
    ) => SetIfAbsentAsync(key, value, new CacheEntryOptions(expiration), cancellationToken);

    /// <inheritdoc />
    public async ValueTask<bool> SetIfAbsentAsync<T>(
        string key,
        T value,
        CacheEntryOptions options,
        CancellationToken cancellationToken = default
    )
    {
        ValidateKey(key);
        ArgumentNullException.ThrowIfNull(value);
        ArgumentNullException.ThrowIfNull(options);
        options.Validate(nameof(options));
        ThrowIfDisposed();
        if (options.Expiration <= TimeSpan.Zero)
        {
            return false;
        }

        var start = Stopwatch.GetTimestamp();
        if (_store is null)
        {
            var added = _local!.SetIfAbsent(
                key,
                value,
                options.Expiration,
                options.Tags,
                options.Size
            );
            RecordOperation("set_if_absent", added ? "miss" : "hit_l1", start);
            return added;
        }

        using var payload = Encode(key, value, options, out var compressed);
        if (payload is null)
        {
            RecordOperation("set_if_absent", "error", start);
            return false;
        }

        bool written;
        try
        {
            written = await _store
                .SetIfAbsentAsync(
                    DataKey(key),
                    payload.WrittenMemory,
                    options.Expiration,
                    TagKeys(options.Tags),
                    cancellationToken
                )
                .ConfigureAwait(false);
        }
        catch (Exception exception) when (!IsCallerCancellation(exception, cancellationToken))
        {
            var kind = NoteStoreFailure(exception, "set_if_absent", key);
            RecordOperation("set_if_absent", "degraded", start);
            return options.OnUnavailable == UnavailableBehavior.Throw
                ? throw new CacheUnavailableException(key, kind, exception)
                : false;
        }

        if (written)
        {
            _local?.Set(
                key,
                value,
                options.LocalExpiration ?? options.Expiration,
                options.Tags,
                payload.WrittenCount
            );
            if (compressed)
            {
                CachingDiagnostics.Compressions.Add(1, _namespaceTag);
            }
        }

        RecordOperation("set_if_absent", written ? "miss" : "hit_l2", start);
        return written;
    }

    /// <inheritdoc />
    public async ValueTask RemoveAsync(string key, CancellationToken cancellationToken = default)
    {
        ValidateKey(key);
        ThrowIfDisposed();
        using var activity = StartActivity("cache.remove", key);
        var start = Stopwatch.GetTimestamp();
        _local?.Remove(key);
        var degraded = false;
        if (_store is not null)
        {
            try
            {
                await _store.RemoveAsync([DataKey(key)], cancellationToken).ConfigureAwait(false);
            }
            catch (Exception exception) when (!IsCallerCancellation(exception, cancellationToken))
            {
                NoteStoreFailure(exception, "remove", key);
                degraded = true;
            }
        }

        await PublishAsync(new CacheInvalidation([key], []), cancellationToken)
            .ConfigureAwait(false);
        RecordOperation("remove", degraded ? "degraded" : "miss", start);
    }

    /// <inheritdoc />
    public async ValueTask RemoveAsync(
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(keys);
        ThrowIfDisposed();
        var list = keys.ToList();
        foreach (var key in list)
        {
            ValidateKey(key);
        }

        if (list.Count == 0)
        {
            return;
        }

        var start = Stopwatch.GetTimestamp();
        _local?.Remove(list);
        var degraded = false;
        if (_store is not null)
        {
            try
            {
                await _store
                    .RemoveAsync(list.ConvertAll(DataKey), cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (!IsCallerCancellation(exception, cancellationToken))
            {
                NoteStoreFailure(exception, "remove", list[0]);
                degraded = true;
            }
        }

        await PublishAsync(new CacheInvalidation(list, []), cancellationToken)
            .ConfigureAwait(false);
        RecordOperation("remove", degraded ? "degraded" : "miss", start);
    }

    /// <inheritdoc />
    public async ValueTask RemoveByTagAsync(
        string tag,
        CancellationToken cancellationToken = default
    )
    {
        CacheKey.Validate(tag, _options.MaxKeyLength, nameof(tag));
        ThrowIfDisposed();
        var start = Stopwatch.GetTimestamp();
        _local?.RemoveByTag(tag);
        var degraded = false;
        if (_store is not null && _store.Capabilities.HasFlag(CacheStoreCapabilities.Tags))
        {
            try
            {
                await _store
                    .RemoveByTagAsync(_tagPrefix + tag, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (!IsCallerCancellation(exception, cancellationToken))
            {
                NoteStoreFailure(exception, "remove_by_tag", tag);
                degraded = true;
            }
        }

        await PublishAsync(new CacheInvalidation([], [tag]), cancellationToken)
            .ConfigureAwait(false);
        RecordOperation("remove_by_tag", degraded ? "degraded" : "miss", start);
    }

    /// <inheritdoc />
    public async ValueTask<IReadOnlyDictionary<string, T>> GetManyAsync<T>(
        IEnumerable<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(keys);
        ThrowIfDisposed();
        using var activity = StartActivity("cache.get_many", null);
        var start = Stopwatch.GetTimestamp();
        var found = new Dictionary<string, T>(StringComparer.Ordinal);
        List<string>? missing = null;
        foreach (var key in keys)
        {
            ValidateKey(key);
            if (_local is not null && _local.TryGet<T>(key, out var hit))
            {
                found[key] = hit;
            }
            else
            {
                (missing ??= []).Add(key);
            }
        }

        var degraded = false;
        if (_store is not null && missing is { Count: > 0 })
        {
            IReadOnlyDictionary<string, CacheStoreEntry> entries;
            try
            {
                entries = await _store
                    .GetManyAsync(missing.ConvertAll(DataKey), cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (!IsCallerCancellation(exception, cancellationToken))
            {
                NoteStoreFailure(exception, "get_many", missing[0]);
                entries = new Dictionary<string, CacheStoreEntry>();
                degraded = true;
            }

            foreach (var key in missing)
            {
                if (
                    entries.TryGetValue(DataKey(key), out var entry)
                    && Decode<T>(key, entry, null) is { Found: true } lookup
                )
                {
                    found[key] = lookup.Value!;
                }
            }
        }

        activity?.SetTag("hostloom.cache.degraded", degraded);
        RecordOperation(
            "get_many",
            degraded ? "degraded"
                : found.Count > 0 ? "hit_l2"
                : "miss",
            start
        );
        return found;
    }

    /// <inheritdoc />
    public async ValueTask WarmupAsync<T>(
        IReadOnlyDictionary<string, T> entries,
        TimeSpan expiration,
        IProgress<CacheWarmupProgress>? progress = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(entries);
        ThrowIfDisposed();
        if (expiration <= TimeSpan.Zero || entries.Count == 0)
        {
            progress?.Report(new CacheWarmupProgress(0, entries.Count));
            return;
        }

        using var activity = StartActivity("cache.warmup", null);
        var start = Stopwatch.GetTimestamp();
        var written = 0;
        var batch = new List<KeyValuePair<string, T>>(_options.Warmup.BatchSize);
        foreach (var pair in entries)
        {
            ValidateKey(pair.Key);
            ArgumentNullException.ThrowIfNull(pair.Value, nameof(entries));
            batch.Add(pair);
            if (batch.Count < _options.Warmup.BatchSize)
            {
                continue;
            }

            if (!await WriteBatchAsync(batch, expiration, cancellationToken).ConfigureAwait(false))
            {
                RecordOperation("warmup", "degraded", start);
                return;
            }

            written += batch.Count;
            progress?.Report(new CacheWarmupProgress(written, entries.Count));
            batch.Clear();
        }

        if (batch.Count > 0)
        {
            if (!await WriteBatchAsync(batch, expiration, cancellationToken).ConfigureAwait(false))
            {
                RecordOperation("warmup", "degraded", start);
                return;
            }

            written += batch.Count;
            progress?.Report(new CacheWarmupProgress(written, entries.Count));
        }

        RecordOperation("warmup", "miss", start);
    }

    /// <summary>Stops the timers, the invalidation loop, and the subscription.</summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) != 0)
        {
            return;
        }

        CachingDiagnostics.Unregister(this);
        _maintenance.Dispose();
        _subscription?.Dispose();
        _pending?.Writer.TryComplete();
        await _disposal.CancelAsync().ConfigureAwait(false);
        if (_invalidationLoop is not null)
        {
            try
            {
                await _invalidationLoop.ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Expected: the loop observes the disposal token.
            }
        }

        _local?.Dispose();
        _guard.Dispose();
        _disposal.Dispose();
    }

    internal CacheDescription Describe(IReadOnlyList<string> warmups)
    {
        var storeName = _store?.GetType().Name ?? "InMemory";
        var invalidation =
            _channel is not null ? $"channel ({_channel})"
            : _store is not null ? "TTL-only"
            : "none";
        var lines = new List<string>
        {
            $"Namespace = {_options.Namespace} (Caching:Namespace)",
            _store is null
                ? "Store = InMemory (UseInMemory: the in-process tier is the only tier)"
                : $"Store = {storeName} (UseStore)",
            _local is null
                ? "L1 = disabled (Caching:L1:Enabled = false)"
                : $"L1 = enabled (Caching:L1:Enabled = true, MaxEntries = {_options.L1.MaxEntries}, MaxBytes = {_options.L1.MaxBytes?.ToString(System.Globalization.CultureInfo.InvariantCulture) ?? "unbounded"}, MaxEntryAge = {_options.L1.MaxEntryAge})",
            _serializer is null
                ? "Serializer = (none: no distributed tier)"
                : $"Serializer = {_serializer.GetType().Name} (UseSystemTextJson / UseSerializer)",
            _channel is not null
                ? $"Invalidation = {invalidation} (Caching:Invalidation:Mode = {_options.Invalidation.Mode}, MaxPending = {_options.Invalidation.MaxPending})"
            : _store is not null
                ? "Invalidation = TTL-only (the store offers no invalidation channel)"
            : "Invalidation = none (single process; staleness bounded by Caching:L1:MaxEntryAge)",
            $"Stampede lease = {_options.Stampede.LeaseDuration} (Caching:Stampede:LeaseDuration, Attempts = {_options.Stampede.Attempts}, WaitBeforeFallback = {_options.Stampede.WaitBeforeFallback})",
            $"Compression = payloads of {_options.Compression.ThresholdBytes} bytes or more (Caching:Compression:ThresholdBytes)",
            warmups.Count == 0
                ? $"Warmups = (none) (Caching:Warmup:BlocksReadiness = {_options.Warmup.BlocksReadiness})"
                : $"Warmups = {string.Join(", ", warmups)} (Caching:Warmup:BlocksReadiness = {_options.Warmup.BlocksReadiness}, BatchSize = {_options.Warmup.BatchSize})",
        };
        return new CacheDescription(
            _options.Namespace,
            storeName,
            _local is not null,
            _serializer?.GetType().Name,
            invalidation,
            warmups,
            lines
        );
    }

    private async ValueTask<T?> GetOrCreateSlowAsync<TState, T>(
        string key,
        TState state,
        Func<TState, CancellationToken, ValueTask<T>> factory,
        CacheEntryOptions options,
        long start,
        CancellationToken cancellationToken
    )
    {
        using var activity = StartActivity("cache.get_or_create", key);
        var degraded = false;
        if (_store is not null)
        {
            var lookup = await ReadFromStoreAsync<T>(
                    key,
                    options.LocalExpiration,
                    cancellationToken
                )
                .ConfigureAwait(false);
            degraded |= lookup.Degraded;
            if (lookup.Found)
            {
                Finish(activity, "get_or_create", "hit_l2", start, degraded, CacheTier.L2);
                return lookup.Value;
            }
        }

        using var guard = await _guard.AcquireAsync(key, cancellationToken).ConfigureAwait(false);
        if (guard.Waited)
        {
            // Another caller ran the factory while this one waited; its result is in a tier.
            if (_local is not null && _local.TryGet<T>(key, out var filled))
            {
                Finish(activity, "get_or_create", "hit_l1", start, degraded, CacheTier.L1);
                return filled;
            }

            if (_store is not null)
            {
                var again = await ReadFromStoreAsync<T>(
                        key,
                        options.LocalExpiration,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
                degraded |= again.Degraded;
                if (again.Found)
                {
                    Finish(activity, "get_or_create", "hit_l2", start, degraded, CacheTier.L2);
                    return again.Value;
                }
            }
        }

        string? leaseKey = null;
        var leaseHeld = false;
        var leaseTaken = 0L;
        if (_store is not null && options.Expiration > TimeSpan.Zero)
        {
            leaseKey = _leasePrefix + key + _versionSuffix;
            leaseTaken = _time.GetTimestamp();
            try
            {
                leaseHeld = await _store
                    .SetIfAbsentAsync(
                        leaseKey,
                        LeasePayload,
                        _options.Stampede.LeaseDuration,
                        cancellationToken: cancellationToken
                    )
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (!IsCallerCancellation(exception, cancellationToken))
            {
                NoteStoreFailure(exception, "lease", key);
                degraded = true;
                leaseKey = null;
            }

            if (!leaseHeld && leaseKey is not null)
            {
                // Another instance holds the lease. Give it a moment to publish, re-check, and if
                // it has not, run the factory anyway: the lease is an optimisation, never a wait.
                for (var attempt = 0; attempt < _options.Stampede.Attempts; attempt++)
                {
                    if (_options.Stampede.WaitBeforeFallback > TimeSpan.Zero)
                    {
                        await Task.Delay(
                                _options.Stampede.WaitBeforeFallback,
                                _time,
                                cancellationToken
                            )
                            .ConfigureAwait(false);
                    }

                    var again = await ReadFromStoreAsync<T>(
                            key,
                            options.LocalExpiration,
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                    degraded |= again.Degraded;
                    if (again.Found)
                    {
                        Finish(activity, "get_or_create", "hit_l2", start, degraded, CacheTier.L2);
                        return again.Value;
                    }

                    if (again.Degraded)
                    {
                        break;
                    }
                }

                CachingDiagnostics.StampedeLeaseMissed.Add(1, _namespaceTag);
                leaseKey = null;
            }
        }

        try
        {
            var factoryStart = Stopwatch.GetTimestamp();
            T value;
            try
            {
                value = await factory(state, cancellationToken).ConfigureAwait(false);
            }
            finally
            {
                CachingDiagnostics.FactoryDuration.Record(
                    Stopwatch.GetElapsedTime(factoryStart).TotalSeconds,
                    _namespaceTag
                );
            }

            if (value is not null && options.Expiration > TimeSpan.Zero)
            {
                degraded |= await WriteAsync(key, value, options, cancellationToken)
                    .ConfigureAwait(false);
            }

            Finish(
                activity,
                "get_or_create",
                degraded ? "degraded" : "miss",
                start,
                degraded,
                CacheTier.None
            );
            return value;
        }
        finally
        {
            // A factory that outlived the lease no longer owns it: the release is an unconditional
            // delete, so releasing now would remove whichever instance holds it next and let two
            // more factories run. The expired lease is already gone or belongs to someone else.
            if (
                leaseHeld
                && leaseKey is not null
                && _time.GetElapsedTime(leaseTaken) < _options.Stampede.LeaseDuration
            )
            {
                await ReleaseLeaseAsync(leaseKey).ConfigureAwait(false);
            }
        }
    }

    private async ValueTask<CacheLookup<T>> GetSlowAsync<T>(
        string key,
        long start,
        CancellationToken cancellationToken
    )
    {
        using var activity = StartActivity("cache.get", key);
        var lookup = await ReadFromStoreAsync<T>(key, null, cancellationToken)
            .ConfigureAwait(false);
        Finish(
            activity,
            "get",
            lookup.Found ? "hit_l2"
                : lookup.Degraded ? "degraded"
                : "miss",
            start,
            lookup.Degraded,
            lookup.Tier
        );
        return lookup;
    }

    private async ValueTask<CacheLookup<T>> ReadFromStoreAsync<T>(
        string key,
        TimeSpan? localExpiration,
        CancellationToken cancellationToken
    )
    {
        CacheStoreEntry? entry;
        try
        {
            entry = await _store!.GetAsync(DataKey(key), cancellationToken).ConfigureAwait(false);
        }
        catch (Exception exception) when (!IsCallerCancellation(exception, cancellationToken))
        {
            NoteStoreFailure(exception, "get", key);
            return CacheLookup.Miss<T>(degraded: true);
        }

        return entry is { } found ? Decode<T>(key, found, localExpiration) : CacheLookup.Miss<T>();
    }

    private CacheLookup<T> Decode<T>(string key, CacheStoreEntry entry, TimeSpan? localExpiration)
    {
        var status = CachePayloadCodec.TryDecode<T>(
            _serializer!,
            entry.Payload.Span,
            _options.MaxPayloadBytes,
            out var value,
            out var tags,
            out var failure
        );
        switch (status)
        {
            case PayloadDecodeStatus.Ok when value is not null:
                if (_local is not null)
                {
                    var remaining = entry.RemainingTimeToLive ?? _options.L1.MaxEntryAge;
                    var local =
                        localExpiration is { } explicitLocal && explicitLocal < remaining
                            ? explicitLocal
                            : remaining;
                    _local.Set(key, value, local, tags, entry.Payload.Length);
                }

                return CacheLookup.Hit(value, CacheTier.L2);
            case PayloadDecodeStatus.VersionMismatch:
                // Written by a newer or older deploy: a miss, and deliberately not an error.
                return CacheLookup.Miss<T>();
            default:
                CachingDiagnostics.Errors.Add(
                    1,
                    _namespaceTag,
                    new KeyValuePair<string, object?>(CachingDiagnostics.KindTag, "serialization")
                );
                _logger.LogError(
                    new EventId(1002, "CachePayloadUnreadable"),
                    failure,
                    "Cached payload for '{Key}' in namespace '{Namespace}' could not be deserialized as {Type}; treating it as a miss so the next factory result overwrites it.",
                    key,
                    _options.Namespace,
                    typeof(T).Name
                );
                return CacheLookup.Miss<T>();
        }
    }

    /// <summary>Writes to the distributed tier, then the in-process tier. Returns whether it degraded.</summary>
    private async ValueTask<bool> WriteAsync<T>(
        string key,
        T value,
        CacheEntryOptions options,
        CancellationToken cancellationToken
    )
    {
        var degraded = false;
        long? size = options.Size;
        if (_store is not null)
        {
            using var payload = Encode(key, value, options, out var compressed);
            if (payload is not null)
            {
                size = payload.WrittenCount;
                try
                {
                    await _store
                        .SetAsync(
                            DataKey(key),
                            payload.WrittenMemory,
                            options.Expiration,
                            TagKeys(options.Tags),
                            cancellationToken
                        )
                        .ConfigureAwait(false);
                    if (compressed)
                    {
                        CachingDiagnostics.Compressions.Add(1, _namespaceTag);
                    }
                }
                catch (Exception exception)
                    when (!IsCallerCancellation(exception, cancellationToken))
                {
                    NoteStoreFailure(exception, "set", key);
                    degraded = true;
                }
            }
        }

        _local?.Set(key, value, options.LocalExpiration ?? options.Expiration, options.Tags, size);
        return degraded;
    }

    private async ValueTask<bool> WriteBatchAsync<T>(
        List<KeyValuePair<string, T>> batch,
        TimeSpan expiration,
        CancellationToken cancellationToken
    )
    {
        if (_store is not null)
        {
            var writers = new List<PooledBufferWriter>(batch.Count);
            try
            {
                var payloads = new List<KeyValuePair<string, ReadOnlyMemory<byte>>>(batch.Count);
                foreach (var (key, value) in batch)
                {
                    // CA2000: ownership moves to the writers list, disposed in the finally below.
#pragma warning disable CA2000
                    var payload = Encode(key, value, new CacheEntryOptions(expiration), out _);
#pragma warning restore CA2000
                    if (payload is null)
                    {
                        continue;
                    }

                    writers.Add(payload);
                    payloads.Add(
                        new KeyValuePair<string, ReadOnlyMemory<byte>>(
                            DataKey(key),
                            payload.WrittenMemory
                        )
                    );
                }

                await _store
                    .SetManyAsync(payloads, expiration, cancellationToken)
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (!IsCallerCancellation(exception, cancellationToken))
            {
                NoteStoreFailure(exception, "warmup", batch[0].Key);
                _logger.LogWarning(
                    new EventId(1006, "CacheWarmupAborted"),
                    "Warmup of namespace '{Namespace}' stopped after a distributed-store failure; the remaining entries will be filled on demand.",
                    _options.Namespace
                );
                return false;
            }
            finally
            {
                foreach (var writer in writers)
                {
                    writer.Dispose();
                }
            }
        }

        if (_local is not null)
        {
            foreach (var (key, value) in batch)
            {
                _local.Set(key, value!, expiration);
            }
        }

        return true;
    }

    private PooledBufferWriter? Encode<T>(
        string key,
        T value,
        CacheEntryOptions options,
        out bool compressed
    )
    {
        var writer = new PooledBufferWriter();
        int bodyLength;
        try
        {
            compressed = CachePayloadCodec.Encode(
                _serializer!,
                value,
                options.Tags,
                _options.Compression.ThresholdBytes,
                writer,
                out bodyLength
            );
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            writer.Dispose();
            CachingDiagnostics.Errors.Add(
                1,
                _namespaceTag,
                new KeyValuePair<string, object?>(CachingDiagnostics.KindTag, "serialization")
            );
            _logger.LogError(
                new EventId(1002, "CachePayloadUnreadable"),
                exception,
                "Value for '{Key}' in namespace '{Namespace}' could not be serialized as {Type}; it is kept in the in-process tier only.",
                key,
                _options.Namespace,
                typeof(T).Name
            );
            compressed = false;
            return null;
        }

        // Both sizes are bounded: the encoded one because it is what the store holds, the body
        // because a reader allocates the declared uncompressed length and trusts it only this far.
        var oversize = Math.Max(writer.WrittenCount, bodyLength);
        if (oversize > _options.MaxPayloadBytes)
        {
            writer.Dispose();
            _logger.LogError(
                new EventId(1003, "CachePayloadTooLarge"),
                "Value for '{Key}' in namespace '{Namespace}' serializes to {Bytes} bytes ({EncodedBytes} encoded), above Caching:MaxPayloadBytes ({MaxPayloadBytes}); it is kept in the in-process tier only.",
                key,
                _options.Namespace,
                bodyLength,
                writer.WrittenCount,
                _options.MaxPayloadBytes
            );
            compressed = false;
            return null;
        }

        return writer;
    }

    private async ValueTask ReleaseLeaseAsync(string leaseKey)
    {
        try
        {
            await _store!.RemoveAsync([leaseKey], CancellationToken.None).ConfigureAwait(false);
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            // The lease expires on its own; a failed release costs at most one lease duration.
            NoteStoreFailure(exception, "lease_release", leaseKey);
        }
    }

    private async ValueTask PublishAsync(
        CacheInvalidation invalidation,
        CancellationToken cancellationToken
    )
    {
        if (_channel is null)
        {
            return;
        }

        using var timeout = new CancellationTokenSource(_options.Invalidation.Timeout, _time);
        using var linked = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            timeout.Token
        );
        try
        {
            await _channel.PublishAsync(invalidation, linked.Token).ConfigureAwait(false);
            CachingDiagnostics.Invalidations.Add(
                1,
                _namespaceTag,
                new KeyValuePair<string, object?>(CachingDiagnostics.DirectionTag, "sent")
            );
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception exception)
        {
            CachingDiagnostics.Errors.Add(
                1,
                _namespaceTag,
                new KeyValuePair<string, object?>(CachingDiagnostics.KindTag, "other")
            );
            _logger.LogWarning(
                new EventId(1004, "CacheInvalidationNotPublished"),
                exception,
                "Invalidation in namespace '{Namespace}' could not be published; other instances serve the old value until it expires.",
                _options.Namespace
            );
        }
    }

    private void OnInvalidation(CacheInvalidation invalidation)
    {
        // A refusal means the queue was completed by disposal, which needs no warning; a full queue
        // reports itself through the drop callback instead.
        _ = _pending?.Writer.TryWrite(invalidation);
    }

    private void ReportDroppedInvalidation()
    {
        CachingDiagnostics.Invalidations.Add(
            1,
            _namespaceTag,
            new KeyValuePair<string, object?>(CachingDiagnostics.DirectionTag, "dropped")
        );
        if (_throttle.ShouldLog(InvalidationThrottleKey))
        {
            _logger.LogWarning(
                new EventId(1005, "CacheInvalidationDropped"),
                "Invalidation queue for namespace '{Namespace}' is full (Caching:Invalidation:MaxPending = {MaxPending}); messages are being dropped and the in-process tier relies on expiry for them. Further warnings are suppressed for {Interval}.",
                _options.Namespace,
                _options.Invalidation.MaxPending,
                _options.Diagnostics.DegradedLogInterval
            );
        }
    }

    private async Task ApplyInvalidationsAsync(CancellationToken cancellationToken)
    {
        await foreach (
            var invalidation in _pending!
                .Reader.ReadAllAsync(cancellationToken)
                .ConfigureAwait(false)
        )
        {
            _local!.Remove(invalidation.Keys);
            foreach (var tag in invalidation.Tags)
            {
                _local.RemoveByTag(tag);
            }

            CachingDiagnostics.Invalidations.Add(
                1,
                _namespaceTag,
                new KeyValuePair<string, object?>(CachingDiagnostics.DirectionTag, "received")
            );
        }
    }

    private void Maintain()
    {
        _local?.RemoveExpired();
        _guard.Reclaim(_options.L1.GuardIdleTime);
    }

    private CacheFailureKind NoteStoreFailure(Exception exception, string operation, string key)
    {
        var kind = exception is CacheStoreException store ? store.Kind : CacheFailureKind.Other;
        CachingDiagnostics.Errors.Add(
            1,
            _namespaceTag,
            new KeyValuePair<string, object?>(
                CachingDiagnostics.KindTag,
                CachingDiagnostics.KindName(kind)
            )
        );
        if (_throttle.ShouldLog(key))
        {
            _logger.LogWarning(
                new EventId(1001, "CacheStoreDegraded"),
                exception,
                "Distributed cache store failed ({Kind}) during {Operation} of '{Key}' in namespace '{Namespace}'; serving from the in-process tier and factories until it recovers. Further warnings for this key are suppressed for {Interval}.",
                kind,
                operation,
                key,
                _options.Namespace,
                _options.Diagnostics.DegradedLogInterval
            );
        }

        return kind;
    }

    private static bool IsCallerCancellation(Exception exception, CancellationToken token) =>
        exception is OperationCanceledException && token.IsCancellationRequested;

    private void RecordOperation(string operation, string outcome, long start) =>
        CachingDiagnostics.OperationDuration.Record(
            Stopwatch.GetElapsedTime(start).TotalSeconds,
            _namespaceTag,
            new KeyValuePair<string, object?>(CachingDiagnostics.OperationTag, operation),
            new KeyValuePair<string, object?>(CachingDiagnostics.OutcomeTag, outcome)
        );

    private void Finish(
        Activity? activity,
        string operation,
        string outcome,
        long start,
        bool degraded,
        CacheTier tier
    )
    {
        if (activity is not null)
        {
            activity.SetTag("hostloom.cache.hit", tier != CacheTier.None);
            activity.SetTag("hostloom.cache.tier", tier.ToString());
            activity.SetTag("hostloom.cache.degraded", degraded);
        }

        RecordOperation(operation, outcome, start);
    }

    private static Activity? StartActivity(string name, string? key)
    {
        var activity = CachingDiagnostics.ActivitySource.StartActivity(name);
        if (activity is not null && key is not null)
        {
            activity.SetTag("hostloom.cache.key", key);
        }

        return activity;
    }

    private string DataKey(string key) => string.Concat(_dataPrefix, key, _versionSuffix);

    private List<string>? TagKeys(IReadOnlyCollection<string>? tags)
    {
        if (tags is not { Count: > 0 })
        {
            return null;
        }

        var keys = new List<string>(tags.Count);
        foreach (var tag in tags)
        {
            keys.Add(_tagPrefix + tag);
        }

        return keys;
    }

    private void ValidateKey(string key) => CacheKey.Validate(key, _options.MaxKeyLength);

    private void ThrowIfDisposed() =>
        ObjectDisposedException.ThrowIf(Volatile.Read(ref _disposed) != 0, this);
}
