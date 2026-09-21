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

    // A generation per key stripe conservatively rejects every single-key fill overlapping an
    // invalidation of that key; tag and flush invalidations move every stripe, and the whole
    // generation guards bulk operations. The gate makes generation checks and L1 insertion
    // atomic with local eviction.
    private const int GenerationStripes = 1024;
    private readonly Lock _invalidationGate = new();
    private readonly long[] _generations = new long[GenerationStripes];
    private long _invalidationGeneration;

    // What this instance published and has not yet seen come back. The channel echoes every
    // publish to its publisher; applying the echo as a fresh invalidation would suppress the
    // very refill that follows a removal, so an echo is recognised and skipped.
    private const int RememberedPublishes = 64;
    private readonly Lock _publishedGate = new();
    private readonly Queue<PublishedInvalidation> _published = new();

    private long FillGeneration => Interlocked.Read(ref _invalidationGeneration);

    private long FillGenerationOf(string key) => Volatile.Read(ref _generations[Stripe(key)]);

    private static int Stripe(string key) =>
        (int)((uint)string.GetHashCode(key, StringComparison.Ordinal) % GenerationStripes);

    /// <summary>The generation stripe of <paramref name="key"/>, for tests that need two keys apart.</summary>
    internal static int StripeOf(string key) => Stripe(key);

    private readonly record struct PublishedInvalidation(
        int Hash,
        CacheInvalidation Message,
        long PublishedAt
    );

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
        // Four guards per in-process entry bounds the single-flight map; beyond that idle guards
        // are reclaimed at once and further keys share striped guards.
        _guard = new KeyedAsyncGuard(
            _time,
            (int)Math.Clamp(4L * options.L1.MaxEntries, 1, int.MaxValue)
        );
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
        var degraded = await WriteAsync(
                key,
                value,
                options,
                FillGenerationOf(key),
                cancellationToken
            )
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
            bool added;
            lock (_invalidationGate)
            {
                added = _local!.SetIfAbsent(
                    key,
                    value,
                    options.Expiration,
                    options.Tags,
                    options.Size
                );
            }
            RecordOperation("set_if_absent", added ? "miss" : "hit_l1", start);
            return added;
        }

        using var payload = Encode(key, value, options, out var compressed);
        if (payload is null)
        {
            RecordOperation("set_if_absent", "error", start);
            return false;
        }

        var generation = FillGenerationOf(key);
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
            lock (_invalidationGate)
            {
                if (generation == _generations[Stripe(key)])
                {
                    _local?.Set(
                        key,
                        value,
                        options.LocalExpiration ?? options.Expiration,
                        options.Tags,
                        payload.WrittenCount,
                        options.EffectiveStaleGrace
                    );
                }
            }
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
        InvalidateLocal(new CacheInvalidation([key], []));
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

        InvalidateLocal(new CacheInvalidation([key], []));
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
        InvalidateLocal(new CacheInvalidation(list, []));
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

        InvalidateLocal(new CacheInvalidation(list, []));
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
        InvalidateLocal(new CacheInvalidation([], [tag]));
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

        InvalidateLocal(new CacheInvalidation([], [tag]));
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
                found[key] = hit!;
            }
            else
            {
                (missing ??= []).Add(key);
            }
        }

        var degraded = false;
        if (_store is not null && missing is { Count: > 0 })
        {
            var generation = FillGeneration;
            var readStart = _time.GetTimestamp();
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
                    && Decode<T>(key, entry, null, generation, readStart, bulk: true)
                        is { Found: true } lookup
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
                ? $"Invalidation = {invalidation} (Caching:Invalidation:Mode = {_options.Invalidation.Mode}, MaxPending = {_options.Invalidation.MaxPending}, FlushLocalOnReconnect = {_options.Invalidation.FlushLocalOnReconnect})"
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
        var haveStale = false;
        T? stale = default;
        KeyedAsyncGuard.Releaser tryGuard = default;
        if (_store is not null)
        {
            var lookup = await ReadFromStoreAsync<T>(key, options, cancellationToken)
                .ConfigureAwait(false);
            degraded |= lookup.Degraded;
            if (lookup.Found)
            {
                Finish(activity, "get_or_create", "hit_l2", start, degraded, CacheTier.L2);
                return lookup.Value;
            }

            // The store cannot answer and an expired copy is still inside its grace window: one
            // caller refreshes through the factory and the rest take the copy at once, instead
            // of queueing behind the factory for a value the outage already made late.
            haveStale =
                lookup.Degraded
                && options.EffectiveStaleGrace is not null
                && _local is not null
                && _local.TryGetWithinGrace(key, out stale, out _);
        }

        if (haveStale && !_guard.TryAcquire(key, out tryGuard))
        {
            Finish(activity, "get_or_create", "hit_stale", start, degraded, CacheTier.L1);
            return stale;
        }

        using var guard = haveStale
            ? tryGuard
            : await _guard.AcquireAsync(key, cancellationToken).ConfigureAwait(false);
        // A caller can receive an earlier store miss after another caller has already filled the
        // cache and released the guard. Re-check even when acquisition did not have to wait.
        if (_local is not null && _local.TryGet<T>(key, out var filled))
        {
            Finish(activity, "get_or_create", "hit_l1", start, degraded, CacheTier.L1);
            return filled;
        }

        if (_store is not null)
        {
            var again = await ReadFromStoreAsync<T>(key, options, cancellationToken)
                .ConfigureAwait(false);
            degraded |= again.Degraded;
            if (again.Found)
            {
                Finish(activity, "get_or_create", "hit_l2", start, degraded, CacheTier.L2);
                return again.Value;
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

                    var again = await ReadFromStoreAsync<T>(key, options, cancellationToken)
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
            var generation = FillGenerationOf(key);
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

            if (value is null)
            {
                if (options.CachesNull)
                {
                    degraded |= await WriteNullAsync(key, options, generation, cancellationToken)
                        .ConfigureAwait(false);
                }
            }
            else if (options.Expiration > TimeSpan.Zero)
            {
                degraded |= await WriteAsync(key, value, options, generation, cancellationToken)
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
        CacheEntryOptions? options,
        CancellationToken cancellationToken
    )
    {
        var generation = FillGenerationOf(key);
        var readStart = _time.GetTimestamp();
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

        return entry is { } found
            ? Decode<T>(key, found, options, generation, readStart, bulk: false)
            : CacheLookup.Miss<T>();
    }

    /// <param name="generation">
    /// The key's stripe generation, or the whole generation when <paramref name="bulk"/>,
    /// captured before the distributed read; an invalidation since then leaves L1 untouched.
    /// </param>
    /// <param name="readStart">
    /// When the distributed read started: a value written to L1 after that is newer than what
    /// the read returned and is kept.
    /// </param>
    private CacheLookup<T> Decode<T>(
        string key,
        CacheStoreEntry entry,
        CacheEntryOptions? options,
        long generation,
        long readStart,
        bool bulk
    )
    {
        var status = CachePayloadCodec.TryDecode<T>(
            _serializer!,
            entry.Payload.Span,
            _options.MaxPayloadBytes,
            out var value,
            out var tags,
            out var isNull,
            out var failure
        );
        switch (status)
        {
            case PayloadDecodeStatus.Ok when isNull:
                // A remembered absence. A non-nullable value type has no null to hand back, so
                // for it the entry is a miss and the next factory result replaces it.
                if (default(T) is not null)
                {
                    return CacheLookup.Miss<T>();
                }

                lock (_invalidationGate)
                {
                    if (generation == CurrentGeneration(key, bulk))
                    {
                        _local?.SetNull(
                            key,
                            LocalTimeToLive(entry, options),
                            tags,
                            options?.EffectiveStaleGrace,
                            notIfWrittenAfter: readStart
                        );
                    }
                }
                return CacheLookup.Hit<T>(default, CacheTier.L2);
            case PayloadDecodeStatus.Ok when value is not null:
                lock (_invalidationGate)
                {
                    if (generation == CurrentGeneration(key, bulk))
                    {
                        _local?.Set(
                            key,
                            value,
                            LocalTimeToLive(entry, options),
                            tags,
                            entry.Payload.Length,
                            options?.EffectiveStaleGrace,
                            notIfWrittenAfter: readStart
                        );
                    }
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

    /// <summary>The in-process life of a distributed hit: its remaining time, bounded by the call's local expiration.</summary>
    private TimeSpan LocalTimeToLive(CacheStoreEntry entry, CacheEntryOptions? options)
    {
        var remaining = entry.RemainingTimeToLive ?? _options.L1.MaxEntryAge;
        return options?.LocalExpiration is { } explicitLocal && explicitLocal < remaining
            ? explicitLocal
            : remaining;
    }

    /// <summary>
    /// Remembers a null factory result in the distributed tier, then the in-process tier, for
    /// <see cref="CacheEntryOptions.NullExpiration"/>. Returns whether it degraded.
    /// </summary>
    private async ValueTask<bool> WriteNullAsync(
        string key,
        CacheEntryOptions options,
        long generation,
        CancellationToken cancellationToken
    )
    {
        if (generation != FillGenerationOf(key))
        {
            return false;
        }
        var degraded = false;
        var expiration = options.NullExpiration!.Value;
        if (_store is not null)
        {
            using var payload = new PooledBufferWriter();
            CachePayloadCodec.EncodeNull(options.Tags, payload);
            try
            {
                await _store
                    .SetAsync(
                        DataKey(key),
                        payload.WrittenMemory,
                        expiration,
                        TagKeys(options.Tags),
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }
            catch (Exception exception) when (!IsCallerCancellation(exception, cancellationToken))
            {
                NoteStoreFailure(exception, "set", key);
                degraded = true;
            }
        }

        var local =
            options.LocalExpiration is { } explicitLocal && explicitLocal < expiration
                ? explicitLocal
                : expiration;
        lock (_invalidationGate)
        {
            if (generation == _generations[Stripe(key)])
            {
                _local?.SetNull(key, local, options.Tags, options.EffectiveStaleGrace);
            }
        }
        return degraded;
    }

    /// <summary>Writes to the distributed tier, then the in-process tier. Returns whether it degraded.</summary>
    private async ValueTask<bool> WriteAsync<T>(
        string key,
        T value,
        CacheEntryOptions options,
        long generation,
        CancellationToken cancellationToken
    )
    {
        if (generation != FillGenerationOf(key))
        {
            return false;
        }
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

        lock (_invalidationGate)
        {
            if (generation == _generations[Stripe(key)])
            {
                _local?.Set(
                    key,
                    value,
                    options.LocalExpiration ?? options.Expiration,
                    options.Tags,
                    size,
                    options.EffectiveStaleGrace
                );
            }
        }
        return degraded;
    }

    private async ValueTask<bool> WriteBatchAsync<T>(
        List<KeyValuePair<string, T>> batch,
        TimeSpan expiration,
        CancellationToken cancellationToken
    )
    {
        var generation = FillGeneration;
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

        lock (_invalidationGate)
        {
            if (_local is not null && generation == _invalidationGeneration)
            {
                foreach (var (key, value) in batch)
                {
                    _local.Set(key, value!, expiration);
                }
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
        // Before the call: the echo can arrive before the publish returns.
        RememberPublished(invalidation);
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
            ForgetPublished(invalidation);
            throw;
        }
        catch (Exception exception)
        {
            ForgetPublished(invalidation);
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
            if (IsOwnEcho(invalidation))
            {
                // Applied when it was published; applying it again would suppress the refill
                // that follows a removal on this instance.
                CachingDiagnostics.Invalidations.Add(
                    1,
                    _namespaceTag,
                    new KeyValuePair<string, object?>(CachingDiagnostics.DirectionTag, "echoed")
                );
                continue;
            }

            InvalidateLocal(invalidation);
            if (invalidation.FlushAll)
            {
                CachingDiagnostics.Invalidations.Add(
                    1,
                    _namespaceTag,
                    new KeyValuePair<string, object?>(CachingDiagnostics.DirectionTag, "flushed")
                );
                if (_logger.IsEnabled(LogLevel.Information))
                {
                    _logger.LogInformation(
                        new EventId(1007, "CacheL1Flushed"),
                        "In-process tier of namespace '{Namespace}' was cleared by an invalidation flush; entries refill from the distributed tier and factories.",
                        _options.Namespace
                    );
                }

                continue;
            }

            CachingDiagnostics.Invalidations.Add(
                1,
                _namespaceTag,
                new KeyValuePair<string, object?>(CachingDiagnostics.DirectionTag, "received")
            );
        }
    }

    /// <summary>Read under <see cref="_invalidationGate"/>: the generation a fill is checked against.</summary>
    private long CurrentGeneration(string key, bool bulk) =>
        bulk ? _invalidationGeneration : _generations[Stripe(key)];

    private void InvalidateLocal(CacheInvalidation invalidation)
    {
        lock (_invalidationGate)
        {
            Interlocked.Increment(ref _invalidationGeneration);
            if (invalidation.FlushAll || invalidation.Tags.Count > 0)
            {
                // The keys a tag covers are not known here, and a flush covers every key.
                for (var stripe = 0; stripe < GenerationStripes; stripe++)
                {
                    _generations[stripe]++;
                }
            }
            else
            {
                foreach (var key in invalidation.Keys)
                {
                    _generations[Stripe(key)]++;
                }
            }

            if (invalidation.FlushAll)
            {
                _local?.Clear();
                return;
            }
            _local?.Remove(invalidation.Keys);
            foreach (var tag in invalidation.Tags)
            {
                _local?.RemoveByTag(tag);
            }
        }
    }

    /// <summary>
    /// Remembers a message about to be published, so its echo is recognised. Bounded by count
    /// and by the publish timeout: an echo later than that is treated as someone else's.
    /// </summary>
    private void RememberPublished(CacheInvalidation invalidation)
    {
        var remembered = new PublishedInvalidation(
            Fingerprint(invalidation),
            invalidation,
            _time.GetTimestamp()
        );
        lock (_publishedGate)
        {
            _published.Enqueue(remembered);
            while (_published.Count > RememberedPublishes)
            {
                _published.Dequeue();
            }
        }
    }

    /// <summary>Forgets a message whose publish failed: no echo will come.</summary>
    private void ForgetPublished(CacheInvalidation invalidation)
    {
        lock (_publishedGate)
        {
            if (_published.Count == 0)
            {
                return;
            }

            var kept = _published.Where(entry => !ReferenceEquals(entry.Message, invalidation));
            var remaining = kept.ToArray();
            _published.Clear();
            foreach (var entry in remaining)
            {
                _published.Enqueue(entry);
            }
        }
    }

    /// <summary>
    /// Whether <paramref name="invalidation"/> is the echo of a message this instance published
    /// within the publish timeout, consuming the remembered publish when it is.
    /// </summary>
    private bool IsOwnEcho(CacheInvalidation invalidation)
    {
        var hash = Fingerprint(invalidation);
        lock (_publishedGate)
        {
            if (_published.Count == 0)
            {
                return false;
            }

            var matched = false;
            var remaining = new List<PublishedInvalidation>(_published.Count);
            while (_published.TryDequeue(out var entry))
            {
                if (_time.GetElapsedTime(entry.PublishedAt) > _options.Invalidation.Timeout)
                {
                    continue;
                }

                if (!matched && entry.Hash == hash && SameMessage(entry.Message, invalidation))
                {
                    matched = true;
                    continue;
                }

                remaining.Add(entry);
            }

            foreach (var entry in remaining)
            {
                _published.Enqueue(entry);
            }

            return matched;
        }
    }

    private static int Fingerprint(CacheInvalidation invalidation)
    {
        var hash = new HashCode();
        hash.Add(invalidation.FlushAll);
        hash.Add(invalidation.Keys.Count);
        foreach (var key in invalidation.Keys)
        {
            hash.Add(key, StringComparer.Ordinal);
        }

        hash.Add(invalidation.Tags.Count);
        foreach (var tag in invalidation.Tags)
        {
            hash.Add(tag, StringComparer.Ordinal);
        }

        return hash.ToHashCode();
    }

    private static bool SameMessage(CacheInvalidation left, CacheInvalidation right) =>
        left.FlushAll == right.FlushAll
        && left.Keys.SequenceEqual(right.Keys, StringComparer.Ordinal)
        && left.Tags.SequenceEqual(right.Tags, StringComparer.Ordinal);

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
