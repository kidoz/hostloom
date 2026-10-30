using System.Threading.RateLimiting;
using HostLoom.RateLimiting.Internal;
using Microsoft.Extensions.Logging;

namespace HostLoom.RateLimiting;

/// <summary>
/// A token-bucket <see cref="RateLimiter"/> whose state is owned by an <see cref="IRateLimitStore"/>.
/// Remote stores support asynchronous acquisition only.
/// </summary>
public sealed class DistributedRateLimiter : RateLimiter
{
    private static readonly EventId StoreFailed = new(1601, "RateLimitStoreFailed");
    private readonly RateLimitingOptions _options;
    private readonly string _storeKey;
    private readonly IRateLimitStore _store;
    private readonly TimeProvider _timeProvider;
    private readonly ILogger<DistributedRateLimiter>? _logger;
    private readonly Lock _logGate = new();
    private DateTimeOffset _lastWarning;

    /// <summary>Creates a limiter over a caller-owned store.</summary>
    public DistributedRateLimiter(
        RateLimitingOptions options,
        string key,
        IRateLimitStore store,
        TimeProvider? timeProvider = null,
        ILogger<DistributedRateLimiter>? logger = null
    )
    {
        ArgumentNullException.ThrowIfNull(options);
        options.ThrowIfInvalid();
        RateLimitKey.Validate(key, options.MaxKeyLength);
        ArgumentNullException.ThrowIfNull(store);
        _options = options.Snapshot();
        _storeKey = $"{_options.Namespace}:rate:{key}";
        _store = store;
        _timeProvider = timeProvider ?? TimeProvider.System;
        _logger = logger;
    }

    /// <summary>The configured namespace used as a telemetry tag.</summary>
    public string Namespace => _options.Namespace;

    /// <inheritdoc />
    public override TimeSpan? IdleDuration => null;

    /// <inheritdoc />
    public override RateLimiterStatistics? GetStatistics() => null;

    /// <inheritdoc />
    protected override RateLimitLease AttemptAcquireCore(int permitCount) =>
        DistributedRateLimitLease.SynchronousFailure;

    /// <inheritdoc />
    protected override async ValueTask<RateLimitLease> AcquireAsyncCore(
        int permitCount,
        CancellationToken cancellationToken
    )
    {
        ArgumentOutOfRangeException.ThrowIfGreaterThan(
            permitCount,
            _options.TokenBucket.TokenLimit
        );
        var started = RateLimitingDiagnostics.Timestamp();
        try
        {
            var decision = await _store
                .AcquireAsync(_storeKey, _options.TokenBucket, permitCount, cancellationToken)
                .ConfigureAwait(false);
            RateLimitingDiagnostics.Record(
                Namespace,
                decision.IsAcquired ? "acquired" : "rejected",
                RateLimitingDiagnostics.Elapsed(started),
                failureKind: null
            );
            return decision.IsAcquired
                ? DistributedRateLimitLease.Acquired(decision, degraded: false)
                : DistributedRateLimitLease.Rejected(decision);
        }
        catch (RateLimitStoreException exception) when (!cancellationToken.IsCancellationRequested)
        {
            var behavior = _options.OnUnavailable;
            RateLimitingDiagnostics.Record(
                Namespace,
                behavior == RateLimitUnavailableBehavior.Allow ? "degraded" : "unavailable",
                RateLimitingDiagnostics.Elapsed(started),
                exception.Kind switch
                {
                    RateLimitFailureKind.Unavailable => "unavailable",
                    RateLimitFailureKind.Timeout => "timeout",
                    _ => "other",
                }
            );
            LogFailure(exception, behavior);
            return behavior == RateLimitUnavailableBehavior.Allow
                ? DistributedRateLimitLease.Acquired(
                    new RateLimitDecision(true, 0, TimeSpan.Zero),
                    degraded: true
                )
                : DistributedRateLimitLease.Unavailable();
        }
    }

    private void LogFailure(
        RateLimitStoreException exception,
        RateLimitUnavailableBehavior behavior
    )
    {
        if (_logger is null || !_logger.IsEnabled(LogLevel.Warning))
        {
            return;
        }

        lock (_logGate)
        {
            var now = _timeProvider.GetUtcNow();
            if (_lastWarning != default && now - _lastWarning < TimeSpan.FromSeconds(30))
            {
                return;
            }

            _lastWarning = now;
            _logger.LogWarning(
                StoreFailed,
                "Rate-limit store failed ({Failure}, {ExceptionType}); applying {Behavior} behavior.",
                exception.Kind,
                exception.GetType().Name,
                behavior
            );
        }
    }
}
