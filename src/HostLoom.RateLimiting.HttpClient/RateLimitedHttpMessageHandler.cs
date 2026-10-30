using System.Threading.RateLimiting;

namespace HostLoom.RateLimiting.HttpClient;

/// <summary>Acquires one permit before every physical outgoing HTTP attempt.</summary>
public sealed class RateLimitedHttpMessageHandler : DelegatingHandler
{
    private readonly RateLimiter _limiter;
    private readonly HttpRateLimitOptions _options;
    private readonly TimeProvider _timeProvider;
    private readonly SemaphoreSlim _waitSlots;

    /// <summary>Creates a handler over a caller-owned limiter.</summary>
    public RateLimitedHttpMessageHandler(
        RateLimiter limiter,
        HttpRateLimitOptions? options = null,
        TimeProvider? timeProvider = null
    )
    {
        ArgumentNullException.ThrowIfNull(limiter);
        options ??= new HttpRateLimitOptions();
        options.ThrowIfInvalid();
        _limiter = limiter;
        _options = options.Snapshot();
        _timeProvider = timeProvider ?? TimeProvider.System;
        _waitSlots = new SemaphoreSlim(_options.QueueLimit);
    }

    /// <inheritdoc />
    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken
    )
    {
        var started = _timeProvider.GetTimestamp();
        var holdsWaitSlot = false;
        try
        {
            while (true)
            {
                using var lease = await _limiter
                    .AcquireAsync(1, cancellationToken)
                    .ConfigureAwait(false);
                if (lease.IsAcquired)
                {
                    var degraded =
                        lease.TryGetMetadata(RateLimitMetadata.Degraded, out bool isDegraded)
                        && isDegraded;
                    HttpRateLimitingDiagnostics.Record(
                        degraded ? "degraded" : "sent",
                        Elapsed(started)
                    );
                    return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
                }

                if (
                    lease.TryGetMetadata(RateLimitMetadata.Unavailable, out bool unavailable)
                    && unavailable
                )
                {
                    HttpRateLimitingDiagnostics.Record("unavailable", Elapsed(started));
                    throw new RateLimiterUnavailableException();
                }

                _ = lease.TryGetMetadata(MetadataName.RetryAfter, out TimeSpan retryAfter);
                var remaining = _options.MaxWait - Elapsed(started);
                if (
                    _options.MaxWait <= TimeSpan.Zero
                    || retryAfter <= TimeSpan.Zero
                    || retryAfter > remaining
                )
                {
                    HttpRateLimitingDiagnostics.Record("rejected", Elapsed(started));
                    throw new RateLimitExceededException(
                        retryAfter > TimeSpan.Zero ? retryAfter : null
                    );
                }

                if (!holdsWaitSlot)
                {
                    if (!_waitSlots.Wait(0, cancellationToken))
                    {
                        HttpRateLimitingDiagnostics.Record("queue_full", Elapsed(started));
                        throw new RateLimitExceededException(retryAfter);
                    }

                    holdsWaitSlot = true;
                }

                await Task.Delay(retryAfter, _timeProvider, cancellationToken)
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            if (holdsWaitSlot)
            {
                _waitSlots.Release();
            }
        }
    }

    /// <inheritdoc />
    protected override void Dispose(bool disposing)
    {
        if (disposing)
        {
            _waitSlots.Dispose();
        }

        base.Dispose(disposing);
    }

    private TimeSpan Elapsed(long started) => _timeProvider.GetElapsedTime(started);
}
