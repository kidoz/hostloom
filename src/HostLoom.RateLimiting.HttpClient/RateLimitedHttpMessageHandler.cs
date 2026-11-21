using System.Threading.RateLimiting;

namespace HostLoom.RateLimiting.HttpClient;

/// <summary>Acquires one permit before every physical outgoing HTTP attempt.</summary>
public sealed class RateLimitedHttpMessageHandler : DelegatingHandler
{
    private static readonly TimeSpan MaximumTimerDelay = TimeSpan.FromMilliseconds(
        uint.MaxValue - 1
    );
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
    protected override HttpResponseMessage Send(
        HttpRequestMessage request,
        CancellationToken cancellationToken
    ) => throw new NotSupportedException("Rate-limited HTTP requests require SendAsync.");

    /// <inheritdoc />
    protected override async Task<HttpResponseMessage> SendAsync(
        HttpRequestMessage request,
        CancellationToken cancellationToken
    )
    {
        var started = _timeProvider.GetTimestamp();
        using var lease = await AcquirePermitAsync(started, cancellationToken)
            .ConfigureAwait(false);
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfBudgetExpired(started);
        var degraded =
            lease.TryGetMetadata(RateLimitMetadata.Degraded, out bool isDegraded) && isDegraded;
        HttpRateLimitingDiagnostics.Record(degraded ? "degraded" : "sent", Elapsed(started));
        return await base.SendAsync(request, cancellationToken).ConfigureAwait(false);
    }

    private async ValueTask<RateLimitLease> AcquirePermitAsync(
        long started,
        CancellationToken cancellationToken
    )
    {
        var holdsWaitSlot = false;
        try
        {
            while (true)
            {
                var lease = await AcquireWithinBudgetAsync(
                        started,
                        holdsWaitSlot,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
                if (lease.IsAcquired)
                {
                    return lease;
                }

                using (lease)
                {
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
                        throw Rejected(started, retryAfter > TimeSpan.Zero ? retryAfter : null);
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

                    var retryStarted = _timeProvider.GetTimestamp();
                    do
                    {
                        cancellationToken.ThrowIfCancellationRequested();
                        ThrowIfBudgetExpired(started);
                        var delay = retryAfter - Elapsed(retryStarted);
                        if (delay <= TimeSpan.Zero)
                        {
                            break;
                        }

                        var budget = _options.MaxWait - Elapsed(started);
                        if (budget <= TimeSpan.Zero)
                        {
                            throw Rejected(started);
                        }

                        if (delay > budget)
                        {
                            delay = budget;
                        }

                        await Task.Delay(
                                delay > MaximumTimerDelay ? MaximumTimerDelay : delay,
                                _timeProvider,
                                cancellationToken
                            )
                            .ConfigureAwait(false);
                        cancellationToken.ThrowIfCancellationRequested();
                        ThrowIfBudgetExpired(started);
                    } while (Elapsed(retryStarted) < retryAfter);
                }
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

    private async ValueTask<RateLimitLease> AcquireWithinBudgetAsync(
        long started,
        bool holdsWaitSlot,
        CancellationToken cancellationToken
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        ThrowIfBudgetExpired(started);
        var localSlot = false;
        CancellationTokenSource? deadline = null;
        RateLimitLease lease;
        try
        {
            if (_limiter is not DistributedRateLimiter && !holdsWaitSlot)
            {
                // Reserve a wait slot before calling a limiter that may enqueue and displace
                // an older waiter. Even when full, an immediately available permit can be used.
                if (_options.MaxWait == TimeSpan.Zero || !_waitSlots.Wait(0, cancellationToken))
                {
                    return _limiter.AttemptAcquire(1);
                }
                localSlot = true;
            }
            if (_options.MaxWait > TimeSpan.Zero)
            {
                deadline = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            }
            var acquisition = _limiter.AcquireAsync(1, deadline?.Token ?? cancellationToken);
            if (acquisition.IsCompletedSuccessfully)
            {
                lease = acquisition.Result;
            }
            else
            {
                var pending = acquisition.AsTask();
                try
                {
                    lease = await WaitForDecisionAsync(pending, started, cancellationToken)
                        .ConfigureAwait(false);
                }
                catch
                {
#pragma warning disable CA2025 // Ownership transfers to the observer, including the cancellation source and eventual lease.
                    _ = DisposeAbandonedLeaseAsync(pending, deadline);
#pragma warning restore CA2025
                    deadline = null;
                    throw;
                }
            }
        }
        finally
        {
            if (localSlot)
            {
                _waitSlots.Release();
            }
            deadline?.Dispose();
        }

        try
        {
            cancellationToken.ThrowIfCancellationRequested();
            ThrowIfBudgetExpired(started);
            return lease;
        }
        catch
        {
            lease.Dispose();
            throw;
        }
    }

    private async Task<RateLimitLease> WaitForDecisionAsync(
        Task<RateLimitLease> pending,
        long started,
        CancellationToken cancellationToken
    )
    {
        if (_options.MaxWait == TimeSpan.Zero)
        {
            // The distributed limiter sends a single backend decision without queueing.
            return await pending.WaitAsync(cancellationToken).ConfigureAwait(false);
        }
        while (true)
        {
            cancellationToken.ThrowIfCancellationRequested();
            var remaining = _options.MaxWait - Elapsed(started);
            if (remaining <= TimeSpan.Zero)
            {
                throw Rejected(started);
            }
            try
            {
                return await pending
                    .WaitAsync(
                        remaining > MaximumTimerDelay ? MaximumTimerDelay : remaining,
                        _timeProvider,
                        cancellationToken
                    )
                    .ConfigureAwait(false);
            }
            catch (TimeoutException) when (!pending.IsFaulted)
            {
                // Long budgets span several timers. Keep waiting for the original acquisition.
            }
        }
    }

    private static async Task DisposeAbandonedLeaseAsync(
        Task<RateLimitLease> pending,
        CancellationTokenSource? deadline
    )
    {
        try
        {
            if (deadline is not null)
            {
                await deadline.CancelAsync().ConfigureAwait(ConfigureAwaitOptions.SuppressThrowing);
            }
            using var lease = await pending.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // Observe a late fault or dispose the eventual lease after the request ends.
        }
        finally
        {
            deadline?.Dispose();
        }
    }

    private void ThrowIfBudgetExpired(long started)
    {
        if (_options.MaxWait > TimeSpan.Zero && Elapsed(started) >= _options.MaxWait)
        {
            throw Rejected(started);
        }
    }

    private RateLimitExceededException Rejected(long started, TimeSpan? retryAfter = null)
    {
        HttpRateLimitingDiagnostics.Record("rejected", Elapsed(started));
        return new RateLimitExceededException(retryAfter);
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
