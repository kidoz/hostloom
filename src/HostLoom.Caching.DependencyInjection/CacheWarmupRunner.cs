using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Caching.DependencyInjection;

internal enum CacheWarmupStatus
{
    NotStarted,
    InProgress,
    Succeeded,
    Failed,
    Cancelled,
}

/// <summary>Progress of the registered warmups, shared by the runner and the readiness check.</summary>
internal sealed class CacheWarmupState
{
    private int _status;
    private int _succeeded;
    private int _failed;

    public CacheWarmupStatus Status => (CacheWarmupStatus)Volatile.Read(ref _status);

    public int Succeeded => Volatile.Read(ref _succeeded);

    public int Failed => Volatile.Read(ref _failed);

    public void MarkStarted() => Volatile.Write(ref _status, (int)CacheWarmupStatus.InProgress);

    public void Record(bool succeeded) =>
        Interlocked.Increment(ref succeeded ? ref _succeeded : ref _failed);

    public void MarkCompleted(bool cancelled)
    {
        var status =
            cancelled ? CacheWarmupStatus.Cancelled
            : Failed > 0 ? CacheWarmupStatus.Failed
            : CacheWarmupStatus.Succeeded;
        Volatile.Write(ref _status, (int)status);
    }
}

/// <summary>
/// Runs every <see cref="ICacheWarmup"/> once after the host starts, in the background so a slow
/// warmup never delays startup. Whether readiness waits is
/// <see cref="CacheWarmupOptions.BlocksReadiness"/>'s decision, not this class's.
/// </summary>
internal sealed class CacheWarmupRunner(
    string? profile,
    IServiceProvider provider,
    ILogger<CacheWarmupRunner> logger
) : IHostedService
{
    private static readonly EventId WarmupCompleted = new(1201, "CacheWarmupCompleted");
    private readonly ICache _cache = CachingProfile.GetRequired<ICache>(provider, profile);
    private readonly IEnumerable<ICacheWarmup> _warmups = CachingProfile.GetRequired<
        IEnumerable<ICacheWarmup>
    >(provider, profile);
    private readonly CacheWarmupState _state = CachingProfile.GetRequired<CacheWarmupState>(
        provider,
        profile
    );
    private readonly CancellationTokenSource _stopping = new();
    private Task? _run;

    public Task StartAsync(CancellationToken cancellationToken)
    {
        _state.MarkStarted();
        _run = Task.Run(() => RunAsync(_stopping.Token), CancellationToken.None);
        return Task.CompletedTask;
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        await _stopping.CancelAsync().ConfigureAwait(false);
        if (_run is not null)
        {
            try
            {
                await _run.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
            catch (OperationCanceledException)
            {
                // Stopping: the warmups observed the token or the host gave up waiting.
            }
        }
    }

    private async Task RunAsync(CancellationToken cancellationToken)
    {
        var cancelled = false;
        try
        {
            foreach (var warmup in _warmups)
            {
                var name = warmup.GetType().Name;
                try
                {
                    await warmup.WarmupAsync(_cache, cancellationToken).ConfigureAwait(false);
                    _state.Record(succeeded: true);
                    if (logger.IsEnabled(LogLevel.Information))
                    {
                        logger.LogInformation(
                            WarmupCompleted,
                            "Cache warmup {Warmup} completed.",
                            name
                        );
                    }
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    cancelled = true;
                    return;
                }
                catch (Exception exception)
                {
                    // Fail-open: a warmup that cannot run leaves the cache to fill on demand.
                    _state.Record(succeeded: false);
                    logger.LogWarning(
                        new EventId(1202, "CacheWarmupFailed"),
                        exception,
                        "Cache warmup {Warmup} failed; entries will be filled on demand.",
                        name
                    );
                }
            }
        }
        finally
        {
            _state.MarkCompleted(cancelled);
        }
    }
}

/// <summary>
/// Readiness contributor for warmups. Reports unhealthy only while a run is in progress when
/// <see cref="CacheWarmupOptions.BlocksReadiness"/> is set. Terminal failure and cancellation stay
/// fail-open. Registered regardless of the store, so the flag means the same thing on every backend.
/// </summary>
internal sealed class CacheWarmupReadinessCheck(string? profile, IServiceProvider provider)
    : IHealthCheck
{
    private readonly CacheWarmupState _state = CachingProfile.GetRequired<CacheWarmupState>(
        provider,
        profile
    );
    private readonly CachingOptions _options = provider
        .GetRequiredService<IOptionsMonitor<CachingOptions>>()
        .Get(CachingProfile.OptionsName(profile));

    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
    {
        var status = _state.Status;
        var progress = $"{_state.Succeeded} succeeded, {_state.Failed} failed";
        if (status == CacheWarmupStatus.NotStarted)
        {
            return Task.FromResult(
                HealthCheckResult.Healthy($"Cache warmup not started: {progress}.")
            );
        }

        if (status != CacheWarmupStatus.InProgress)
        {
            var outcome = status switch
            {
                CacheWarmupStatus.Succeeded => "succeeded",
                CacheWarmupStatus.Failed => "failed",
                CacheWarmupStatus.Cancelled => "cancelled",
                _ => throw new InvalidOperationException($"Unexpected warmup status {status}."),
            };
            return Task.FromResult(
                HealthCheckResult.Healthy($"Cache warmup {outcome}: {progress}.")
            );
        }

        return Task.FromResult(
            _options.Warmup.BlocksReadiness
                ? HealthCheckResult.Unhealthy(
                    $"Cache warmup is still running: {progress}; Caching:Warmup:BlocksReadiness is true."
                )
                : HealthCheckResult.Healthy(
                    $"Cache warmup is still running: {progress}; Caching:Warmup:BlocksReadiness is false."
                )
        );
    }
}
