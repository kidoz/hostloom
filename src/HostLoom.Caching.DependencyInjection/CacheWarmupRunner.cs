using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Caching.DependencyInjection;

/// <summary>Progress of the registered warmups, shared by the runner and the readiness check.</summary>
internal sealed class CacheWarmupState
{
    private readonly TaskCompletionSource _completed = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    public bool Completed => _completed.Task.IsCompleted;

    public int Succeeded { get; private set; }

    public int Failed { get; private set; }

    public void Record(bool succeeded)
    {
        if (succeeded)
        {
            Succeeded++;
        }
        else
        {
            Failed++;
        }
    }

    public void MarkCompleted() => _completed.TrySetResult();
}

/// <summary>
/// Runs every <see cref="ICacheWarmup"/> once after the host starts, in the background so a slow
/// warmup never delays startup. Whether readiness waits is
/// <see cref="CacheWarmupOptions.BlocksReadiness"/>'s decision, not this class's.
/// </summary>
internal sealed class CacheWarmupRunner(
    ICache cache,
    IEnumerable<ICacheWarmup> warmups,
    CacheWarmupState state,
    ILogger<CacheWarmupRunner> logger
) : IHostedService
{
    private static readonly EventId WarmupCompleted = new(1201, "CacheWarmupCompleted");
    private readonly CancellationTokenSource _stopping = new();
    private Task? _run;

    public Task StartAsync(CancellationToken cancellationToken)
    {
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
        try
        {
            foreach (var warmup in warmups)
            {
                var name = warmup.GetType().Name;
                try
                {
                    await warmup.WarmupAsync(cache, cancellationToken).ConfigureAwait(false);
                    state.Record(succeeded: true);
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
                    return;
                }
                catch (Exception exception)
                {
                    // Fail-open: a warmup that cannot run leaves the cache to fill on demand.
                    state.Record(succeeded: false);
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
            state.MarkCompleted();
        }
    }
}

/// <summary>
/// Readiness contributor for warmups. Reports unhealthy until every warmup has finished when
/// <see cref="CacheWarmupOptions.BlocksReadiness"/> is set, and healthy otherwise. Registered
/// regardless of the store, so the flag means the same thing on every backend.
/// </summary>
internal sealed class CacheWarmupReadinessCheck(
    CacheWarmupState state,
    IOptions<CachingOptions> options
) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
    {
        var blocks = options.Value.Warmup.BlocksReadiness;
        if (state.Completed)
        {
            return Task.FromResult(
                HealthCheckResult.Healthy(
                    $"Cache warmup completed: {state.Succeeded} succeeded, {state.Failed} failed."
                )
            );
        }

        return Task.FromResult(
            blocks
                ? HealthCheckResult.Unhealthy(
                    "Cache warmup is still running and Caching:Warmup:BlocksReadiness is true."
                )
                : HealthCheckResult.Healthy(
                    "Cache warmup is still running; Caching:Warmup:BlocksReadiness is false."
                )
        );
    }
}
