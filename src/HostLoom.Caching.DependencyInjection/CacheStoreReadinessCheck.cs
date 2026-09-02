using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace HostLoom.Caching.DependencyInjection;

/// <summary>
/// Readiness: can the distributed store answer right now? Asks the store's
/// <see cref="ICacheStoreHealthProbe"/> when it registered one, and does not invent a verdict
/// when it did not. Never used for liveness, so a store outage cannot become a restart storm.
/// </summary>
internal sealed class CacheStoreReadinessCheck(IServiceProvider provider) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
    {
        if (provider.GetService(typeof(ICacheStoreHealthProbe)) is not ICacheStoreHealthProbe probe)
        {
            return HealthCheckResult.Healthy(
                "The cache store does not report health; the in-process tier is always available."
            );
        }

        var health = await probe.CheckHealthAsync(cancellationToken).ConfigureAwait(false);
        return health.IsHealthy
            ? HealthCheckResult.Healthy(health.Description)
            : HealthCheckResult.Unhealthy(health.Description);
    }
}
