using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace HostLoom.RateLimiting.DependencyInjection;

internal sealed class RateLimitStoreReadinessCheck(string? profile, IServiceProvider provider)
    : IHealthCheck
{
    private readonly IRateLimitStoreHealthProbe? _probe =
        RateLimitingProfile.Get<IRateLimitStoreHealthProbe>(provider, profile);

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
    {
        if (_probe is null)
        {
            return HealthCheckResult.Healthy(
                "The rate-limit store does not report health; readiness assumes it is reachable."
            );
        }

        var health = await _probe.CheckHealthAsync(cancellationToken).ConfigureAwait(false);
        return health.IsHealthy
            ? HealthCheckResult.Healthy(health.Description)
            : HealthCheckResult.Unhealthy(health.Description);
    }
}
