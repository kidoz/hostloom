using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Options;

namespace HostLoom.Locking.DependencyInjection;

/// <summary>
/// Readiness: can locks be acquired right now? Asks the provider's probe when it has one and
/// otherwise reports healthy with the reason, so a silent provider is never mistaken for a
/// broken one.
/// </summary>
internal sealed class LockProviderReadinessCheck(string? profile, IServiceProvider provider)
    : IHealthCheck
{
    private readonly LockingOptions _options = provider
        .GetRequiredService<IOptionsMonitor<LockingOptions>>()
        .Get(LockingProfile.OptionsName(profile));
    private readonly ILockProviderHealthProbe? _probe =
        LockingProfile.Get<ILockProviderHealthProbe>(provider, profile);

    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
    {
        if (!_options.Enabled)
        {
            return HealthCheckResult.Healthy(
                "HostLoom locking is disabled (Locking:Enabled = false); nothing to reach."
            );
        }

        var probe = _probe;
        if (probe is null)
        {
            return HealthCheckResult.Healthy(
                "The lock provider does not report health; readiness assumes it is reachable."
            );
        }

        var health = await probe.CheckHealthAsync(cancellationToken).ConfigureAwait(false);
        return health.IsHealthy
            ? HealthCheckResult.Healthy(health.Description)
            : HealthCheckResult.Unhealthy(health.Description);
    }
}
