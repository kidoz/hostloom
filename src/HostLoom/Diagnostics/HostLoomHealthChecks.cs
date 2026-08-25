using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace HostLoom;

/// <summary>
/// Liveness: is this process worth keeping alive? Deliberately shallow. It never contacts the
/// broker, because a broker outage answering "restart me" turns one outage into a restart storm
/// across every pod that talks to it. Broker reachability belongs in readiness.
/// </summary>
internal sealed class HostLoomLivenessCheck(EndpointRuntimeState state) : IHealthCheck
{
    public Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    ) =>
        Task.FromResult(
            HealthCheckResult.Healthy(
                state.Listening
                    ? $"HostLoom is running with {state.EndpointCount} endpoint(s)."
                    : "HostLoom is running."
            )
        );
}

/// <summary>
/// Readiness: can this instance serve requests right now? Endpoints must be listening, and the
/// broker must be reachable when the transport can tell us.
/// </summary>
internal sealed class HostLoomReadinessCheck(
    HostLoomConfiguration configuration,
    EndpointRuntimeState state,
    IRequestBroker broker
) : IHealthCheck
{
    public async Task<HealthCheckResult> CheckHealthAsync(
        HealthCheckContext context,
        CancellationToken cancellationToken = default
    )
    {
        var expected = configuration.Endpoints.Count;
        if (expected > 0 && !state.Listening)
        {
            return HealthCheckResult.Unhealthy(
                $"HostLoom has {expected} endpoint(s) that are not listening yet."
            );
        }

        if (broker is not IBrokerHealthProbe probe)
        {
            // The transport cannot report, so do not invent a verdict.
            return HealthCheckResult.Healthy(
                expected == 0
                    ? "HostLoom is client-only; the transport does not report broker health."
                    : $"HostLoom is listening on {state.EndpointCount} endpoint(s); the transport does not report broker health."
            );
        }

        var health = await probe.CheckHealthAsync(cancellationToken).ConfigureAwait(false);
        return health.IsHealthy
            ? HealthCheckResult.Healthy(health.Description)
            : HealthCheckResult.Unhealthy(health.Description);
    }
}
