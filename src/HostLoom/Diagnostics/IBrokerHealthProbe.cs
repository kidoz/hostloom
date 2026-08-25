namespace HostLoom;

/// <summary>
/// Optional capability a transport may implement so readiness can report broker reachability.
/// A broker that does not implement it is treated as reachable, because "cannot tell" must not
/// read as "broken".
/// </summary>
public interface IBrokerHealthProbe
{
    /// <summary>
    /// Reports whether the broker is currently usable. Must not throw for an unreachable broker;
    /// return a failed result instead.
    /// </summary>
    ValueTask<BrokerHealth> CheckHealthAsync(CancellationToken cancellationToken);
}

/// <summary>Outcome of <see cref="IBrokerHealthProbe.CheckHealthAsync"/>.</summary>
/// <param name="IsHealthy">Whether the broker is usable right now.</param>
/// <param name="Description">Human-readable detail, surfaced in the health report.</param>
public sealed record BrokerHealth(bool IsHealthy, string Description)
{
    public static BrokerHealth Healthy(string description = "Broker is reachable.") =>
        new(true, description);

    public static BrokerHealth Unhealthy(string description) => new(false, description);
}
