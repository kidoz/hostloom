namespace HostLoom.Pipelines;

/// <summary>Circuit breaker states, following the standard closed/open/half-open model.</summary>
public enum CircuitState
{
    /// <summary>Calls flow through normally.</summary>
    Closed,

    /// <summary>Calls are rejected until the reset interval elapses.</summary>
    Open,

    /// <summary>A single trial call is allowed through to test whether the downstream recovered.</summary>
    HalfOpen,
}
