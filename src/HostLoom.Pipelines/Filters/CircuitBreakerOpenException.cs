namespace HostLoom.Pipelines;

/// <summary>Thrown instead of invoking the rest of the pipeline while the circuit breaker is open.</summary>
public sealed class CircuitBreakerOpenException : Exception
{
    public CircuitBreakerOpenException(TimeSpan resetInterval)
        : base($"The circuit breaker is open and admits a trial call after {resetInterval}.") =>
        ResetInterval = resetInterval;

    public TimeSpan ResetInterval { get; }
}
