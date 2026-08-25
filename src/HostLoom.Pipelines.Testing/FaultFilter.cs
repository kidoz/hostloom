namespace HostLoom.Pipelines.Testing;

/// <summary>
/// Fails the first <c>failures</c> sends, then passes through — the deterministic way to exercise
/// retry, circuit-breaker, and error-isolation behaviour.
/// </summary>
public sealed class FaultFilter<TContext> : IFilter<TContext>
    where TContext : class, IPipeContext
{
    private readonly int _failures;
    private readonly Func<Exception> _exceptionFactory;
    private int _sends;

    public FaultFilter(int failures, Func<Exception>? exceptionFactory = null)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(failures);
        _failures = failures;
        _exceptionFactory =
            exceptionFactory
            ?? (static () => new InvalidOperationException("Injected test fault."));
    }

    public int Sends => Volatile.Read(ref _sends);

    public ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        ArgumentNullException.ThrowIfNull(next);
        return Interlocked.Increment(ref _sends) <= _failures
            ? ValueTask.FromException(_exceptionFactory())
            : next.SendAsync(context);
    }

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope("fault");
        scope.Set("failures", _failures);
    }
}
