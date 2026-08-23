namespace HostLoom.Pipelines;

internal sealed class CircuitBreakerFilter<TContext> : IFilter<TContext> where TContext : class, IPipeContext
{
    private readonly int _failureThreshold;
    private readonly TimeSpan _resetInterval;
    private readonly TimeProvider _timeProvider;
    private readonly Lock _gate = new();
    private CircuitState _state = CircuitState.Closed;
    private int _consecutiveFailures;
    private DateTimeOffset _openedAt;

    public CircuitBreakerFilter(int failureThreshold, TimeSpan resetInterval, TimeProvider timeProvider)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(failureThreshold, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(resetInterval, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _failureThreshold = failureThreshold;
        _resetInterval = resetInterval;
        _timeProvider = timeProvider;
    }

    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        if (!TryEnter())
        {
            throw new CircuitBreakerOpenException(_resetInterval);
        }

        try
        {
            await next.SendAsync(context).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            OnCancelled();
            throw;
        }
        catch (Exception)
        {
            OnFailure();
            throw;
        }

        OnSuccess();
    }

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope("circuitBreaker");
        scope.Set("failureThreshold", _failureThreshold);
        scope.Set("resetInterval", _resetInterval);
        lock (_gate)
        {
            scope.Set("state", _state.ToString());
        }
    }

    private bool TryEnter()
    {
        lock (_gate)
        {
            switch (_state)
            {
                case CircuitState.Closed:
                    return true;
                case CircuitState.HalfOpen:
                    // One trial at a time; concurrent callers keep being rejected until it resolves.
                    return false;
                default:
                    if (_timeProvider.GetUtcNow() - _openedAt < _resetInterval)
                    {
                        return false;
                    }

                    _state = CircuitState.HalfOpen;
                    return true;
            }
        }
    }

    private void OnSuccess()
    {
        lock (_gate)
        {
            _state = CircuitState.Closed;
            _consecutiveFailures = 0;
        }
    }

    private void OnFailure()
    {
        lock (_gate)
        {
            if (_state == CircuitState.HalfOpen || ++_consecutiveFailures >= _failureThreshold)
            {
                _state = CircuitState.Open;
                _openedAt = _timeProvider.GetUtcNow();
                _consecutiveFailures = 0;
            }
        }
    }

    private void OnCancelled()
    {
        lock (_gate)
        {
            // A cancelled trial proves nothing about the downstream. Reopen so the next caller gets
            // a fresh trial after the reset interval, rather than leaving the circuit stuck half-open.
            if (_state == CircuitState.HalfOpen)
            {
                _state = CircuitState.Open;
                _openedAt = _timeProvider.GetUtcNow();
            }
        }
    }
}
