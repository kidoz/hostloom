namespace HostLoom.Pipelines;

internal sealed class RateLimitFilter<TContext> : IFilter<TContext>
    where TContext : class, IPipeContext
{
    private readonly int _limit;
    private readonly TimeSpan _interval;
    private readonly TimeProvider _timeProvider;
    private readonly Lock _gate = new();
    private long _windowStart;
    private int _used;

    public RateLimitFilter(int limit, TimeSpan interval, TimeProvider timeProvider)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _limit = limit;
        _interval = interval;
        _timeProvider = timeProvider;
        _windowStart = timeProvider.GetTimestamp();
    }

    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        // Waits rather than throwing: a rate limit shapes throughput, whereas a tripped circuit
        // breaker reports that the downstream is unavailable.
        while (!TryTakePermit(out var wait))
        {
            await Task.Delay(wait, _timeProvider, context.CancellationToken).ConfigureAwait(false);
        }

        await next.SendAsync(context).ConfigureAwait(false);
    }

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope("rateLimit");
        scope.Set("limit", _limit);
        scope.Set("interval", _interval);
    }

    private bool TryTakePermit(out TimeSpan wait)
    {
        lock (_gate)
        {
            var now = _timeProvider.GetTimestamp();
            var elapsed = _timeProvider.GetElapsedTime(_windowStart, now);
            if (elapsed >= _interval)
            {
                _windowStart = now;
                _used = 0;
            }

            if (_used < _limit)
            {
                _used++;
                wait = TimeSpan.Zero;
                return true;
            }

            wait = _interval - elapsed;
            if (wait <= TimeSpan.Zero)
            {
                wait = TimeSpan.FromMilliseconds(1);
            }

            return false;
        }
    }
}
