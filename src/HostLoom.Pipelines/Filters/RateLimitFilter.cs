namespace HostLoom.Pipelines;

internal sealed class RateLimitFilter<TContext> : IFilter<TContext>
    where TContext : class, IPipeContext
{
    private readonly int _limit;
    private readonly TimeSpan _interval;
    private readonly TimeProvider _timeProvider;
    private readonly Lock _gate = new();
    private DateTimeOffset _windowStart;
    private int _used;

    public RateLimitFilter(int limit, TimeSpan interval, TimeProvider timeProvider)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(interval, TimeSpan.Zero);
        ArgumentNullException.ThrowIfNull(timeProvider);
        _limit = limit;
        _interval = interval;
        _timeProvider = timeProvider;
        _windowStart = timeProvider.GetUtcNow();
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
            var now = _timeProvider.GetUtcNow();
            if (now - _windowStart >= _interval)
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

            wait = _interval - (now - _windowStart);
            if (wait <= TimeSpan.Zero)
            {
                wait = TimeSpan.FromMilliseconds(1);
            }

            return false;
        }
    }
}
