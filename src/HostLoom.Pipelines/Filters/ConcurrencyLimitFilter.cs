namespace HostLoom.Pipelines;

internal sealed class ConcurrencyLimitFilter<TContext> : IFilter<TContext>
    where TContext : class, IPipeContext
{
    private readonly int _limit;
    private readonly SemaphoreSlim _semaphore;

    public ConcurrencyLimitFilter(int limit)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(limit, 1);
        _limit = limit;
        _semaphore = new SemaphoreSlim(limit, limit);
    }

    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        await _semaphore.WaitAsync(context.CancellationToken).ConfigureAwait(false);
        try
        {
            await next.SendAsync(context).ConfigureAwait(false);
        }
        finally
        {
            _semaphore.Release();
        }
    }

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope("concurrencyLimit");
        scope.Set("limit", _limit);
    }
}
