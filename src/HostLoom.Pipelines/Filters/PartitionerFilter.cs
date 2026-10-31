namespace HostLoom.Pipelines;

/// <summary>
/// Runs one call at a time for each key, on this process. Another process can run the same key
/// concurrently. A null key is refused so every caller cannot silently share one lane.
/// </summary>
internal sealed class PartitionerFilter<TContext> : IFilter<TContext>
    where TContext : class, IPipeContext
{
    private readonly int _partitionCount;
    private readonly Func<TContext, object> _keySelector;
    private readonly SemaphoreSlim[] _lanes;

    public PartitionerFilter(int partitionCount, Func<TContext, object> keySelector)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partitionCount, 1);
        ArgumentNullException.ThrowIfNull(keySelector);
        _partitionCount = partitionCount;
        _keySelector = keySelector;
        _lanes = new SemaphoreSlim[partitionCount];
        for (var index = 0; index < partitionCount; index++)
        {
            _lanes[index] = new SemaphoreSlim(1, 1);
        }
    }

    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        var key =
            _keySelector(context)
            ?? throw new InvalidOperationException("The partition key selector returned null.");
        var lane = _lanes[(uint)key.GetHashCode() % (uint)_partitionCount];
        await lane.WaitAsync(context.CancellationToken).ConfigureAwait(false);
        try
        {
            await next.SendAsync(context).ConfigureAwait(false);
        }
        finally
        {
            lane.Release();
        }
    }

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope("partitioner");
        scope.Set("partitionCount", _partitionCount);
    }
}
