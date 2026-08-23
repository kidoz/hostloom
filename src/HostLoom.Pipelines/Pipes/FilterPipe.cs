namespace HostLoom.Pipelines;

internal sealed class FilterPipe<TContext>(IFilter<TContext> filter, IPipe<TContext> next) : IPipe<TContext>
    where TContext : class, IPipeContext
{
    public ValueTask SendAsync(TContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        context.CancellationToken.ThrowIfCancellationRequested();
        return filter.SendAsync(context, next);
    }

    public void Probe(IProbeContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        context.CancellationToken.ThrowIfCancellationRequested();
        filter.Probe(context);
        next.Probe(context);
    }
}
