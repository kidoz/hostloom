namespace HostLoom.Pipelines;

internal sealed class DelegateFilter<TContext>(string name, Func<TContext, IPipe<TContext>, ValueTask> filter)
    : IFilter<TContext> where TContext : class, IPipeContext
{
    public ValueTask SendAsync(TContext context, IPipe<TContext> next) => filter(context, next);
    public void Probe(IProbeContext context) => context.CreateScope(name);
}
