namespace HostLoom.Pipelines;

internal sealed class ExecuteFilter<TContext>(string name, Func<TContext, ValueTask> action)
    : IFilter<TContext> where TContext : class, IPipeContext
{
    public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
    {
        await action(context).ConfigureAwait(false);
        await next.SendAsync(context).ConfigureAwait(false);
    }

    public void Probe(IProbeContext context) => context.CreateScope(name);
}
