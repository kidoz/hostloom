namespace HostLoom.Pipelines;

internal sealed class TerminalFilter<TContext>(string name, Func<TContext, ValueTask> action)
    : IFilter<TContext> where TContext : class, IPipeContext
{
    public ValueTask SendAsync(TContext context, IPipe<TContext> next) => action(context);

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope(name);
        scope.Set("terminal", true);
    }
}
