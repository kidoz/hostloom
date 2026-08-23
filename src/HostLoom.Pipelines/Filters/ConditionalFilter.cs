namespace HostLoom.Pipelines;

internal sealed class ConditionalFilter<TContext>(
    string name,
    Func<TContext, bool> predicate,
    IReadOnlyList<IFilter<TContext>> filters) : IFilter<TContext> where TContext : class, IPipeContext
{
    public ValueTask SendAsync(TContext context, IPipe<TContext> next) =>
        predicate(context) ? PipeComposer.Compose(filters, next).SendAsync(context) : next.SendAsync(context);

    public void Probe(IProbeContext context)
    {
        var scope = context.CreateScope(name);
        scope.Set("condition", predicate.Method.Name);
        foreach (var filter in filters) filter.Probe(scope);
    }
}
