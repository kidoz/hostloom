namespace HostLoom.Pipelines;

internal static class PipeComposer
{
    public static IPipe<TContext> Compose<TContext>(
        IReadOnlyList<IFilter<TContext>> filters,
        IPipe<TContext> tail
    )
        where TContext : class, IPipeContext
    {
        var pipe = tail;
        for (var index = filters.Count - 1; index >= 0; index--)
            pipe = new FilterPipe<TContext>(filters[index], pipe);
        return pipe;
    }
}
