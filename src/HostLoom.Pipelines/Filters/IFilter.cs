namespace HostLoom.Pipelines;

public interface IFilter<TContext> : IProbeSite
    where TContext : class, IPipeContext
{
    ValueTask SendAsync(TContext context, IPipe<TContext> next);
}
