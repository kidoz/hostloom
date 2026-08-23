namespace HostLoom.Pipelines;

public interface IPipe<in TContext> : IProbeSite where TContext : class, IPipeContext
{
    ValueTask SendAsync(TContext context);
}
