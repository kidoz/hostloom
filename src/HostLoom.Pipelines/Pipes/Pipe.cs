namespace HostLoom.Pipelines;

public static class Pipe
{
    public static IPipe<TContext> Create<TContext>(Action<PipeBuilder<TContext>> configure)
        where TContext : class, IPipeContext
    {
        ArgumentNullException.ThrowIfNull(configure);
        var builder = new PipeBuilder<TContext>();
        configure(builder);
        return builder.Build();
    }

    public static IPipe<TContext> Empty<TContext>() where TContext : class, IPipeContext => EmptyPipe<TContext>.Instance;
}
