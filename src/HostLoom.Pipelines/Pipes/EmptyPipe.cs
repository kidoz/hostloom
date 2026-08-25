namespace HostLoom.Pipelines;

internal sealed class EmptyPipe<TContext> : IPipe<TContext>
    where TContext : class, IPipeContext
{
    public static EmptyPipe<TContext> Instance { get; } = new();

    private EmptyPipe() { }

    public ValueTask SendAsync(TContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        context.CancellationToken.ThrowIfCancellationRequested();
        return ValueTask.CompletedTask;
    }

    public void Probe(IProbeContext context) => context.CreateScope("empty");
}
