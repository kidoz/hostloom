namespace HostLoom.Pipelines.Testing;

public static class PipeHarness
{
    /// <summary>Composes a pipe for a test and returns a harness that captures the outcome of each send.</summary>
    public static PipeHarness<TContext> For<TContext>(Action<PipeBuilder<TContext>> configure)
        where TContext : class, IPipeContext
    {
        ArgumentNullException.ThrowIfNull(configure);
        return new PipeHarness<TContext>(Pipe.Create(configure));
    }
}

public sealed class PipeHarness<TContext>
    where TContext : class, IPipeContext
{
    internal PipeHarness(IPipe<TContext> pipe) => Pipe = pipe;

    public IPipe<TContext> Pipe { get; }

    public ProbeResult Probe(CancellationToken cancellationToken = default) =>
        PipelineProbe.Inspect(Pipe, cancellationToken);

    /// <summary>Sends and captures instead of throwing, so a test asserts on the result either way.</summary>
    public async ValueTask<PipeSendResult<TContext>> SendAsync(TContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        try
        {
            await Pipe.SendAsync(context).ConfigureAwait(false);
            return new PipeSendResult<TContext>(context, null);
        }
        catch (Exception exception)
        {
            return new PipeSendResult<TContext>(context, exception);
        }
    }
}

/// <summary>The outcome of one harness send: the context as the pipeline left it, and the fault if any.</summary>
public sealed class PipeSendResult<TContext>
    where TContext : class, IPipeContext
{
    internal PipeSendResult(TContext context, Exception? exception)
    {
        Context = context;
        Exception = exception;
    }

    public TContext Context { get; }

    public Exception? Exception { get; }

    public bool Completed => Exception is null;
}
