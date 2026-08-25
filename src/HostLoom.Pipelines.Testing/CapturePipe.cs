namespace HostLoom.Pipelines.Testing;

/// <summary>
/// A stand-in for the <c>next</c> pipe when unit-testing one filter without composing a pipeline.
/// Records every context it receives; set <see cref="Fault"/> to make the downstream call fail.
/// </summary>
public sealed class CapturePipe<TContext> : IPipe<TContext>
    where TContext : class, IPipeContext
{
    private readonly Lock _gate = new();
    private readonly List<TContext> _sent = [];

    /// <summary>Thrown by every subsequent send once set.</summary>
    public Exception? Fault { get; set; }

    public IReadOnlyList<TContext> Sent
    {
        get
        {
            lock (_gate)
            {
                return _sent.ToArray();
            }
        }
    }

    public bool WasSent => Sent.Count > 0;

    public ValueTask SendAsync(TContext context)
    {
        ArgumentNullException.ThrowIfNull(context);
        lock (_gate)
        {
            _sent.Add(context);
        }

        return Fault is null ? ValueTask.CompletedTask : ValueTask.FromException(Fault);
    }

    public void Probe(IProbeContext context) => context.CreateScope("capture");
}
