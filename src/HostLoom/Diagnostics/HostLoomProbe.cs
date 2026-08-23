using HostLoom.Pipelines;

namespace HostLoom;

/// <summary>
/// Structural diagnostics for the HostLoom runtime. Execution-free, so it is safe to call from a
/// health or debug endpoint on every request.
/// </summary>
public sealed class HostLoomProbe
{
    private readonly ReceivePipeline _pipeline;

    internal HostLoomProbe(ReceivePipeline pipeline) => _pipeline = pipeline;

    /// <summary>Immutable tree of the filters composed around handler execution.</summary>
    public ProbeResult ReceivePipeline(CancellationToken cancellationToken = default) =>
        _pipeline.Probe(cancellationToken);
}
