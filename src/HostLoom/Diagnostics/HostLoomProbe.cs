using HostLoom.Pipelines;

namespace HostLoom;

/// <summary>
/// Structural diagnostics for the HostLoom runtime. Execution-free, so it is safe to call from a
/// health or debug endpoint on every request.
/// </summary>
public sealed class HostLoomProbe
{
    private readonly MessageDispatcher _dispatcher;

    internal HostLoomProbe(MessageDispatcher dispatcher) => _dispatcher = dispatcher;

    /// <summary>Immutable tree of the filters composed around handler execution.</summary>
    public ProbeResult ReceivePipeline(CancellationToken cancellationToken = default) =>
        _dispatcher.ProbeReceivePipeline(cancellationToken);
}
