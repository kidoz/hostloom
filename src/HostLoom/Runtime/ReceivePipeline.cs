using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom;

/// <summary>
/// The filters configured around handler execution, shared by requests and events.
/// </summary>
/// <remarks>
/// Composed once, not per delivery: a circuit breaker or rate limit is only meaningful if its state
/// is shared across everything the process receives. One pipeline means one verdict — a breaker
/// tripped by failing requests also rejects events, which is the intended reading of "the receive
/// pipeline" rather than an accident.
/// </remarks>
internal sealed class ReceivePipeline
{
    private readonly IPipe<ReceiveContext> _pipe;

    public ReceivePipeline(HostLoomConfiguration configuration, IServiceScopeFactory scopeFactory)
    {
        _pipe = Pipe.Create<ReceiveContext>(builder =>
        {
            configuration.ReceivePipeline?.Invoke(builder);
            builder.Use(new ExecuteReceiveFilter(scopeFactory));
        });
    }

    public ValueTask SendAsync(ReceiveContext context) => _pipe.SendAsync(context);

    public ProbeResult Probe(CancellationToken cancellationToken = default) =>
        PipelineProbe.Inspect(_pipe, cancellationToken);
}
