using HostLoom.Pipelines;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom;

/// <summary>
/// Terminal filter of the receive pipeline. Opens the scope and runs the delivery's handlers.
/// </summary>
internal sealed class ExecuteReceiveFilter(IServiceScopeFactory scopeFactory) : IFilter<ReceiveContext>
{
    public async ValueTask SendAsync(ReceiveContext context, IPipe<ReceiveContext> next)
    {
        // The scope opens inside the filter, so a retrying pipeline gives every attempt a fresh
        // one. Reusing the scope would hand the retry the scoped state a failed attempt left behind.
        var scope = scopeFactory.CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
        {
            await context.InvokeAsync(scope.ServiceProvider, context.CancellationToken).ConfigureAwait(false);
        }
    }

    public void Probe(IProbeContext context) => context.CreateScope("executeReceive");
}
