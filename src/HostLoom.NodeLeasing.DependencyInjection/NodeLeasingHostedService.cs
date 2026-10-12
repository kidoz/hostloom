using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace HostLoom.NodeLeasing.DependencyInjection;

/// <summary>Starts every registered elector with the host and stops each with the host, releasing the leases they hold.</summary>
internal sealed class NodeLeasingHostedService(
    IServiceProvider provider,
    NodeLeasingRegistration registration
) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var pool in registration.Pools)
        {
            await provider
                .GetRequiredKeyedService<NodeIdElector>(pool)
                .StartAsync(cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var pool in registration.Pools)
        {
            await provider
                .GetRequiredKeyedService<NodeIdElector>(pool)
                .StopAsync(cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
