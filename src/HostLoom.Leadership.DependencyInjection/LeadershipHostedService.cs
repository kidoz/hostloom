using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;

namespace HostLoom.Leadership.DependencyInjection;

/// <summary>Starts every registered elector with the host and stops each with the host, releasing the leases they hold.</summary>
internal sealed class LeadershipHostedService(
    IServiceProvider provider,
    LeadershipRegistration registration
) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken)
    {
        foreach (var role in registration.Roles)
        {
            await provider
                .GetRequiredKeyedService<LeaderElector>(role)
                .StartAsync(cancellationToken)
                .ConfigureAwait(false);
        }
    }

    public async Task StopAsync(CancellationToken cancellationToken)
    {
        foreach (var role in registration.Roles)
        {
            await provider
                .GetRequiredKeyedService<LeaderElector>(role)
                .StopAsync(cancellationToken)
                .ConfigureAwait(false);
        }
    }
}
