using Microsoft.Extensions.Hosting;

namespace HostLoom.Scheduling.DependencyInjection;

/// <summary>Starts the composed <see cref="Scheduler"/> with the host and stops it with the host.</summary>
internal sealed class SchedulerHostedService(Scheduler scheduler) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken) =>
        scheduler.StartAsync(cancellationToken);

    public Task StopAsync(CancellationToken cancellationToken) =>
        scheduler.StopAsync(cancellationToken);
}
