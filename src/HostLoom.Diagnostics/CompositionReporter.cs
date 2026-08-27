using Microsoft.Extensions.Hosting;

namespace HostLoom.Diagnostics;

/// <summary>
/// Reports the composition once, when the host starts. Hosted services start in registration
/// order, so declaring the diagnostics before the services being described puts the manifest at
/// the top of the startup log, where it belongs.
/// </summary>
internal sealed class CompositionReporter(IServiceProvider serviceProvider) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        CompositionDiagnostics.Report(serviceProvider);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
