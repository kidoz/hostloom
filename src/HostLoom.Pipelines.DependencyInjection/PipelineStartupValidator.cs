using Microsoft.Extensions.Hosting;

namespace HostLoom.Pipelines.DependencyInjection;

/// <summary>
/// Fails host startup when a pipeline is misconfigured — a duplicate name or a filter with a
/// missing constructor dependency — instead of letting the first run discover it.
/// </summary>
internal sealed class PipelineStartupValidator(IServiceProvider serviceProvider) : IHostedService
{
    public Task StartAsync(CancellationToken cancellationToken)
    {
        PipelineValidator.Validate(serviceProvider);
        return Task.CompletedTask;
    }

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
