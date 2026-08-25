using Microsoft.Extensions.Hosting;

namespace HostLoom.Pipelines.DependencyInjection;

/// <summary>
/// Fails host startup when a pipeline is misconfigured — a duplicate name or a filter with a
/// missing constructor dependency — instead of letting the first run discover it.
/// </summary>
internal sealed class PipelineStartupValidator(IServiceProvider serviceProvider) : IHostedService
{
    public async Task StartAsync(CancellationToken cancellationToken) =>
        await PipelineValidator
            .ValidateAsync(serviceProvider, cancellationToken)
            .ConfigureAwait(false);

    public Task StopAsync(CancellationToken cancellationToken) => Task.CompletedTask;
}
