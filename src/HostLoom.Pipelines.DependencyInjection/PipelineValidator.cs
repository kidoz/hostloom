using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;

namespace HostLoom.Pipelines.DependencyInjection;

/// <summary>
/// Validates every registered pipeline and logs each resolved topology. The generic host runs
/// this automatically at startup; call it directly when composing a provider without a host.
/// </summary>
public static class PipelineValidator
{
    public static async ValueTask ValidateAsync(
        IServiceProvider services,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        var definitions = services.GetServices<IPipelineDefinition>().ToList();

        var duplicates = definitions
            .GroupBy(definition => definition.Name, StringComparer.Ordinal)
            .Where(group => group.Count() > 1)
            .Select(group => group.Key)
            .ToList();
        if (duplicates.Count > 0)
        {
            throw new InvalidOperationException(
                $"Duplicate pipeline names: {string.Join(", ", duplicates)}. Pipeline names must be unique per service provider."
            );
        }

        var logger = services
            .GetService<ILoggerFactory>()
            ?.CreateLogger("HostLoom.Pipelines.DependencyInjection.PipelineValidator");
        var scope = services.GetRequiredService<IServiceScopeFactory>().CreateAsyncScope();
        await using (scope.ConfigureAwait(false))
            foreach (var definition in definitions)
            {
                foreach (var filter in definition.Filters)
                {
                    cancellationToken.ThrowIfCancellationRequested();
                    try
                    {
                        _ = scope.ServiceProvider.GetRequiredKeyedService(
                            filter.FilterType,
                            filter.ServiceKey
                        );
                    }
                    catch (InvalidOperationException exception)
                    {
                        throw new InvalidOperationException(
                            $"Pipeline '{definition.Name}' cannot construct filter '{filter.Name}' "
                                + $"of type '{filter.FilterType.Name}'. "
                                + "Register its constructor dependencies, or the pipeline will fail on its first run.",
                            exception
                        );
                    }
                }

                if (logger?.IsEnabled(LogLevel.Information) == true)
                {
                    logger.LogInformation(
                        "HostLoom pipeline '{PipelineName}' topology: {Topology}",
                        definition.Name,
                        definition.Topology.Describe()
                    );
                }
            }
    }
}
