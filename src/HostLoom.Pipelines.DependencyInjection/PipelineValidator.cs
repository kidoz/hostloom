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

                    // Validate exactly what a run would construct. A filter switched off for this
                    // environment is never resolved by the runner, so demanding that it be
                    // constructible here would refuse to start the host over a filter that would
                    // never execute — which is the whole point of turning it off.
                    if (
                        filter.EnabledWhen is not null
                        && !filter.EnabledWhen(scope.ServiceProvider)
                    )
                    {
                        continue;
                    }

                    try
                    {
                        _ = scope.ServiceProvider.GetRequiredKeyedService(
                            filter.FilterType,
                            filter.ServiceKey
                        );
                    }
                    catch (Exception exception) when (exception is not OperationCanceledException)
                    {
                        // Any constructor failure is the same startup problem, not only the
                        // container's own InvalidOperationException for a missing dependency.
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
