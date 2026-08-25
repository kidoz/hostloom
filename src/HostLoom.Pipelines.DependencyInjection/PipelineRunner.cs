using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Pipelines.DependencyInjection;

internal sealed class PipelineRunner<TContext>(
    PipelineDefinition<TContext> definition,
    IServiceScopeFactory scopeFactory
) : IPipelineRunner<TContext>
    where TContext : class, IPipeContext
{
    public string PipelineName => definition.Name;

    public PipelineTopology Topology => definition.Topology;

    public async ValueTask RunAsync(TContext context)
    {
        ArgumentNullException.ThrowIfNull(context);

        using var activity = PipelineRunnerDiagnostics.ActivitySource.StartActivity(
            "hostloom pipeline run"
        );
        activity?.SetTag("hostloom.pipeline.name", definition.Name);

        var tags = new TagList { { "hostloom.pipeline.name", definition.Name } };
        var start = Stopwatch.GetTimestamp();
        PipelineRunnerDiagnostics.ActiveRuns.Add(1, tags);
        var outcome = "success";
        try
        {
            var scope = scopeFactory.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                var pipe = BuildPipe(scope.ServiceProvider);
                await pipe.SendAsync(context).ConfigureAwait(false);
            }
        }
        catch (OperationCanceledException)
        {
            outcome = "canceled";
            throw;
        }
        catch (Exception exception)
        {
            outcome = "failure";
            activity?.SetStatus(ActivityStatusCode.Error, exception.Message);
            throw;
        }
        finally
        {
            PipelineRunnerDiagnostics.ActiveRuns.Add(-1, tags);
            tags.Add("hostloom.pipeline.outcome", outcome);
            PipelineRunnerDiagnostics.RunDuration.Record(
                Stopwatch.GetElapsedTime(start).TotalSeconds,
                tags
            );
        }
    }

    // Rebuilt per run: filters are transient with scoped dependencies, and EnabledWhen is a
    // per-run decision. The build itself is a list walk and a fold, cheap next to any filter.
    private IPipe<TContext> BuildPipe(IServiceProvider provider)
    {
        var builder = new PipeBuilder<TContext>();
        foreach (var configure in definition.OuterFilters)
        {
            configure(builder);
        }

        foreach (var stage in definition.Stages)
        {
            foreach (var registration in stage.Filters)
            {
                if (registration.EnabledWhen is not null && !registration.EnabledWhen(provider))
                {
                    continue;
                }

                var filter =
                    (IFilter<TContext>)
                        provider.GetRequiredKeyedService(
                            registration.FilterType,
                            registration.ServiceKey
                        );
                builder.Use(
                    definition.Instrumented
                        ? new InstrumentedFilter<TContext>(
                            filter,
                            definition.Name,
                            stage.Name,
                            registration.Name
                        )
                        : filter
                );
            }
        }

        return builder.Build();
    }
}
