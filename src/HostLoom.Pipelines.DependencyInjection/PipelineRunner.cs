using System.Diagnostics;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Pipelines.DependencyInjection;

internal sealed class PipelineRunner<TContext>(
    PipelineDefinition<TContext> definition,
    IServiceScopeFactory scopeFactory
) : IPipelineRunner<TContext>
    where TContext : class, IPipeContext
{
    // The run-level wrappers (retry, timeout) around a terminal that opens one scope per attempt.
    // Those wrappers keep no state between sends, so one composition serves every concurrent run;
    // everything an attempt owns is created inside AttemptFilter.
    private readonly IPipe<TContext> _pipe = ComposeRun(definition, scopeFactory);

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
            await _pipe.SendAsync(context).ConfigureAwait(false);
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

    private static IPipe<TContext> ComposeRun(
        PipelineDefinition<TContext> definition,
        IServiceScopeFactory scopeFactory
    )
    {
        var builder = new PipeBuilder<TContext>();
        foreach (var configure in definition.OuterFilters)
        {
            configure(builder);
        }

        return builder.Use(new AttemptFilter(definition, scopeFactory)).Build();
    }

    /// <summary>
    /// Terminal of the run-level wrappers: each send is one attempt, run in its own scope with the
    /// stage filters resolved from it. Nothing follows it, so the downstream pipe is not invoked.
    /// </summary>
    private sealed class AttemptFilter(
        PipelineDefinition<TContext> definition,
        IServiceScopeFactory scopeFactory
    ) : IFilter<TContext>
    {
        public async ValueTask SendAsync(TContext context, IPipe<TContext> next)
        {
            // The scope opens beneath the retry and timeout wrappers, so a retry gets a fresh scope
            // and fresh filter instances. Reusing the failed attempt's scope would hand the retry
            // the scoped state that attempt left behind, such as a unit of work holding its changes.
            var scope = scopeFactory.CreateAsyncScope();
            await using (scope.ConfigureAwait(false))
            {
                var pipe = BuildStages(scope.ServiceProvider);
                await pipe.SendAsync(context).ConfigureAwait(false);
            }
        }

        // Rebuilt per attempt: filters are transient with scoped dependencies, and EnabledWhen is a
        // per-attempt decision. The build itself is a list walk and a fold, cheap next to any filter.
        private IPipe<TContext> BuildStages(IServiceProvider provider)
        {
            var builder = new PipeBuilder<TContext>();
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
}
