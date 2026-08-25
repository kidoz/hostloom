namespace HostLoom.Pipelines.DependencyInjection;

internal sealed record PipelineFilterDefinition(
    Type FilterType,
    string Name,
    Func<IServiceProvider, bool>? EnabledWhen
);

internal sealed record PipelineStageDefinition(
    string Name,
    IReadOnlyList<PipelineFilterDefinition> Filters
);

/// <summary>Registration-time view of a pipeline, shared by the registry, validator, and harnesses.</summary>
internal interface IPipelineDefinition
{
    string Name { get; }
    Type ContextType { get; }
    PipelineTopology Topology { get; }
    IEnumerable<Type> FilterTypes { get; }
}

internal sealed class PipelineDefinition<TContext> : IPipelineDefinition
    where TContext : class, IPipeContext
{
    public PipelineDefinition(
        string name,
        IReadOnlyList<PipelineStageDefinition> stages,
        IReadOnlyList<Action<PipeBuilder<TContext>>> outerFilters,
        bool instrumented
    )
    {
        Name = name;
        Stages = stages;
        OuterFilters = outerFilters;
        Instrumented = instrumented;
        Topology = new PipelineTopology(
            name,
            stages
                .Select(stage => new PipelineStageTopology(
                    stage.Name,
                    stage
                        .Filters.Select(filter => new PipelineFilterTopology(
                            filter.Name,
                            filter.FilterType,
                            filter.EnabledWhen is not null
                        ))
                        .ToArray()
                ))
                .ToArray()
        );
    }

    public string Name { get; }

    public IReadOnlyList<PipelineStageDefinition> Stages { get; }

    /// <summary>Wrappers around the whole pipeline (timeout, retry), applied in declaration order, first outermost.</summary>
    public IReadOnlyList<Action<PipeBuilder<TContext>>> OuterFilters { get; }

    public bool Instrumented { get; }

    public Type ContextType => typeof(TContext);

    public PipelineTopology Topology { get; }

    public IEnumerable<Type> FilterTypes =>
        Stages.SelectMany(stage => stage.Filters).Select(filter => filter.FilterType).Distinct();
}
