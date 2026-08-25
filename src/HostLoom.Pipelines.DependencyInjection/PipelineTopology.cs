namespace HostLoom.Pipelines.DependencyInjection;

/// <summary>The resolved shape of one registered pipeline: its stages and their filters in execution order.</summary>
public sealed record PipelineTopology(
    string PipelineName,
    IReadOnlyList<PipelineStageTopology> Stages
)
{
    /// <summary>One line for logs and diagnostics; a trailing '?' marks a conditionally enabled filter.</summary>
    public string Describe() =>
        string.Join(
            " -> ",
            Stages.Select(stage =>
                $"{stage.Name}[{string.Join(", ", stage.Filters.Select(filter => filter.IsConditional ? filter.Name + "?" : filter.Name))}]"
            )
        );
}

public sealed record PipelineStageTopology(
    string Name,
    IReadOnlyList<PipelineFilterTopology> Filters
);

public sealed record PipelineFilterTopology(string Name, Type FilterType, bool IsConditional);
