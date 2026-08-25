namespace HostLoom.Pipelines.DependencyInjection;

/// <summary>
/// Executes one registered pipeline. Each run gets its own dependency-injection scope, resolves
/// the enabled filters transiently in stage order, composes them, and sends the context through.
/// Exceptions propagate unchanged, so a caller with at-least-once semantics sees every failure.
/// </summary>
/// <remarks>
/// Resolve by key (the pipeline name) when a process hosts several pipelines for one context
/// type; the unkeyed registration resolves only while exactly one pipeline exists for the type.
/// </remarks>
public interface IPipelineRunner<TContext>
    where TContext : class, IPipeContext
{
    string PipelineName { get; }

    /// <summary>The declared stage and filter order, available without executing anything.</summary>
    PipelineTopology Topology { get; }

    ValueTask RunAsync(TContext context);
}
