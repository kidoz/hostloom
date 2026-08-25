namespace HostLoom.Pipelines;

/// <summary>
/// Timeout support for pipelines whose context derives from <see cref="PipeContext"/>. The
/// constraint is what makes the timeout honest: the filter must substitute a linked token into
/// the context for the downstream call, and only <see cref="PipeContext"/> supports that swap.
/// </summary>
public static class PipeBuilderTimeoutExtensions
{
    /// <summary>
    /// Fails the remainder of the pipeline with <see cref="PipelineTimeoutException"/> when it runs
    /// longer than <paramref name="timeout"/>. Downstream filters observe the deadline through
    /// <see cref="IPipeContext.CancellationToken"/>; caller cancellation is always rethrown as
    /// cancellation, never converted into a timeout.
    /// </summary>
    public static PipeBuilder<TContext> UseTimeout<TContext>(
        this PipeBuilder<TContext> builder,
        TimeSpan timeout,
        TimeProvider? timeProvider = null
    )
        where TContext : PipeContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.Use(
            new TimeoutFilter<TContext>(timeout, timeProvider ?? TimeProvider.System)
        );
    }
}
