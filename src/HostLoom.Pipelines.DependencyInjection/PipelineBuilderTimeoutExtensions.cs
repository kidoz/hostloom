namespace HostLoom.Pipelines.DependencyInjection;

/// <summary>
/// Timeout support for registered pipelines whose context derives from <see cref="PipeContext"/>,
/// which is what allows the deadline to reach downstream filters through the context's token.
/// </summary>
public static class PipelineBuilderTimeoutExtensions
{
    /// <summary>
    /// Bounds the whole run: when it exceeds <paramref name="timeout"/> the run fails with
    /// <see cref="PipelineTimeoutException"/> and the context's token is cancelled so in-flight
    /// filter work stops. Nested with <c>WithRetry</c> in declaration order, first outermost, so
    /// declaring the timeout first makes it a budget across all retry attempts.
    /// </summary>
    public static PipelineBuilder<TContext> WithTimeout<TContext>(
        this PipelineBuilder<TContext> builder,
        TimeSpan timeout,
        TimeProvider? timeProvider = null
    )
        where TContext : PipeContext
    {
        ArgumentNullException.ThrowIfNull(builder);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeout, TimeSpan.Zero);
        builder.AddOuterFilter(pipe => pipe.UseTimeout(timeout, timeProvider));
        return builder;
    }
}
