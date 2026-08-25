namespace HostLoom.Pipelines;

/// <summary>Thrown when a pipeline exceeds its timeout, as opposed to being cancelled by its caller.</summary>
public sealed class PipelineTimeoutException : TimeoutException
{
    /// <param name="innerException">
    /// The cancellation the deadline produced, or <see langword="null"/> when a filter overran the
    /// deadline without observing the token and so never threw.
    /// </param>
    public PipelineTimeoutException(TimeSpan timeout, Exception? innerException = null)
        : base($"The pipeline did not complete within {timeout}.", innerException) =>
        Timeout = timeout;

    public TimeSpan Timeout { get; }
}
