namespace HostLoom.Scheduling;

/// <summary>What a job knows about the run it is executing.</summary>
/// <param name="Schedule">The schedule name.</param>
/// <param name="DueAt">When the trigger wanted the run to start.</param>
/// <param name="StartedAt">When the run actually started, after any claim.</param>
/// <param name="Sequence">The run's ordinal within this process, counting from one.</param>
public sealed record ScheduledRun(
    string Schedule,
    DateTimeOffset DueAt,
    DateTimeOffset StartedAt,
    long Sequence
)
{
    /// <summary>How late the run started; zero or positive.</summary>
    public TimeSpan Lag => StartedAt - DueAt;
}
