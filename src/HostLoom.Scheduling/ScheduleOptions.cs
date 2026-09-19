namespace HostLoom.Scheduling;

/// <summary>Per-schedule settings. A <see langword="null"/> member falls back to <see cref="SchedulingOptions"/>.</summary>
public sealed class ScheduleOptions
{
    /// <summary>
    /// Run on at most one instance at a time. Each run first claims the schedule through the
    /// <see cref="IScheduleGuard"/>; a run whose claim is held elsewhere is skipped, never queued.
    /// Requires a guard; the in-process sequential rule applies whether or not this is set.
    /// </summary>
    public bool Exclusive { get; set; }

    /// <summary>
    /// How long other instances are refused after this one claims an occurrence. The claim is kept
    /// past the run until the lease ends or this instance's next occurrence is due, whichever
    /// comes first, so a slower instance reaching the same occurrence is refused rather than
    /// running it again. Choose at least the clock skew between instances plus the run's
    /// duration, and no more than the schedule's period. Default: <see cref="SchedulingOptions.DefaultLease"/>.
    /// </summary>
    public TimeSpan? Lease { get; set; }

    /// <summary>
    /// Cancels the run's token after this long. The run ends as
    /// <see cref="ScheduleRunOutcome.TimedOut"/> only when the job honours the token; a job that
    /// ignores it keeps running and the next run waits for it. Default: no timeout.
    /// </summary>
    public TimeSpan? Timeout { get; set; }

    /// <summary>Every violation, each naming the schedule and the setting at fault.</summary>
    public IReadOnlyList<string> Validate(string schedule)
    {
        List<string> problems = [];
        if (Lease is { } lease && lease <= TimeSpan.Zero)
        {
            problems.Add($"Schedule '{schedule}': Lease must be positive.");
        }

        if (Timeout is { } timeout && timeout <= TimeSpan.Zero)
        {
            problems.Add($"Schedule '{schedule}': Timeout must be positive.");
        }

        return problems;
    }
}
