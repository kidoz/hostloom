namespace HostLoom.Scheduling;

/// <summary>How a scheduled run ended, as reported by state, metrics, and logs.</summary>
public enum ScheduleRunOutcome
{
    /// <summary>The job returned.</summary>
    Succeeded,

    /// <summary>The job threw; the schedule continues.</summary>
    Failed,

    /// <summary>The job observed the cancellation raised by <see cref="ScheduleOptions.Timeout"/>.</summary>
    TimedOut,

    /// <summary>The job observed the cancellation raised by stopping the scheduler.</summary>
    Canceled,

    /// <summary>The exclusive claim was lost mid-run and the job observed the cancellation.</summary>
    ClaimLost,

    /// <summary>Another instance held the exclusive claim; the job did not run.</summary>
    Skipped,

    /// <summary>The guard threw while claiming; the job did not run.</summary>
    GuardFailed,
}
