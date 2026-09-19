namespace HostLoom.Scheduling;

/// <summary>
/// One unit of scheduled work. The scheduler runs a schedule's job sequentially: a run never
/// starts while the previous run of the same schedule is still executing in this process. Across
/// processes, exclusivity is the <see cref="IScheduleGuard"/>'s concern, and only for schedules
/// marked <see cref="ScheduleOptions.Exclusive"/>.
/// </summary>
public interface IScheduledJob
{
    /// <summary>
    /// Runs the job. The token is cancelled when the host stops, when
    /// <see cref="ScheduleOptions.Timeout"/> elapses, or when an exclusive claim is lost. An
    /// exception ends the run as <see cref="ScheduleRunOutcome.Failed"/>; the schedule continues.
    /// </summary>
    ValueTask ExecuteAsync(ScheduledRun run, CancellationToken cancellationToken);
}
