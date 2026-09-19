using Microsoft.Extensions.Logging;

namespace HostLoom.Scheduling;

/// <summary>
/// Stable event ids for every log line the scheduling kernel writes, so a logging pipeline can
/// filter or alert on one condition without matching message text.
/// </summary>
public static class SchedulingEvents
{
    /// <summary>Warning at construction: <c>Scheduling:Enabled</c> is false, nothing runs.</summary>
    public static readonly EventId Disabled = new(3200, "SchedulingDisabled");

    /// <summary>Information when a schedule's loop starts: its trigger and first due time.</summary>
    public static readonly EventId ScheduleStarted = new(3201, "ScheduleStarted");

    /// <summary>Warning: the job threw; the schedule continues.</summary>
    public static readonly EventId RunFailed = new(3202, "ScheduleRunFailed");

    /// <summary>Warning: the run exceeded its timeout and was cancelled.</summary>
    public static readonly EventId RunTimedOut = new(3203, "ScheduleRunTimedOut");

    /// <summary>Debug: another instance held the exclusive claim, the run was skipped.</summary>
    public static readonly EventId RunSkipped = new(3204, "ScheduleRunSkipped");

    /// <summary>Warning: the guard threw while claiming; the run was skipped.</summary>
    public static readonly EventId GuardFailed = new(3205, "ScheduleGuardFailed");

    /// <summary>Warning: the exclusive claim was lost mid-run; the run was cancelled.</summary>
    public static readonly EventId ClaimLost = new(3206, "ScheduleClaimLost");

    /// <summary>Information: the trigger has no further occurrence; the schedule's loop ended.</summary>
    public static readonly EventId ScheduleEnded = new(3207, "ScheduleEnded");

    /// <summary>Error: the schedule's loop itself faulted, which is a defect; the schedule stopped.</summary>
    public static readonly EventId LoopFaulted = new(3208, "ScheduleLoopFaulted");
}
