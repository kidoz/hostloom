namespace HostLoom.Scheduling;

/// <summary>A point-in-time view of one schedule, from <see cref="Scheduler.GetState"/>.</summary>
/// <param name="Name">The schedule name.</param>
/// <param name="NextDue">When the next run is due, or <see langword="null"/> before start, while running, or after the trigger ended.</param>
/// <param name="Running">Whether a run is executing right now.</param>
/// <param name="Runs">Runs started in this process, including skipped ones.</param>
/// <param name="LastOutcome">How the most recent run ended, or <see langword="null"/> before the first.</param>
/// <param name="LastStartedAt">When the most recent run started.</param>
/// <param name="LastCompletedAt">When the most recent run ended.</param>
/// <param name="ClaimHeldUntil">
/// For an exclusive schedule, when the claim kept after the last run is released, or
/// <see langword="null"/> when none is held. Other instances are refused until then.
/// </param>
public sealed record ScheduleState(
    string Name,
    DateTimeOffset? NextDue,
    bool Running,
    long Runs,
    ScheduleRunOutcome? LastOutcome,
    DateTimeOffset? LastStartedAt,
    DateTimeOffset? LastCompletedAt,
    DateTimeOffset? ClaimHeldUntil
);
