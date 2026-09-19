namespace HostLoom.Scheduling;

/// <summary>
/// Decides when a schedule runs next. Triggers are immutable and safe to share. The three shapes
/// follow the Spring conventions: a cron expression, a fixed rate counted from the previous due
/// time, and a fixed delay counted from the previous completion.
/// </summary>
public abstract class ScheduleTrigger
{
    /// <summary>Short trigger description, surfaced by the probe and the startup log.</summary>
    public abstract string Description { get; }

    /// <summary>
    /// The next due time strictly after <paramref name="now"/>, or <see langword="null"/> when
    /// the trigger has no further occurrence.
    /// </summary>
    /// <param name="now">The current time.</param>
    /// <param name="history">What the previous run of this schedule looked like, if any.</param>
    public abstract DateTimeOffset? GetNextDue(DateTimeOffset now, ScheduleHistory history);

    /// <summary>
    /// A cron trigger. The expression has five fields (minute, hour, day of month, month, day of
    /// week) or six with a leading seconds field, as in Spring. Occurrences are evaluated in
    /// <paramref name="timeZone"/>, UTC by default.
    /// </summary>
    /// <exception cref="FormatException">The expression is not a supported cron expression.</exception>
    public static ScheduleTrigger Cron(string expression, TimeZoneInfo? timeZone = null) =>
        new CronTrigger(CronExpression.Parse(expression), timeZone);

    /// <summary>
    /// Runs every <paramref name="period"/> counted from the previous due time, so a slow run
    /// does not drift the schedule. A run that is already late starts at once; the periods it
    /// missed are not replayed. The first run is due after <paramref name="initialDelay"/>,
    /// which defaults to zero.
    /// </summary>
    public static ScheduleTrigger FixedRate(TimeSpan period, TimeSpan? initialDelay = null) =>
        new FixedRateTrigger(period, initialDelay);

    /// <summary>
    /// Runs <paramref name="delay"/> after the previous run completed, so two runs are always
    /// separated by at least the delay. The first run is due after <paramref name="initialDelay"/>,
    /// which defaults to <paramref name="delay"/>.
    /// </summary>
    public static ScheduleTrigger FixedDelay(TimeSpan delay, TimeSpan? initialDelay = null) =>
        new FixedDelayTrigger(delay, initialDelay);
}

/// <summary>The previous run of a schedule, as a trigger sees it.</summary>
/// <param name="LastDue">When the previous run was due, or <see langword="null"/> before the first run.</param>
/// <param name="LastStartedAt">When the previous run started, or <see langword="null"/>.</param>
/// <param name="LastCompletedAt">When the previous run ended, whatever its outcome, or <see langword="null"/>.</param>
public readonly record struct ScheduleHistory(
    DateTimeOffset? LastDue,
    DateTimeOffset? LastStartedAt,
    DateTimeOffset? LastCompletedAt
);
