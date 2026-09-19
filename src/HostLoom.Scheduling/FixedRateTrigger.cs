using System.Globalization;

namespace HostLoom.Scheduling;

/// <summary>Runs at a fixed period counted from the previous due time. See <see cref="ScheduleTrigger.FixedRate"/>.</summary>
public sealed class FixedRateTrigger : ScheduleTrigger
{
    /// <summary>Creates the trigger.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The period is not positive or the initial delay is negative.</exception>
    public FixedRateTrigger(TimeSpan period, TimeSpan? initialDelay = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(period, TimeSpan.Zero);
        var initial = initialDelay ?? TimeSpan.Zero;
        ArgumentOutOfRangeException.ThrowIfLessThan(initial, TimeSpan.Zero, nameof(initialDelay));
        Period = period;
        InitialDelay = initial;
        Description = string.Create(
            CultureInfo.InvariantCulture,
            $"fixed rate {period}, initial delay {initial}"
        );
    }

    /// <summary>Time between consecutive due times.</summary>
    public TimeSpan Period { get; }

    /// <summary>Delay before the first due time.</summary>
    public TimeSpan InitialDelay { get; }

    /// <inheritdoc />
    public override string Description { get; }

    /// <inheritdoc />
    public override DateTimeOffset? GetNextDue(DateTimeOffset now, ScheduleHistory history)
    {
        if (history.LastDue is not { } lastDue)
        {
            return now + InitialDelay;
        }

        // Counted from the due time, not the start, so a slow run does not drift the schedule; a
        // period that has already passed is not replayed, the run is simply due now.
        var next = lastDue + Period;
        return next > now ? next : now;
    }
}
