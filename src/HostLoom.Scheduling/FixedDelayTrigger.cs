using System.Globalization;

namespace HostLoom.Scheduling;

/// <summary>Runs a fixed delay after the previous completion. See <see cref="ScheduleTrigger.FixedDelay"/>.</summary>
public sealed class FixedDelayTrigger : ScheduleTrigger
{
    /// <summary>Creates the trigger.</summary>
    /// <exception cref="ArgumentOutOfRangeException">The delay is not positive or the initial delay is negative.</exception>
    public FixedDelayTrigger(TimeSpan delay, TimeSpan? initialDelay = null)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(delay, TimeSpan.Zero);
        var initial = initialDelay ?? delay;
        ArgumentOutOfRangeException.ThrowIfLessThan(initial, TimeSpan.Zero, nameof(initialDelay));
        Delay = delay;
        InitialDelay = initial;
        Description = string.Create(
            CultureInfo.InvariantCulture,
            $"fixed delay {delay}, initial delay {initial}"
        );
    }

    /// <summary>Time between a completion and the next due time.</summary>
    public TimeSpan Delay { get; }

    /// <summary>Delay before the first due time.</summary>
    public TimeSpan InitialDelay { get; }

    /// <inheritdoc />
    public override string Description { get; }

    /// <inheritdoc />
    public override DateTimeOffset? GetNextDue(DateTimeOffset now, ScheduleHistory history) =>
        history.LastCompletedAt is { } completed ? completed + Delay : now + InitialDelay;
}
