namespace HostLoom.Scheduling;

/// <summary>A <see cref="ScheduleTrigger"/> over a <see cref="CronExpression"/> in one time zone.</summary>
public sealed class CronTrigger : ScheduleTrigger
{
    /// <summary>Creates the trigger; <paramref name="timeZone"/> defaults to UTC.</summary>
    public CronTrigger(CronExpression expression, TimeZoneInfo? timeZone = null)
    {
        ArgumentNullException.ThrowIfNull(expression);
        Expression = expression;
        TimeZone = timeZone ?? TimeZoneInfo.Utc;
        Description = $"cron '{expression.Expression}' ({TimeZone.Id})";
    }

    /// <summary>The parsed expression.</summary>
    public CronExpression Expression { get; }

    /// <summary>The zone the expression is evaluated in.</summary>
    public TimeZoneInfo TimeZone { get; }

    /// <inheritdoc />
    public override string Description { get; }

    /// <inheritdoc />
    public override DateTimeOffset? GetNextDue(DateTimeOffset now, ScheduleHistory history) =>
        Expression.GetNextOccurrence(now, TimeZone);
}
