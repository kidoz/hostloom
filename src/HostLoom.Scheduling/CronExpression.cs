using System.Globalization;

namespace HostLoom.Scheduling;

/// <summary>
/// A parsed cron expression. Five fields (minute, hour, day of month, month, day of week) or six
/// with a leading seconds field, as Spring reads them. Each field accepts <c>*</c>, values,
/// ranges (<c>a-b</c>), lists (<c>a,b</c>), and steps (<c>*/n</c>, <c>a/n</c>, <c>a-b/n</c>);
/// months and days of week also accept their three-letter English names; day of month and day
/// of week accept <c>?</c> as a synonym for <c>*</c>. Day of week counts Sunday as 0 or 7. When
/// both day fields are restricted an occurrence matches either, as in Vixie cron. The
/// <c>L</c>, <c>W</c>, and <c>#</c> extensions are not supported. Instances are immutable.
/// </summary>
public sealed class CronExpression
{
    private const int LookaheadYears = 5;

    private static readonly string[] MonthNames =
    [
        "JAN",
        "FEB",
        "MAR",
        "APR",
        "MAY",
        "JUN",
        "JUL",
        "AUG",
        "SEP",
        "OCT",
        "NOV",
        "DEC",
    ];

    private static readonly string[] DayNames = ["SUN", "MON", "TUE", "WED", "THU", "FRI", "SAT"];

    private readonly ulong _seconds;
    private readonly ulong _minutes;
    private readonly ulong _hours;
    private readonly ulong _daysOfMonth;
    private readonly ulong _months;
    private readonly ulong _daysOfWeek;
    private readonly bool _restrictsDayOfMonth;
    private readonly bool _restrictsDayOfWeek;

    private CronExpression(
        string expression,
        ulong seconds,
        ulong minutes,
        ulong hours,
        ulong daysOfMonth,
        ulong months,
        ulong daysOfWeek,
        bool restrictsDayOfMonth,
        bool restrictsDayOfWeek
    )
    {
        Expression = expression;
        _seconds = seconds;
        _minutes = minutes;
        _hours = hours;
        _daysOfMonth = daysOfMonth;
        _months = months;
        _daysOfWeek = daysOfWeek;
        _restrictsDayOfMonth = restrictsDayOfMonth;
        _restrictsDayOfWeek = restrictsDayOfWeek;
    }

    /// <summary>The expression as given, with its fields separated by single spaces.</summary>
    public string Expression { get; }

    /// <summary>Parses <paramref name="expression"/>.</summary>
    /// <exception cref="FormatException">The expression is not a supported cron expression; the message names the field at fault.</exception>
    public static CronExpression Parse(string expression)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(expression);
        var fields = expression.Split(
            [' ', '\t'],
            StringSplitOptions.RemoveEmptyEntries | StringSplitOptions.TrimEntries
        );
        if (fields.Length is not (5 or 6))
        {
            throw new FormatException(
                $"Cron expression '{expression}' has {fields.Length} fields; five (minute hour day-of-month month day-of-week) or six with leading seconds are supported."
            );
        }

        var offset = fields.Length == 6 ? 1 : 0;
        var seconds =
            offset == 1 ? ParseField(fields[0], "seconds", 0, 59, null, false, out _) : 1UL;
        var minutes = ParseField(fields[offset], "minute", 0, 59, null, false, out _);
        var hours = ParseField(fields[offset + 1], "hour", 0, 23, null, false, out _);
        var daysOfMonth = ParseField(
            fields[offset + 2],
            "day-of-month",
            1,
            31,
            null,
            true,
            out var restrictsDayOfMonth
        );
        var months = ParseField(fields[offset + 3], "month", 1, 12, MonthNames, false, out _);
        var daysOfWeek = ParseField(
            fields[offset + 4],
            "day-of-week",
            0,
            7,
            DayNames,
            true,
            out var restrictsDayOfWeek
        );

        // Sunday is 0 or 7; fold 7 into 0 so ranges such as FRI-SUN (5-7) work.
        if ((daysOfWeek & (1UL << 7)) != 0)
        {
            daysOfWeek = (daysOfWeek | 1UL) & ~(1UL << 7);
        }

        return new CronExpression(
            string.Join(' ', fields),
            seconds,
            minutes,
            hours,
            daysOfMonth,
            months,
            daysOfWeek,
            restrictsDayOfMonth,
            restrictsDayOfWeek
        );
    }

    /// <summary>Parses <paramref name="expression"/>, reporting failure instead of throwing.</summary>
    public static bool TryParse(string? expression, out CronExpression? result)
    {
        result = null;
        if (string.IsNullOrWhiteSpace(expression))
        {
            return false;
        }

        try
        {
            result = Parse(expression);
            return true;
        }
        catch (FormatException)
        {
            return false;
        }
    }

    /// <summary>
    /// The first occurrence strictly after <paramref name="after"/>, evaluated in
    /// <paramref name="timeZone"/> (UTC by default), or <see langword="null"/> when none exists
    /// within the next five years. Local times a daylight-saving transition skips are not
    /// occurrences; an ambiguous local time occurs once, at its earlier instant.
    /// </summary>
    public DateTimeOffset? GetNextOccurrence(DateTimeOffset after, TimeZoneInfo? timeZone = null)
    {
        var zone = timeZone ?? TimeZoneInfo.Utc;
        var local = TimeZoneInfo.ConvertTime(after, zone).DateTime;
        local = DateTime
            .SpecifyKind(
                new DateTime(local.Ticks - (local.Ticks % TimeSpan.TicksPerSecond)),
                DateTimeKind.Unspecified
            )
            .AddSeconds(1);
        var limit = local.AddYears(LookaheadYears);

        while (local < limit)
        {
            if (!Has(_months, local.Month))
            {
                local = new DateTime(local.Year, local.Month, 1, 0, 0, 0).AddMonths(1);
                continue;
            }

            if (!DayMatches(local))
            {
                local = local.Date.AddDays(1);
                continue;
            }

            if (!Has(_hours, local.Hour))
            {
                local = new DateTime(local.Year, local.Month, local.Day, local.Hour, 0, 0).AddHours(
                    1
                );
                continue;
            }

            if (!Has(_minutes, local.Minute))
            {
                local = new DateTime(
                    local.Year,
                    local.Month,
                    local.Day,
                    local.Hour,
                    local.Minute,
                    0
                ).AddMinutes(1);
                continue;
            }

            if (!Has(_seconds, local.Second))
            {
                local = local.AddSeconds(1);
                continue;
            }

            if (zone.IsInvalidTime(local))
            {
                // Inside a spring-forward gap: no instant carries this local time.
                local = new DateTime(
                    local.Year,
                    local.Month,
                    local.Day,
                    local.Hour,
                    local.Minute,
                    0
                ).AddMinutes(1);
                continue;
            }

            var offset = zone.IsAmbiguousTime(local)
                ? zone.GetAmbiguousTimeOffsets(local).Max()
                : zone.GetUtcOffset(local);
            return new DateTimeOffset(local, offset);
        }

        return null;
    }

    /// <inheritdoc />
    public override string ToString() => Expression;

    private static bool Has(ulong mask, int value) => (mask & (1UL << value)) != 0;

    private bool DayMatches(DateTime local)
    {
        var dayOfMonth = Has(_daysOfMonth, local.Day);
        var dayOfWeek = Has(_daysOfWeek, (int)local.DayOfWeek);
        if (_restrictsDayOfMonth && _restrictsDayOfWeek)
        {
            return dayOfMonth || dayOfWeek;
        }

        return _restrictsDayOfMonth ? dayOfMonth
            : _restrictsDayOfWeek ? dayOfWeek
            : true;
    }

    private static ulong ParseField(
        string text,
        string field,
        int minimum,
        int maximum,
        string[]? names,
        bool allowsQuestionMark,
        out bool restricted
    )
    {
        if (text == "*" || (allowsQuestionMark && text == "?"))
        {
            restricted = false;
            return Range(minimum, maximum);
        }

        restricted = true;
        var mask = 0UL;
        foreach (var item in text.Split(','))
        {
            if (item.Length == 0)
            {
                throw Invalid(field, text, "an empty list item");
            }

            var step = 1;
            var range = item;
            var slash = item.IndexOf('/', StringComparison.Ordinal);
            if (slash >= 0)
            {
                range = item[..slash];
                var stepText = item[(slash + 1)..];
                if (
                    !int.TryParse(
                        stepText,
                        NumberStyles.None,
                        CultureInfo.InvariantCulture,
                        out step
                    )
                    || step < 1
                )
                {
                    throw Invalid(field, text, $"the step '{stepText}'");
                }
            }

            int low;
            int high;
            if (range == "*")
            {
                low = minimum;
                high = maximum;
            }
            else
            {
                var dash = range.IndexOf('-', StringComparison.Ordinal);
                if (dash >= 0)
                {
                    low = ParseValue(range[..dash], field, text, minimum, maximum, names);
                    high = ParseValue(range[(dash + 1)..], field, text, minimum, maximum, names);
                    if (high == 0 && low > 0 && maximum == 7)
                    {
                        // A day-of-week range ending in Sunday, such as FRI-SUN: Sunday is 7 there.
                        high = 7;
                    }

                    if (high < low)
                    {
                        throw Invalid(
                            field,
                            text,
                            $"the range '{range}' (its end precedes its start)"
                        );
                    }
                }
                else
                {
                    low = ParseValue(range, field, text, minimum, maximum, names);
                    high = slash >= 0 ? maximum : low;
                }
            }

            for (var value = low; value <= high; value += step)
            {
                mask |= 1UL << value;
            }
        }

        return mask;
    }

    private static int ParseValue(
        string text,
        string field,
        string fieldText,
        int minimum,
        int maximum,
        string[]? names
    )
    {
        if (
            int.TryParse(text, NumberStyles.None, CultureInfo.InvariantCulture, out var number)
            && number >= minimum
            && number <= maximum
        )
        {
            return number;
        }

        if (names is not null)
        {
            for (var index = 0; index < names.Length; index++)
            {
                if (string.Equals(names[index], text, StringComparison.OrdinalIgnoreCase))
                {
                    return index + minimum;
                }
            }
        }

        throw Invalid(
            field,
            fieldText,
            $"the value '{text}' (expected {minimum} to {maximum}{(names is null ? "" : " or a name")})"
        );
    }

    private static ulong Range(int minimum, int maximum)
    {
        var mask = 0UL;
        for (var value = minimum; value <= maximum; value++)
        {
            mask |= 1UL << value;
        }

        return mask;
    }

    private static FormatException Invalid(string field, string text, string detail) =>
        new($"The cron {field} field '{text}' is not valid: {detail}.");
}
