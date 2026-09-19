using HostLoom.Scheduling;
using Xunit;

namespace HostLoom.Tests;

public sealed class CronExpressionTests
{
    // Thursday, 1 January 1970, 00:00:00 UTC.
    private static readonly DateTimeOffset Epoch = DateTimeOffset.UnixEpoch;

    [Theory]
    [InlineData("*/15 * * * *", "1970-01-01T00:15:00Z")]
    [InlineData("0 0 * * * *", "1970-01-01T01:00:00Z")]
    [InlineData("30 0 2 * * MON-FRI", "1970-01-01T02:00:30Z")]
    [InlineData("0 0 12 1 JAN,JUL *", "1970-01-01T12:00:00Z")]
    [InlineData("5/10 * * * *", "1970-01-01T00:05:00Z")]
    [InlineData("0 0-6/2 * * *", "1970-01-01T02:00:00Z")]
    [InlineData("0 0 0 ? * MON", "1970-01-05T00:00:00Z")]
    [InlineData("0 0 0 * * 7", "1970-01-04T00:00:00Z")]
    [InlineData("0 0 0 * * FRI-SUN", "1970-01-02T00:00:00Z")]
    [InlineData("0 0 0 1 * *", "1970-02-01T00:00:00Z")]
    [InlineData("0 0 0 29 2 *", "1972-02-29T00:00:00Z")]
    public void Next_occurrence_follows_the_expression(string expression, string expected)
    {
        var cron = CronExpression.Parse(expression);

        var next = cron.GetNextOccurrence(Epoch);

        Assert.Equal(DateTimeOffset.Parse(expected, null), next);
    }

    [Fact]
    public void Both_day_fields_restricted_match_either_as_in_vixie_cron()
    {
        // The 15th is a Thursday; the first Sunday, the 4th, comes first.
        var cron = CronExpression.Parse("0 0 0 15 * SUN");

        Assert.Equal(
            DateTimeOffset.Parse("1970-01-04T00:00:00Z", null),
            cron.GetNextOccurrence(Epoch)
        );
        Assert.Equal(
            DateTimeOffset.Parse("1970-01-15T00:00:00Z", null),
            cron.GetNextOccurrence(DateTimeOffset.Parse("1970-01-11T00:00:00Z", null))
        );
    }

    [Fact]
    public void An_occurrence_at_the_exact_instant_is_excluded()
    {
        var cron = CronExpression.Parse("0 0 * * * *");

        Assert.Equal(Epoch.AddHours(1), cron.GetNextOccurrence(Epoch));
        Assert.Equal(Epoch.AddHours(1), cron.GetNextOccurrence(Epoch.AddMilliseconds(1)));
        Assert.Equal(Epoch.AddHours(1), cron.GetNextOccurrence(Epoch.AddSeconds(59)));
    }

    [Fact]
    public void An_expression_with_no_occurrence_returns_null()
    {
        Assert.Null(CronExpression.Parse("0 0 0 30 2 *").GetNextOccurrence(Epoch));
    }

    [Fact]
    public void Occurrences_are_evaluated_in_the_given_zone()
    {
        var plusTwo = TimeZoneInfo.CreateCustomTimeZone(
            "plus-two",
            TimeSpan.FromHours(2),
            "+2",
            "+2"
        );
        var cron = CronExpression.Parse("0 0 9 * * *");

        var next = cron.GetNextOccurrence(Epoch, plusTwo);

        Assert.NotNull(next);
        Assert.Equal(TimeSpan.FromHours(2), next.Value.Offset);
        Assert.Equal(9, next.Value.Hour);
        Assert.Equal(Epoch.AddHours(7), next.Value);
    }

    [Fact]
    public void A_local_time_a_daylight_transition_skips_is_not_an_occurrence()
    {
        TimeZoneInfo zone;
        try
        {
            zone = TimeZoneInfo.FindSystemTimeZoneById("America/New_York");
        }
        catch (TimeZoneNotFoundException)
        {
            Assert.Skip("The America/New_York zone is not installed.");
            return;
        }

        // Clocks jump from 02:00 to 03:00 on 8 March 2026; 02:30 does not exist that day.
        var cron = CronExpression.Parse("0 30 2 * * *");
        var after = new DateTimeOffset(2026, 3, 8, 0, 0, 0, TimeSpan.FromHours(-5));

        var next = cron.GetNextOccurrence(after, zone);

        Assert.Equal(new DateTimeOffset(2026, 3, 9, 2, 30, 0, TimeSpan.FromHours(-4)), next);
    }

    [Theory]
    [InlineData("* * * *", "4 fields")]
    [InlineData("* * * * * * *", "7 fields")]
    [InlineData("60 * * * *", "minute")]
    [InlineData("0 24 * * *", "hour")]
    [InlineData("0 0 0 * *", "day-of-month")]
    [InlineData("0 0 * 13 *", "month")]
    [InlineData("0 0 * * 8", "day-of-week")]
    [InlineData("0 0 L * *", "day-of-month")]
    [InlineData("0 0 * * MONDAY", "day-of-week")]
    [InlineData("5-1 * * * *", "precedes its start")]
    [InlineData("*/0 * * * *", "step")]
    [InlineData("1,,2 * * * *", "empty list item")]
    public void Malformed_expressions_are_rejected_naming_the_field(
        string expression,
        string detail
    )
    {
        var failure = Assert.Throws<FormatException>(() => CronExpression.Parse(expression));

        Assert.Contains(detail, failure.Message, StringComparison.Ordinal);
        Assert.False(CronExpression.TryParse(expression, out var parsed));
        Assert.Null(parsed);
    }

    [Fact]
    public void Parse_normalises_whitespace_and_TryParse_reports_success()
    {
        Assert.True(CronExpression.TryParse("  0   0\t*  * *   * ", out var cron));

        Assert.Equal("0 0 * * * *", cron!.Expression);
        Assert.Equal("0 0 * * * *", cron.ToString());
        Assert.False(CronExpression.TryParse(" ", out _));
        Assert.Throws<ArgumentException>(() => CronExpression.Parse(" "));
    }
}
