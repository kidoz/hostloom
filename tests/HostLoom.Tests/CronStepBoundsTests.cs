using HostLoom.Scheduling;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A cron step is bounded by the width of its field. Before the bound, an oversized step
/// wrapped the integer walk over the field and, through a negative shift, selected values the
/// expression never named: <c>1/2147483647</c> fired at both :00 and :01.
/// </summary>
public sealed class CronStepBoundsTests
{
    // Thursday, 1 January 1970, 00:00:00 UTC.
    private static readonly DateTimeOffset Epoch = DateTimeOffset.UnixEpoch;

    [Theory]
    [InlineData("1/2147483647 * * * *", "minute", "1 to 59")]
    [InlineData("*/1000 * * * *", "minute", "1 to 59")]
    [InlineData("*/60 * * * *", "minute", "1 to 59")]
    [InlineData("0 */24 * * *", "hour", "1 to 23")]
    [InlineData("0 0 */31 * *", "day-of-month", "1 to 30")]
    [InlineData("0 0 * */12 *", "month", "1 to 11")]
    [InlineData("0 0 * * */8", "day-of-week", "1 to 7")]
    [InlineData("*/60 * * * * *", "seconds", "1 to 59")]
    [InlineData("*/2147483648 * * * *", "minute", "the step '2147483648'")]
    public void A_step_wider_than_the_field_is_rejected_naming_the_field_and_the_bound(
        string expression,
        string field,
        string detail
    )
    {
        var failure = Assert.Throws<FormatException>(() => CronExpression.Parse(expression));

        Assert.Contains($"cron {field} field", failure.Message, StringComparison.Ordinal);
        Assert.Contains(detail, failure.Message, StringComparison.Ordinal);
        Assert.False(CronExpression.TryParse(expression, out var parsed));
        Assert.Null(parsed);
    }

    [Theory]
    [InlineData("*/59 * * * *", "1970-01-01T00:59:00Z", "1970-01-01T01:00:00Z")]
    [InlineData("1/58 * * * *", "1970-01-01T00:01:00Z", "1970-01-01T00:59:00Z")]
    [InlineData("0 */23 * * *", "1970-01-01T23:00:00Z", "1970-01-02T00:00:00Z")]
    [InlineData("0 0 */30 * *", "1970-01-31T00:00:00Z", "1970-02-01T00:00:00Z")]
    [InlineData("0 0 * * */7", "1970-01-04T00:00:00Z", "1970-01-11T00:00:00Z")]
    [InlineData("*/1 * * * *", "1970-01-01T00:01:00Z", "1970-01-01T00:02:00Z")]
    public void A_step_at_the_field_bound_yields_only_the_values_it_names(
        string expression,
        string first,
        string second
    )
    {
        var cron = CronExpression.Parse(expression);

        var one = cron.GetNextOccurrence(Epoch);
        Assert.NotNull(one);
        var two = cron.GetNextOccurrence(one.Value);

        Assert.Equal(DateTimeOffset.Parse(first, null), one);
        Assert.Equal(DateTimeOffset.Parse(second, null), two);
    }
}
