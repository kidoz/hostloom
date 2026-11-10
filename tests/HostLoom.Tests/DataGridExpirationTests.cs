using HostLoom.DataGrid;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridExpirationTests
{
    [Fact]
    public void DeadlineAndRemainingLifetimePreserveSingleTickPrecision()
    {
        var time = new DataGridTimestamp(12345);
        var expiration = DataGridExpiration.After(time, TimeSpan.FromTicks(3));
        Assert.Equal(new DataGridTimestamp(12348), expiration.Deadline);
        Assert.Equal(TimeSpan.FromTicks(3), expiration.GetRemainingTimeToLive(time));
        Assert.Equal(TimeSpan.FromTicks(1), expiration.GetRemainingTimeToLive(new(12347)));
        Assert.False(expiration.IsExpired(new(12347)));
        Assert.True(expiration.IsExpired(new(12348)));
        Assert.True(expiration.IsExpired(new(12349)));
        Assert.Equal(TimeSpan.Zero, expiration.GetRemainingTimeToLive(new(12349)));
    }

    [Fact]
    public void MissingDeadlineMeansNoExpirationEvenAtMaximumTime()
    {
        Assert.Equal(DataGridExpiration.Never, default);
        Assert.Null(DataGridExpiration.Never.Deadline);
        var maximum = DataGridTimestamp.FromUtc(DateTimeOffset.MaxValue);
        Assert.False(DataGridExpiration.Never.IsExpired(maximum));
        Assert.Null(DataGridExpiration.Never.GetRemainingTimeToLive(maximum));
        Assert.True(DataGridExpiration.At(default).IsExpired(default));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(-1)]
    [InlineData(long.MinValue)]
    public void NonpositiveLifetimesAreRejected(long ticks) =>
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DataGridExpiration.After(default, TimeSpan.FromTicks(ticks))
        );

    [Fact]
    public void DeadlineOverflowIsRejectedWithoutClamping()
    {
        var maximum = DataGridTimestamp.FromUtc(DateTimeOffset.MaxValue);
        var penultimate = new DataGridTimestamp(maximum.UtcTicks - 1);
        Assert.Equal(
            maximum,
            DataGridExpiration.After(penultimate, TimeSpan.FromTicks(1)).Deadline
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DataGridExpiration.After(maximum, TimeSpan.FromTicks(1))
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DataGridExpiration.After(new(1), TimeSpan.MaxValue)
        );
    }

    [Fact]
    public void TimestampRejectsOutOfRangeRestoredState()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new DataGridTimestamp(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridTimestamp(DateTimeOffset.MaxValue.Ticks + 1)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() => new DataGridTimestamp(long.MaxValue));
        Assert.Equal(0, default(DataGridTimestamp).UtcTicks);
    }

    [Fact]
    public void BackwardClockAndLeaderChangesCannotReviveAnExpiredDeadline()
    {
        var clock = new ManualClock(new DateTimeOffset(2026, 11, 10, 12, 0, 0, TimeSpan.Zero));
        var applied = DataGridTimestamp.Sample(clock);
        var expiration = DataGridExpiration.After(applied, TimeSpan.FromSeconds(1));
        clock.UtcNow += TimeSpan.FromSeconds(2);
        applied = applied.AdvanceTo(DataGridTimestamp.Sample(clock));
        Assert.True(expiration.IsExpired(applied));

        var restoredTime = new DataGridTimestamp(applied.UtcTicks);
        var restoredExpiry = DataGridExpiration.At(new(expiration.Deadline!.Value.UtcTicks));
        clock.UtcNow -= TimeSpan.FromHours(1);
        var newLeaderTime = restoredTime.AdvanceTo(DataGridTimestamp.Sample(clock));
        Assert.Equal(applied, newLeaderTime);
        Assert.True(restoredExpiry.IsExpired(newLeaderTime));
        Assert.Equal(TimeSpan.Zero, restoredExpiry.GetRemainingTimeToLive(newLeaderTime));
    }

    [Fact]
    public void BackwardJumpFreezesRemainingLifetimeUntilClockCatchesUp()
    {
        var time = new DataGridTimestamp(100);
        var expiration = DataGridExpiration.After(time, TimeSpan.FromTicks(10));
        time = time.AdvanceTo(new(90));
        Assert.Equal(TimeSpan.FromTicks(10), expiration.GetRemainingTimeToLive(time));
        time = time.AdvanceTo(new(105));
        Assert.Equal(TimeSpan.FromTicks(5), expiration.GetRemainingTimeToLive(time));
    }

    [Fact]
    public void SamplingDoesNotAdvanceAppliedTimeAndForwardJumpCanExpireEntries()
    {
        var clock = new ManualClock(new DateTimeOffset(2026, 11, 10, 12, 0, 0, TimeSpan.Zero));
        var applied = DataGridTimestamp.Sample(clock);
        var expiration = DataGridExpiration.After(applied, TimeSpan.FromMinutes(1));
        clock.UtcNow += TimeSpan.FromDays(1);
        var proposed = DataGridTimestamp.Sample(clock);
        Assert.False(expiration.IsExpired(applied));
        Assert.True(expiration.IsExpired(applied.AdvanceTo(proposed)));
        Assert.Throws<ArgumentNullException>(() => DataGridTimestamp.Sample(null!));
    }

    [Fact]
    public void ReplayAndSnapshotRestoreProduceTheSameExpirationDecisions()
    {
        // A small independent integer model checks time and expiry over a fixed, replayable trace.
        long[] proposals = [100, 105, 104, 109, 120, 90, 121];
        var applied = default(DataGridTimestamp);
        long modelTime = 0;
        var expiration = DataGridExpiration.At(new(110));
        foreach (var proposed in proposals)
        {
            modelTime = Math.Max(modelTime, proposed);
            applied = new DataGridTimestamp(applied.UtcTicks).AdvanceTo(new(proposed));
            expiration = DataGridExpiration.At(new(expiration.Deadline!.Value.UtcTicks));
            Assert.Equal(modelTime, applied.UtcTicks);
            Assert.Equal(modelTime >= 110, expiration.IsExpired(applied));
            Assert.Equal(
                TimeSpan.FromTicks(Math.Max(0, 110 - modelTime)),
                expiration.GetRemainingTimeToLive(applied)
            );
        }
    }

    [Fact]
    public void ReplacementHasItsOwnDeadlineWhileRetryRetainsTheOriginal()
    {
        var original = DataGridExpiration.After(new(100), TimeSpan.FromTicks(10));
        var retained = DataGridExpiration.At(original.Deadline!.Value);
        var replacement = DataGridExpiration.After(new(105), TimeSpan.FromTicks(20));
        Assert.Equal(original, retained);
        Assert.True(original.IsExpired(new(110)));
        Assert.False(replacement.IsExpired(new(110)));
        Assert.Equal(TimeSpan.FromTicks(15), replacement.GetRemainingTimeToLive(new(110)));
    }

    [Fact]
    public void OffsetConversionUsesUtcTicksWithoutRounding()
    {
        var instant = new DateTimeOffset(2026, 11, 10, 12, 0, 0, TimeSpan.FromHours(3)).AddTicks(1);
        Assert.Equal(
            DataGridTimestamp.FromUtc(instant.ToUniversalTime()),
            DataGridTimestamp.FromUtc(instant)
        );
        Assert.Equal(instant.UtcTicks, DataGridTimestamp.FromUtc(instant).UtcTicks);
    }

    private sealed class ManualClock(DateTimeOffset utcNow) : TimeProvider
    {
        public DateTimeOffset UtcNow { get; set; } = utcNow;

        public override DateTimeOffset GetUtcNow() => UtcNow;
    }
}
