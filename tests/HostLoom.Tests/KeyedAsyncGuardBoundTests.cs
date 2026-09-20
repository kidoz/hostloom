using HostLoom.Caching.Internal;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The single-flight map is bounded: idle guards are reclaimed early once it is full, and keys
/// that still find no room share striped guards without losing per-key exclusivity.
/// </summary>
public sealed class KeyedAsyncGuardBoundTests
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Fact]
    public async Task IdleGuards_AreReclaimedAtOnceWhenTheMapIsFull()
    {
        var clock = new TestClock();
        using var guard = new KeyedAsyncGuard(clock, maxGuards: 4);
        for (var i = 0; i < 4; i++)
        {
            using var held = await guard.AcquireAsync($"catalog:{i}", Token);
        }

        // Released guards stay for the idle window, so the map is full of idle entries.
        Assert.Equal(4, guard.TrackedCount);
        Assert.Equal(0, guard.ActiveCount);

        // No idle time has passed, yet a fifth key finds room by reclaiming the idle four.
        using (await guard.AcquireAsync("catalog:4", Token))
        {
            Assert.Equal(1, guard.TrackedCount);
            Assert.Equal(1, guard.ActiveCount);
        }

        Assert.Equal(0, guard.Overflows);

        // The ordinary idle reclaim still applies below the bound.
        clock.Advance(TimeSpan.FromMinutes(11));
        guard.Reclaim(TimeSpan.FromMinutes(10));
        Assert.Equal(0, guard.TrackedCount);
    }

    [Fact]
    public async Task HeldGuards_OverflowIntoStripesThatStayExclusivePerKey()
    {
        var clock = new TestClock();
        using var guard = new KeyedAsyncGuard(clock, maxGuards: 2);
        var first = await guard.AcquireAsync("catalog:eu", Token);
        var second = await guard.AcquireAsync("catalog:us", Token);
        Assert.Equal(2, guard.TrackedCount);

        // Both tracked guards are held, so a third key is guarded by a stripe instead.
        var third = await guard.AcquireAsync("rates:eu", Token);
        Assert.Equal(1, guard.Overflows);
        Assert.Equal(2, guard.TrackedCount);
        Assert.Equal(3, guard.ActiveCount);

        // The striped key is still exclusive: nobody else gets in while it is held.
        Assert.False(guard.TryAcquire("rates:eu", out _));
        var waiter = guard.AcquireAsync("rates:eu", Token).AsTask();
        Assert.False(waiter.IsCompleted);

        // Freeing a tracked guard lets the key get a guard of its own, which must still respect
        // the stripe holder rather than run alongside it.
        first.Dispose();
        Assert.False(guard.TryAcquire("rates:eu", out _));
        Assert.Equal(2, guard.TrackedCount);

        third.Dispose();
        var fourth = await waiter;
        Assert.True(fourth.Waited);
        Assert.False(guard.TryAcquire("rates:eu", out _));

        fourth.Dispose();
        Assert.True(guard.TryAcquire("rates:eu", out var fifth));
        fifth.Dispose();
        second.Dispose();
        Assert.Equal(0, guard.ActiveCount);

        clock.Advance(TimeSpan.FromMinutes(11));
        guard.Reclaim(TimeSpan.FromMinutes(10));
        Assert.Equal(0, guard.TrackedCount);
    }

    [Fact]
    public async Task CancelledWaitOnAStripe_ReleasesEveryReference()
    {
        var clock = new TestClock();
        using var guard = new KeyedAsyncGuard(clock, maxGuards: 1);
        var holder = await guard.AcquireAsync("catalog:eu", Token);
        var striped = await guard.AcquireAsync("rates:eu", Token);
        using var cancellation = new CancellationTokenSource();
        var waiter = guard.AcquireAsync("rates:eu", cancellation.Token).AsTask();
        Assert.False(waiter.IsCompleted);

        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiter);
        Assert.Equal(2, guard.ActiveCount);
        striped.Dispose();
        holder.Dispose();
        Assert.Equal(0, guard.ActiveCount);
    }
}
