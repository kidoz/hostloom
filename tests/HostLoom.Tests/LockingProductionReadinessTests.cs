using HostLoom.Locking;
using Xunit;

namespace HostLoom.Tests;

public sealed class LockingProductionReadinessTests
{
    [Theory(Timeout = 10_000)]
    [InlineData(LostLeaseBehavior.Observe, false)]
    [InlineData(LostLeaseBehavior.Observe, true)]
    [InlineData(LostLeaseBehavior.Cancel, false)]
    [InlineData(LostLeaseBehavior.Cancel, true)]
    public async Task Execute_does_not_start_an_action_after_its_acquisition_lease_has_expired(
        LostLeaseBehavior onLost,
        bool autoExtend
    )
    {
        var clock = new TestClock();
        var provider = new InMemoryLockProvider(clock);
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            new SlowLockProvider(provider, clock, TimeSpan.FromSeconds(2)),
            clock
        );
        await using var competitor = new DistributedLock(
            new LockingOptions { Namespace = "catalog", DetectReentrancy = false },
            provider,
            clock
        );
        var actionStarted = false;
        var overlappingOwner = false;

        // The backend grants a one-second lease; its successful reply takes two seconds.
        // Even a cancellation-aware action must not begin with an already expired lease.
        var failure = await Record.ExceptionAsync(async () =>
            await locks.ExecuteWithLockAsync(
                "inventory",
                async token =>
                {
                    token.ThrowIfCancellationRequested();
                    actionStarted = true;
                    await using var successor = await competitor.TryAcquireAsync(
                        "inventory",
                        cancellationToken: token
                    );
                    overlappingOwner = successor is not null;
                },
                new LockOptions
                {
                    Lease = TimeSpan.FromSeconds(1),
                    OnLost = onLost,
                    AutoExtend = autoExtend,
                },
                TestContext.Current.CancellationToken
            )
        );

        Assert.False(overlappingOwner, "The action ran while a successor held the same lock.");
        Assert.False(actionStarted, "An expired acquisition must not start protected work.");
        var unavailable = Assert.IsType<LockProviderUnavailableException>(failure);
        Assert.Equal(LockFailureKind.Timeout, unavailable.Kind);
        Assert.Equal(1, unavailable.Attempts);
        Assert.Equal(TimeSpan.FromSeconds(2), unavailable.Waited);
    }

    [Theory(Timeout = 10_000)]
    [InlineData(1000, false)]
    [InlineData(1000, true)]
    [InlineData(2000, false)]
    [InlineData(2000, true)]
    public async Task TryAcquire_rejects_a_successful_reply_at_or_after_expiry(
        int latencyMilliseconds,
        bool autoExtend
    )
    {
        var clock = new TestClock();
        var backend = new InMemoryLockProvider(clock);
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            new SlowLockProvider(backend, clock, TimeSpan.FromMilliseconds(latencyMilliseconds)),
            clock
        );
        var failure = await Assert.ThrowsAsync<LockProviderUnavailableException>(async () =>
        {
            await using var handle = await locks.TryAcquireAsync(
                "inventory",
                new LockOptions
                {
                    Lease = TimeSpan.FromSeconds(1),
                    AutoExtend = autoExtend,
                    MaxWait = TimeSpan.Zero,
                },
                TestContext.Current.CancellationToken
            );
        });
        Assert.Equal(LockFailureKind.Timeout, failure.Kind);
        Assert.Equal(1, failure.Attempts);
        Assert.Equal(0, clock.PendingTimers);
    }
}
