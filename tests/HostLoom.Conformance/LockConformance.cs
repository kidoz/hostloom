using System.Diagnostics;
using HostLoom.Locking;
using HostLoom.Locking.Testing;
using Xunit;

namespace HostLoom.Conformance;

/// <summary>What a lock scenario needs: instances over one shared provider, a clock, and a fault injector.</summary>
public sealed class LockConformanceFixture
{
    /// <summary>Creates a new lock instance over the shared provider, like a second service instance.</summary>
    public required Func<IDistributedLock> CreateLock { get; init; }

    /// <summary>The clock every instance and the provider run on.</summary>
    public required ConformanceClock Clock { get; init; }

    /// <summary>The fault injector in front of the shared provider.</summary>
    public required FaultingLockProvider Faults { get; init; }
}

/// <summary>
/// Backend-neutral lock scenarios. The unit suite runs them on the in-process provider, with and
/// without a container; the integration suite runs the same methods on Redis.
/// </summary>
public static class LockConformance
{
    /// <summary>Every scenario by name, so a test project can enumerate them as theory data.</summary>
    public static IReadOnlyDictionary<
        string,
        Func<LockConformanceFixture, Task>
    > Scenarios { get; } =
        new Dictionary<string, Func<LockConformanceFixture, Task>>(StringComparer.Ordinal)
        {
            [nameof(Exclusivity_AcrossTwoInstances)] = Exclusivity_AcrossTwoInstances,
            [nameof(Contention_PastMaxWait_ThrowsLockNotAcquired)] =
                Contention_PastMaxWait_ThrowsLockNotAcquired,
            [nameof(LostLease_IsObservableAndTakenOver)] = LostLease_IsObservableAndTakenOver,
            [nameof(StaleOwner_CannotReleaseTheNewOwner)] = StaleOwner_CannotReleaseTheNewOwner,
            [nameof(ProviderUnavailable_ThrowsProviderUnavailable)] =
                ProviderUnavailable_ThrowsProviderUnavailable,
            [nameof(ReleaseFailure_IsLoggedNotThrown)] = ReleaseFailure_IsLoggedNotThrown,
            [nameof(ActionException_PropagatesAndReleases)] = ActionException_PropagatesAndReleases,
            [nameof(Extend_MovesTheLeaseEnd)] = Extend_MovesTheLeaseEnd,
            [nameof(OnLostCancel_CancelsTheActionToken)] = OnLostCancel_CancelsTheActionToken,
        };

    public static async Task Exclusivity_AcrossTwoInstances(LockConformanceFixture fixture)
    {
        var a = fixture.CreateLock();
        var b = fixture.CreateLock();

        var held = await a.TryAcquireAsync(
            "exclusive",
            new LockOptions { Lease = TimeSpan.FromSeconds(30) }
        );
        Assert.NotNull(held);
        Assert.True(held.IsHeld);
        Assert.Equal("exclusive", held.Key);

        var busy = await b.TryAcquireAsync("exclusive");
        Assert.Null(busy);

        await held.DisposeAsync();
        Assert.False(held.IsHeld);

        var afterRelease = await b.TryAcquireAsync("exclusive");
        Assert.NotNull(afterRelease);
        await afterRelease.DisposeAsync();
    }

    public static async Task Contention_PastMaxWait_ThrowsLockNotAcquired(
        LockConformanceFixture fixture
    )
    {
        var a = fixture.CreateLock();
        var b = fixture.CreateLock();
        var held = await a.TryAcquireAsync(
            "contended",
            new LockOptions { Lease = TimeSpan.FromMinutes(1) }
        );
        Assert.NotNull(held);

        var attempt = b.ExecuteWithLockAsync(
                "contended",
                static _ => ValueTask.FromResult(1),
                new LockOptions
                {
                    MaxWait = TimeSpan.FromMilliseconds(300),
                    Retry = LockRetryPolicy.Linear(10, TimeSpan.FromMilliseconds(50)),
                }
            )
            .AsTask();
        await PumpAsync(attempt, fixture.Clock, TimeSpan.FromMilliseconds(50));

        var exception = await Assert.ThrowsAsync<LockNotAcquiredException>(() => attempt);
        Assert.Equal("contended", exception.Key);
        Assert.True(exception.Attempts >= 1);
        Assert.True(exception.Waited <= TimeSpan.FromMilliseconds(350));
        await held.DisposeAsync();
    }

    public static async Task LostLease_IsObservableAndTakenOver(LockConformanceFixture fixture)
    {
        var a = fixture.CreateLock();
        var b = fixture.CreateLock();
        var held = await a.TryAcquireAsync(
            "lease",
            new LockOptions { Lease = TimeSpan.FromSeconds(1) }
        );
        Assert.NotNull(held);
        Assert.False(held.LostToken.IsCancellationRequested);

        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(1));

        Assert.False(held.IsHeld);
        Assert.True(held.LostToken.IsCancellationRequested);
        var takenOver = await b.TryAcquireAsync("lease");
        Assert.NotNull(takenOver);
        await takenOver.DisposeAsync();
        await held.DisposeAsync();
    }

    public static async Task StaleOwner_CannotReleaseTheNewOwner(LockConformanceFixture fixture)
    {
        var a = fixture.CreateLock();
        var b = fixture.CreateLock();
        var c = fixture.CreateLock();
        var stale = await a.TryAcquireAsync(
            "owner",
            new LockOptions { Lease = TimeSpan.FromSeconds(1) }
        );
        Assert.NotNull(stale);
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(1));
        var current = await b.TryAcquireAsync(
            "owner",
            new LockOptions { Lease = TimeSpan.FromMinutes(1) }
        );
        Assert.NotNull(current);

        await stale.DisposeAsync();

        Assert.Null(await c.TryAcquireAsync("owner"));
        Assert.True(current.IsHeld);
        await current.DisposeAsync();
    }

    public static async Task ProviderUnavailable_ThrowsProviderUnavailable(
        LockConformanceFixture fixture
    )
    {
        var a = fixture.CreateLock();
        fixture.Faults.FailAll(LockFailureKind.Unavailable);

        var exception = await Assert.ThrowsAsync<LockProviderUnavailableException>(async () =>
            await a.ExecuteWithLockAsync("down", static _ => ValueTask.FromResult(1))
        );

        Assert.Equal("down", exception.Key);
        Assert.Equal(LockFailureKind.Unavailable, exception.Kind);
        fixture.Faults.Heal();
    }

    public static async Task ReleaseFailure_IsLoggedNotThrown(LockConformanceFixture fixture)
    {
        var a = fixture.CreateLock();
        var held = await a.TryAcquireAsync(
            "release",
            new LockOptions { Lease = TimeSpan.FromSeconds(30) }
        );
        Assert.NotNull(held);
        fixture.Faults.FailAll(LockFailureKind.Other);

        await held.DisposeAsync();

        fixture.Faults.Heal();
        Assert.True(fixture.Faults.Faulted >= 1);
    }

    public static async Task ActionException_PropagatesAndReleases(LockConformanceFixture fixture)
    {
        var a = fixture.CreateLock();

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await a.ExecuteWithLockAsync<int>(
                "faulting-action",
                static _ => throw new InvalidOperationException("action failed")
            )
        );

        var afterwards = await a.TryAcquireAsync("faulting-action");
        Assert.NotNull(afterwards);
        await afterwards.DisposeAsync();
    }

    public static async Task Extend_MovesTheLeaseEnd(LockConformanceFixture fixture)
    {
        var a = fixture.CreateLock();
        var held = await a.TryAcquireAsync(
            "extend",
            new LockOptions { Lease = TimeSpan.FromSeconds(10) }
        );
        Assert.NotNull(held);
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(5));

        var before = fixture.Clock.Provider.GetUtcNow();
        Assert.True(await held.ExtendAsync(TimeSpan.FromSeconds(10)));
        var after = fixture.Clock.Provider.GetUtcNow();

        Assert.InRange(
            held.LeaseEnd,
            before + TimeSpan.FromSeconds(10),
            after + TimeSpan.FromSeconds(10)
        );
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(6));
        Assert.True(held.IsHeld);
        await held.DisposeAsync();
    }

    public static async Task OnLostCancel_CancelsTheActionToken(LockConformanceFixture fixture)
    {
        var a = fixture.CreateLock();
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var execution = a.ExecuteWithLockAsync(
                "cancel-on-loss",
                async token =>
                {
                    started.TrySetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    return 1;
                },
                new LockOptions
                {
                    Lease = TimeSpan.FromSeconds(1),
                    OnLost = LostLeaseBehavior.Cancel,
                }
            )
            .AsTask();
        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));

        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(1));

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => execution);
    }

    /// <summary>
    /// Drives a task that waits on the manual clock: advances the clock in steps and yields until
    /// the task completes, bounded by wall-clock time so a regression fails loudly.
    /// </summary>
    public static async Task PumpAsync(Task task, ConformanceClock clock, TimeSpan step)
    {
        var start = Stopwatch.GetTimestamp();
        while (!task.IsCompleted)
        {
            if (Stopwatch.GetElapsedTime(start) > TimeSpan.FromSeconds(10))
            {
                Assert.Fail("The task did not complete while the clock was advanced.");
            }

            await clock.AdvanceAsync(step);
            await Task.Yield();
        }
    }
}
