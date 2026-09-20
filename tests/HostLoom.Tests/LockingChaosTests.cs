using HostLoom.Locking;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Controlled faults around acquisition: a crowd contending for one key, a provider that answers
/// nothing, and an outage that heals between two calls. The hypothesis is the one a lock exists
/// for: two holders are never admitted at once, a provider whose state is unknown produces a typed
/// unavailable error rather than a reported acquisition, and every lease is given back however the
/// call ended.
/// </summary>
public sealed class LockingChaosTests
{
    private static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task A_crowd_contending_for_one_key_never_has_two_holders_and_returns_every_lease()
    {
        const int callers = 32;
        const int rounds = 4;
        var token = TestContext.Current.CancellationToken;
        var provider = new InMemoryLockProvider();
        await using var locks = new DistributedLock(
            new LockingOptions
            {
                Namespace = "inventory",
                DefaultLease = TimeSpan.FromSeconds(30),
                // A wall-clock bound rather than a lease bound: the test fails as an assertion
                // rather than as a hang if the lock ever stops handing the key on.
                Retry = LockRetryPolicy.Interval(10_000, TimeSpan.FromMilliseconds(1)),
            },
            provider
        );
        var meter = new Meter();
        var options = new LockOptions { MaxWait = Bounded };

        var work = Enumerable
            .Range(0, callers)
            .Select(_ =>
                Task.Run(
                    async () =>
                    {
                        for (var round = 0; round < rounds; round++)
                        {
                            await locks.ExecuteWithLockAsync(
                                "catalog:refresh",
                                async _ =>
                                {
                                    meter.Enter();
                                    await Task.Yield();
                                    meter.Exit();
                                },
                                options,
                                token
                            );
                        }
                    },
                    token
                )
            )
            .ToArray();
        await Task.WhenAll(work).WaitAsync(Bounded, token);

        Assert.Equal(1, meter.Peak);
        Assert.Equal(callers * rounds, meter.Entries);
        Assert.Equal(0, meter.InFlight);
        // Every acquisition was released, including the ones that had to wait for their turn.
        Assert.Equal(0, provider.Count);
    }

    [Fact]
    public async Task A_provider_that_answers_nothing_reports_no_lock_and_leaves_none_behind()
    {
        var token = TestContext.Current.CancellationToken;
        var backend = new InMemoryLockProvider();
        var faulting = new HostLoom.Locking.Testing.FaultingLockProvider(backend);
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "inventory" },
            faulting
        );
        faulting.FailAll(LockFailureKind.Unavailable);

        // Unknown is not free and not held: the caller is told the provider failed, and is never
        // handed a lease it could act on.
        var unavailable = await Assert.ThrowsAsync<LockProviderUnavailableException>(async () =>
            await locks.TryAcquireAsync(
                "catalog:refresh",
                new LockOptions { MaxWait = TimeSpan.Zero },
                token
            )
        );
        Assert.Equal(LockFailureKind.Unavailable, unavailable.Kind);
        Assert.Equal(0, backend.Count);

        var alsoUnavailable = await Assert.ThrowsAsync<LockProviderUnavailableException>(async () =>
            await locks.ExecuteWithLockAsync(
                "catalog:refresh",
                _ => throw new InvalidOperationException("the guarded work must not run"),
                new LockOptions { MaxWait = TimeSpan.Zero },
                token
            )
        );
        Assert.Equal(LockFailureKind.Unavailable, alsoUnavailable.Kind);

        // Recovery: nothing was left behind for the outage to block, so the first call after it
        // acquires immediately.
        faulting.Heal();
        await using var handle = await locks.TryAcquireAsync(
            "catalog:refresh",
            cancellationToken: token
        );
        Assert.NotNull(handle);
        Assert.True(handle.IsHeld);
        Assert.Equal(1, backend.Count);
        await handle.DisposeAsync();
        Assert.Equal(0, backend.Count);
    }

    [Fact]
    public async Task A_provider_fault_is_surfaced_rather_than_absorbed_into_the_retry_budget()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var backend = new InMemoryLockProvider(clock);
        var faulting = new FaultingLockProvider(
            backend,
            LockOperation.Acquire,
            LockFailureKind.Timeout,
            count: 1
        );
        await using var locks = new DistributedLock(
            new LockingOptions
            {
                Namespace = "inventory",
                Retry = LockRetryPolicy.Interval(10, TimeSpan.FromSeconds(1)),
            },
            faulting,
            clock
        );

        // The retry policy is for a key someone else holds. A provider that failed is a different
        // answer: the caller learns at once instead of waiting out ten retries for a backend that
        // may be down for minutes.
        var failure = await Assert.ThrowsAsync<LockProviderUnavailableException>(async () =>
            await locks.TryAcquireAsync(
                "catalog:refresh",
                new LockOptions { MaxWait = Bounded },
                token
            )
        );
        Assert.Equal(LockFailureKind.Timeout, failure.Kind);
        Assert.Equal(1, faulting.Faulted);
        Assert.Equal(0, clock.PendingTimers);

        // Recovery is the caller's to decide: the very next attempt succeeds.
        await using var handle = await locks.TryAcquireAsync(
            "catalog:refresh",
            cancellationToken: token
        );
        Assert.NotNull(handle);
        Assert.True(handle.IsHeld);
    }

    /// <summary>Counts holders inside the guarded section, and the most ever seen at once.</summary>
    private sealed class Meter
    {
        private int _inFlight;
        private int _peak;
        private int _entries;

        public int InFlight => Volatile.Read(ref _inFlight);

        public int Peak => Volatile.Read(ref _peak);

        public int Entries => Volatile.Read(ref _entries);

        public void Enter()
        {
            Interlocked.Increment(ref _entries);
            var current = Interlocked.Increment(ref _inFlight);
            var peak = Volatile.Read(ref _peak);
            while (current > peak)
            {
                var actual = Interlocked.CompareExchange(ref _peak, current, peak);
                if (actual == peak)
                {
                    break;
                }

                peak = actual;
            }
        }

        public void Exit() => Interlocked.Decrement(ref _inFlight);
    }
}
