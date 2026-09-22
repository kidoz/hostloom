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
        var provider = new RecordingLockProvider(
            new SlowLockProvider(backend, clock, TimeSpan.FromMilliseconds(latencyMilliseconds))
        );
        var logger = new RecordingLogger<DistributedLock>();
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            provider,
            clock,
            logger
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

        // The grant was rejected, not forgotten: whatever the backend still holds for this owner
        // is given back once, with the owner token of the rejected attempt. Here the backend's
        // own lease ran out with the reply, so the owner-checked release finds nothing.
        var acquire = Assert.Single(provider.Acquires);
        var release = Assert.Single(provider.Releases);
        Assert.Equal(acquire.Key, release.Key);
        Assert.Equal(acquire.Owner, release.Owner);
        Assert.False(release.Released);
        Assert.Equal(0, backend.Count);
        var entry = Assert.Single(
            logger.Entries,
            e => e.Event.Id == LockingEvents.OrphanRelease.Id
        );
        Assert.Contains("absent", entry.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(acquire.Owner, entry.Message, StringComparison.Ordinal);
    }

    [Fact(Timeout = 10_000)]
    public async Task Reordered_extension_replies_do_not_outlive_the_backends_latest_lease()
    {
        var clock = new TestClock();
        var backend = new InMemoryLockProvider(clock);
        var provider = new DelayedFirstExtensionProvider(backend);
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            provider,
            clock
        );
        await using var competitor = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            backend,
            clock
        );
        var token = TestContext.Current.CancellationToken;
        await using var handle = await locks.TryAcquireAsync(
            "inventory",
            new LockOptions { Lease = TimeSpan.FromSeconds(5) },
            token
        );
        Assert.NotNull(handle);

        var earlier = handle.ExtendAsync(TimeSpan.FromSeconds(10), token).AsTask();
        await provider.FirstApplied.Task.WaitAsync(token);
        // Without serialization, this command changes the backend lease and completes before
        // the earlier reply is returned. A serialized implementation may queue it instead.
        var later = handle.ExtendAsync(TimeSpan.FromSeconds(1), token).AsTask();
        provider.ReturnFirst.TrySetResult();
        await earlier;
        Assert.True(await later);
        clock.Advance(TimeSpan.FromSeconds(1));

        await using var successor = await competitor.TryAcquireAsync(
            "inventory",
            cancellationToken: token
        );
        Assert.NotNull(successor);
        Assert.False(handle.IsHeld, "An older reply revived ownership beyond the backend lease.");
        Assert.True(handle.LostToken.IsCancellationRequested);
    }

    [Fact(Timeout = 10_000)]
    public async Task Cancelling_a_queued_renewal_does_not_dispatch_or_block_the_next_renewal()
    {
        var clock = new TestClock();
        var provider = new DelayedFirstExtensionProvider(new InMemoryLockProvider(clock));
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            provider,
            clock
        );
        var token = TestContext.Current.CancellationToken;
        await using var handle = await locks.TryAcquireAsync("inventory", cancellationToken: token);
        Assert.NotNull(handle);
        var first = handle.ExtendAsync(TimeSpan.FromSeconds(10), token).AsTask();
        await provider.FirstApplied.Task.WaitAsync(token);
        using var cancelled = new CancellationTokenSource();
        var waiting = handle.ExtendAsync(TimeSpan.FromSeconds(1), cancelled.Token).AsTask();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.Equal(1, provider.Extensions);
        provider.ReturnFirst.TrySetResult();
        Assert.True(await first);
        Assert.True(await handle.ExtendAsync(TimeSpan.FromSeconds(2), token));
        Assert.Equal(2, provider.Extensions);
    }

    [Fact(Timeout = 10_000)]
    public async Task Cancelling_an_inflight_renewal_releases_the_queue()
    {
        var clock = new TestClock();
        var provider = new DelayedFirstExtensionProvider(new InMemoryLockProvider(clock));
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            provider,
            clock
        );
        var token = TestContext.Current.CancellationToken;
        await using var handle = await locks.TryAcquireAsync("inventory", cancellationToken: token);
        Assert.NotNull(handle);
        using var cancelled = new CancellationTokenSource();
        var first = handle.ExtendAsync(TimeSpan.FromSeconds(10), cancelled.Token).AsTask();
        await provider.FirstApplied.Task.WaitAsync(token);
        var waiting = handle.ExtendAsync(TimeSpan.FromSeconds(1), token).AsTask();
        await cancelled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => first);
        Assert.True(await waiting);
        Assert.Equal(2, provider.Extensions);
        clock.Advance(TimeSpan.FromSeconds(1));
        Assert.False(handle.IsHeld);
    }

    [Theory(Timeout = 10_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_queued_renewal_does_not_dispatch_after_expiry_or_disposal(bool dispose)
    {
        var clock = new TestClock();
        var backend = new InMemoryLockProvider(clock);
        var provider = new DelayedFirstExtensionProvider(backend);
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            provider,
            clock
        );
        var token = TestContext.Current.CancellationToken;
        await using var handle = await locks.TryAcquireAsync(
            "inventory",
            new LockOptions { Lease = TimeSpan.FromSeconds(5) },
            token
        );
        Assert.NotNull(handle);
        var first = handle.ExtendAsync(TimeSpan.FromSeconds(10), token).AsTask();
        await provider.FirstApplied.Task.WaitAsync(token);
        var waiting = handle.ExtendAsync(TimeSpan.FromSeconds(20), token).AsTask();
        if (dispose)
        {
            await handle.DisposeAsync();
        }
        else
        {
            // The local deadline remains five seconds until the first reply is received.
            clock.Advance(TimeSpan.FromSeconds(5));
            Assert.True(handle.LostToken.IsCancellationRequested);
        }
        provider.ReturnFirst.TrySetResult();
        Assert.False(await first);
        Assert.False(await waiting);
        Assert.Equal(1, provider.Extensions);
        Assert.False(handle.IsHeld);
        clock.Advance(TimeSpan.FromSeconds(10));
        Assert.True(
            await backend.TryAcquireAsync(
                "catalog:lock:inventory",
                "successor",
                TimeSpan.FromSeconds(5),
                token
            )
        );
        await handle.DisposeAsync();
        Assert.False(
            await backend.TryAcquireAsync(
                "catalog:lock:inventory",
                "another",
                TimeSpan.FromSeconds(5),
                token
            )
        );
    }

    [Fact(Timeout = 10_000)]
    public async Task Manual_renewal_is_serialized_with_an_automatic_heartbeat()
    {
        var clock = new TestClock();
        var provider = new DelayedFirstExtensionProvider(new InMemoryLockProvider(clock));
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            provider,
            clock
        );
        var token = TestContext.Current.CancellationToken;
        await using var handle = await locks.TryAcquireAsync(
            "inventory",
            new LockOptions { Lease = TimeSpan.FromSeconds(5), AutoExtend = true },
            token
        );
        Assert.NotNull(handle);
        clock.Advance(TimeSpan.FromMilliseconds(2500));
        await provider.FirstApplied.Task.WaitAsync(token);
        var manual = handle.ExtendAsync(TimeSpan.FromSeconds(1), token).AsTask();
        Assert.Equal(1, provider.Extensions);
        provider.ReturnFirst.TrySetResult();
        Assert.True(await manual);
        Assert.Equal(2, provider.Extensions);
        Assert.Equal(clock.GetUtcNow() + TimeSpan.FromSeconds(1), handle.LeaseEnd);
    }

    private sealed class DelayedFirstExtensionProvider(ILockProvider inner) : ILockProvider
    {
        private int _extensions;

        public int Extensions => Volatile.Read(ref _extensions);

        public TaskCompletionSource FirstApplied { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource ReturnFirst { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public ValueTask<bool> TryAcquireAsync(
            string key,
            string owner,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        ) => inner.TryAcquireAsync(key, owner, lease, cancellationToken);

        public ValueTask<bool> ReleaseAsync(
            string key,
            string owner,
            CancellationToken cancellationToken = default
        ) => inner.ReleaseAsync(key, owner, cancellationToken);

        public async ValueTask<bool> ExtendAsync(
            string key,
            string owner,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        )
        {
            var ordinal = Interlocked.Increment(ref _extensions);
            var extended = await inner.ExtendAsync(key, owner, lease, cancellationToken);
            if (ordinal == 1)
            {
                FirstApplied.TrySetResult();
                await ReturnFirst.Task.WaitAsync(cancellationToken);
            }
            return extended;
        }
    }
}
