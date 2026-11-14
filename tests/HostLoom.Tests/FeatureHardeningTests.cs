using System.Buffers;
using System.Runtime.CompilerServices;
using HostLoom.Caching;
using HostLoom.Locking;
using HostLoom.Scheduling;
using HostLoom.Transport.InMemory;
using HostLoom.Valkey.Internal;
using NSubstitute;
using ValkeyDotNet;
using Xunit;

namespace HostLoom.Tests;

public sealed class FeatureHardeningTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Expiration = TimeSpan.FromMinutes(1);
    private static readonly ICacheValueSerializer Serializer =
        SystemTextJsonCacheValueSerializer.CreateReflectionBased();

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Conditional_encoding_failure_is_never_reported_as_an_existing_key(
        bool oversized
    )
    {
        var store = new InMemoryDistributedCacheStore();
        var options = new CachingOptions { Namespace = "hardening", MaxPayloadBytes = 1024 };
        await using var cache = new TieredCache(
            options,
            store,
            oversized ? Serializer : new BrokenSerializer()
        );
        foreach (
            var behavior in new[] { UnavailableBehavior.ReturnFalse, UnavailableBehavior.Throw }
        )
        {
            var entry = new CacheEntryOptions(Expiration) { OnUnavailable = behavior };
            if (oversized)
                await Assert.ThrowsAsync<CacheUnavailableException>(async () =>
                    await cache.SetIfAbsentAsync(
                        "catalog",
                        new string('x', 2048),
                        entry,
                        TestContext.Current.CancellationToken
                    )
                );
            else
                await Assert.ThrowsAsync<CacheUnavailableException>(async () =>
                    await cache.SetIfAbsentAsync(
                        "catalog",
                        "books",
                        entry,
                        TestContext.Current.CancellationToken
                    )
                );
        }
        Assert.Equal(0, store.Count);
    }

    [Theory]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task An_explicit_write_supersedes_an_older_factory_in_both_tiers(
        bool distributed,
        bool nullResult
    )
    {
        var token = TestContext.Current.CancellationToken;
        var store = distributed ? new InMemoryDistributedCacheStore() : null;
        await using var cache = new TieredCache(
            new CachingOptions { Namespace = "hardening" },
            store,
            distributed ? Serializer : null
        );
        var started = Signal();
        var release = Signal();
        var entry = new CacheEntryOptions(Expiration) { NullExpiration = Expiration };
        var pending = cache
            .GetOrCreateAsync<string?>(
                "catalog",
                async _ =>
                {
                    started.TrySetResult();
                    await release.Task.WaitAsync(Bound, token);
                    return nullResult ? null : "old";
                },
                entry,
                token
            )
            .AsTask();
        await started.Task.WaitAsync(Bound, token);
        try
        {
            await cache.SetAsync("catalog", "new", entry, token);
        }
        finally
        {
            release.TrySetResult();
        }
        await pending.WaitAsync(Bound, token);
        Assert.Equal("new", await cache.GetAsync<string>("catalog", token));
        if (distributed)
        {
            await using var reader = new TieredCache(
                new CachingOptions { Namespace = "hardening" },
                store,
                Serializer
            );
            Assert.Equal("new", await reader.GetAsync<string>("catalog", token));
        }
    }

    [Fact]
    public async Task Strict_removal_exposes_a_distributed_failure()
    {
        var store = Substitute.For<IDistributedCacheStore>();
        store
            .RemoveAsync(Arg.Any<IReadOnlyCollection<string>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
                ValueTask.FromException(
                    new CacheStoreException(CacheFailureKind.Unavailable, "offline")
                )
            );
        await using var cache = new TieredCache(
            new CachingOptions { Namespace = "hardening" },
            store,
            Serializer
        );
        await cache.RemoveAsync("catalog", TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<CacheUnavailableException>(async () =>
            await cache.RemoveAsync(
                "catalog",
                UnavailableBehavior.Throw,
                TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task Large_bulk_removal_publishes_a_bounded_flush()
    {
        var channel = Substitute.For<ICacheInvalidationChannel>();
        await using var cache = new TieredCache(
            new CachingOptions { Namespace = "hardening" },
            channel: channel
        );
        await cache.RemoveAsync(
            Enumerable.Range(0, 10_001).Select(i => "catalog:" + i),
            TestContext.Current.CancellationToken
        );
        await channel
            .Received(1)
            .PublishAsync(
                Arg.Is<CacheInvalidation>(message => message.FlushAll),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact]
    public async Task A_lease_is_not_held_after_its_monotonic_deadline_even_when_timers_are_delayed()
    {
        var clock = new DelayedTimerClock();
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "hardening" },
            new InMemoryLockProvider(),
            clock
        );
        await using var handle = await locks.TryAcquireAsync(
            "catalog",
            new LockOptions { Lease = Expiration },
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(handle);
        clock.Elapsed = Expiration;
        Assert.False(handle.IsHeld);
        Assert.True(handle.LostToken.IsCancellationRequested);
        Assert.False(await handle.ExtendAsync(Expiration, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task An_early_expiry_timer_is_rearmed_until_the_monotonic_deadline()
    {
        var clock = new DelayedTimerClock();
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "early-timer" },
            new InMemoryLockProvider(),
            clock
        );
        await using var handle = await locks.TryAcquireAsync(
            "catalog",
            new LockOptions { Lease = TimeSpan.FromSeconds(10) },
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(handle);
        clock.Elapsed = TimeSpan.FromSeconds(9);
        clock.FireTimers(TimeSpan.FromSeconds(10));
        Assert.True(handle.IsHeld);
        Assert.False(handle.LostToken.IsCancellationRequested);
        clock.Elapsed = TimeSpan.FromSeconds(10);
        clock.FireTimers(TimeSpan.FromSeconds(1));
        Assert.True(handle.LostToken.IsCancellationRequested);
    }

    [Fact]
    public async Task Heartbeat_retry_uses_the_monotonic_lease_after_a_wall_clock_jump()
    {
        var clock = new DelayedTimerClock();
        var provider = Substitute.For<ILockProvider>();
        provider
            .TryAcquireAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(ValueTask.FromResult(true));
        provider
            .ExtendAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(_ => ValueTask.FromException<bool>(new IOException("offline")));
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "heartbeat" },
            provider,
            clock
        );
        await using var handle = await locks.TryAcquireAsync(
            "catalog",
            new LockOptions { Lease = TimeSpan.FromSeconds(20), AutoExtend = true },
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(handle);
        clock.WallTime = DateTimeOffset.UnixEpoch.AddDays(1);
        clock.AdvanceTimers(TimeSpan.FromSeconds(10));
        Assert.True(handle.IsHeld);
        Assert.Equal(1, clock.TimersDueAt(DateTimeOffset.UnixEpoch.AddSeconds(15)));
    }

    [Theory]
    [InlineData(ValkeyCommandDeliveryStatus.NotSent, LockFailureKind.Unavailable)]
    [InlineData(ValkeyCommandDeliveryStatus.MayHaveBeenSent, LockFailureKind.Other)]
    public void Valkey_connection_loss_preserves_delivery_certainty(
        ValkeyCommandDeliveryStatus status,
        LockFailureKind expected
    )
    {
        var failure = ValkeyFailures.Lock(
            new ValkeyConnectionException("reply lost", new IOException(), status),
            "acquire"
        );
        Assert.Equal(expected, failure.Kind);
    }

    [Theory]
    [InlineData("0 0 */2 * MON", "1970-01-05T00:00:00Z")]
    [InlineData("0 0 15 * */2", "1970-01-15T00:00:00Z")]
    public void Cron_wildcard_steps_keep_both_day_masks(string expression, string expected)
    {
        Assert.Equal(
            DateTimeOffset.Parse(expected, null),
            CronExpression.Parse(expression).GetNextOccurrence(DateTimeOffset.UnixEpoch)
        );
    }

    [Fact]
    public async Task Transport_disposal_ends_a_publication_waiting_on_an_uncooperative_handler()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var broker = new InMemoryRequestBroker(null, clock);
        var started = Signal();
        var release = Signal();
        await using var subscription = await broker.SubscribeAsync(
            "catalog",
            "audit",
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task.WaitAsync(Bound, token);
            },
            token
        );
        var publication = broker.PublishAsync("catalog", new byte[] { 1 }, token).AsTask();
        await started.Task.WaitAsync(Bound, token);
        var stopping = broker.DisposeAsync().AsTask();
        try
        {
            await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
                await publication.WaitAsync(Bound, token)
            );
        }
        finally
        {
            release.TrySetResult();
            await stopping.WaitAsync(Bound, token);
        }
    }

    [Fact]
    public async Task Early_stream_disposal_cancels_the_handler_before_disposing_its_iterator()
    {
        var token = TestContext.Current.CancellationToken;
        await using var broker = new InMemoryRequestBroker();
        var cancelled = false;
        async IAsyncEnumerable<object?> Read(
            [EnumeratorCancellation] CancellationToken cancellation
        )
        {
            try
            {
                yield return "books";
                await Task.Yield();
            }
            finally
            {
                cancelled = cancellation.IsCancellationRequested;
            }
        }
        await using var subscription = await broker.ListenAsync(
            "catalog",
            (_, cancellation) => Read(cancellation),
            token
        );
        var enumerator = broker
            .StreamAsync("catalog", new object(), Expiration, token)
            .GetAsyncEnumerator(token);
        Assert.True(await enumerator.MoveNextAsync());
        await enumerator.DisposeAsync();
        Assert.True(cancelled);
    }

    private static TaskCompletionSource Signal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);

    private sealed class BrokenSerializer : ICacheValueSerializer
    {
        public void Serialize<T>(IBufferWriter<byte> writer, T value) =>
            throw new InvalidOperationException("encoding failed");

        public T? Deserialize<T>(ReadOnlySpan<byte> bytes) => throw new NotSupportedException();
    }

    private sealed class DelayedTimerClock : TimeProvider
    {
        private readonly TestClock _timers = new();
        public TimeSpan Elapsed { get; set; }
        public DateTimeOffset WallTime { get; set; } = DateTimeOffset.UnixEpoch;

        public void FireTimers(TimeSpan delta) => _timers.Advance(delta);

        public void AdvanceTimers(TimeSpan delta)
        {
            Elapsed += delta;
            _timers.Advance(delta);
        }

        public int TimersDueAt(DateTimeOffset due) => _timers.PendingTimersDueAt(due);

        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => Elapsed.Ticks;

        public override DateTimeOffset GetUtcNow() => WallTime;

        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period
        ) => _timers.CreateTimer(callback, state, dueTime, period);
    }
}
