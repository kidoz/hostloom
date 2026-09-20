using System.Diagnostics;
using HostLoom.Caching;
using HostLoom.Caching.Testing;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Controlled faults against the distributed tier and the invalidation channel: a store that
/// answers nothing while a crowd of callers arrives, a factory that throws with callers queued
/// behind it, a channel that cannot publish, and disposal during an in-flight factory. The
/// hypothesis under every fault is the same: the cache keeps serving, one factory runs per key,
/// callers are never stranded, staleness is bounded by the in-process expiry, and an outage
/// surfaces as a degraded metric and one warning per key rather than as a flood or a hang.
/// </summary>
public sealed class CachingChaosTests
{
    private static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);
    private static readonly TimeSpan Expiration = TimeSpan.FromMinutes(10);

    private readonly TestClock _clock = new();
    private readonly ICacheValueSerializer _serializer =
        SystemTextJsonCacheValueSerializer.CreateReflectionBased();

    [Fact]
    public async Task A_store_outage_still_runs_one_factory_per_key_for_a_crowd_of_callers()
    {
        const int callers = 24;
        string[] keys = ["catalog:eu", "catalog:apac", "catalog:us"];
        var token = TestContext.Current.CancellationToken;
        var logger = new RecordingLogger<TieredCache>();
        var faulting = new FaultingCacheStore(new InMemoryDistributedCacheStore(_clock));
        await using var cache = new TieredCache(
            Options(),
            faulting,
            _serializer,
            timeProvider: _clock,
            logger: logger
        );
        var factories = new int[keys.Length];
        faulting.FailAll(CacheFailureKind.Unavailable);

        var calls = Enumerable
            .Range(0, callers)
            .Select(index =>
                Task.Run(
                    async () =>
                        await cache.GetOrCreateAsync(
                            keys[index % keys.Length],
                            _ =>
                            {
                                Interlocked.Increment(ref factories[index % keys.Length]);
                                return ValueTask.FromResult(
                                    new Catalog(keys[index % keys.Length], index % keys.Length)
                                );
                            },
                            new CacheEntryOptions(Expiration),
                            token
                        ),
                    token
                )
            )
            .ToArray();
        var values = await Task.WhenAll(calls).WaitAsync(Bounded, token);

        // Every caller is served without an exception, and the outage costs one factory run per
        // key rather than one per caller: the in-process guard still single-flights when the
        // cluster-wide lease cannot be taken.
        Assert.All(values, value => Assert.NotNull(value));
        Assert.Equal([1, 1, 1], factories);
        Assert.True(faulting.Faulted >= keys.Length, "the outage was not exercised");

        // The warnings are compressed well below the failing calls, but the throttle reads and
        // writes its last-logged timestamp in two steps, so a burst of callers arriving together
        // can pass the check together: the guarantee under concurrency is compression, not one
        // line per key. The steady state below is where the per-key interval is exact.
        var burst = logger.Entries.Count(entry =>
            entry.Event.Id == 1001 && entry.Level == LogLevel.Warning
        );
        Assert.InRange(burst, keys.Length, faulting.Faulted - 1);

        // A write for a key already warned about: it reaches the failing store and stays silent.
        await cache.SetAsync(
            "catalog:eu",
            new Catalog("catalog:eu", 9),
            new CacheEntryOptions(Expiration),
            token
        );
        Assert.Equal(burst, logger.Entries.Count(entry => entry.Event.Id == 1001));

        // Recovery: the store answers again and the next distinct key reaches it.
        faulting.Heal();
        await cache.SetAsync(
            "catalog:emea",
            new Catalog("catalog:emea", 4),
            new CacheEntryOptions(Expiration),
            token
        );
        await using var elsewhere = new TieredCache(
            Options(),
            faulting.Inner,
            _serializer,
            timeProvider: _clock
        );
        var replicated = await elsewhere.TryGetAsync<Catalog>("catalog:emea", token);
        Assert.True(replicated.Found);

        // The writes made during the outage were never replayed: another instance still misses
        // them, and they live only in the in-process tier that produced them.
        var lost = await elsewhere.TryGetAsync<Catalog>("catalog:eu", token);
        Assert.False(lost.Found);
    }

    [Fact]
    public async Task A_factory_that_throws_under_an_outage_does_not_poison_the_callers_behind_it()
    {
        var token = TestContext.Current.CancellationToken;
        var faulting = new FaultingCacheStore(new InMemoryDistributedCacheStore(_clock));
        await using var cache = new TieredCache(
            Options(),
            faulting,
            _serializer,
            timeProvider: _clock
        );
        faulting.FailAll(CacheFailureKind.Timeout);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var first = cache
            .GetOrCreateAsync<Catalog>(
                "catalog:eu",
                async _ =>
                {
                    entered.SetResult();
                    await release.Task;
                    throw new InvalidOperationException("the catalog service refused the read");
                },
                new CacheEntryOptions(Expiration),
                token
            )
            .AsTask();
        await entered.Task.WaitAsync(Bounded, token);

        // The second caller is queued on the guard the failing factory holds.
        var second = cache
            .GetOrCreateAsync(
                "catalog:eu",
                _ => ValueTask.FromResult(new Catalog("catalog:eu", 7)),
                new CacheEntryOptions(Expiration),
                token
            )
            .AsTask();
        release.SetResult();

        // The failure belongs to the caller that asked for it; the one behind runs its own
        // factory instead of inheriting an exception it never caused.
        await Assert.ThrowsAsync<InvalidOperationException>(() => first.WaitAsync(Bounded, token));
        var value = await second.WaitAsync(Bounded, token);
        Assert.Equal(7, value!.Items);
    }

    [Fact]
    public async Task An_outage_only_throws_where_set_if_absent_promised_it_would()
    {
        var token = TestContext.Current.CancellationToken;
        var faulting = new FaultingCacheStore(new InMemoryDistributedCacheStore(_clock));
        await using var cache = new TieredCache(
            Options(),
            faulting,
            _serializer,
            timeProvider: _clock
        );
        var catalog = new Catalog("catalog:eu", 3);
        faulting.FailAll(CacheFailureKind.Unavailable);

        // The one promised surfaced error.
        var unavailable = await Assert.ThrowsAsync<CacheUnavailableException>(async () =>
            await cache.SetIfAbsentAsync(
                "catalog:eu",
                catalog,
                new CacheEntryOptions(Expiration) { OnUnavailable = UnavailableBehavior.Throw },
                token
            )
        );
        Assert.Equal(CacheFailureKind.Unavailable, unavailable.Kind);

        // Everything else fails open: the same outage, and not one of these throws.
        Assert.False(
            await cache.SetIfAbsentAsync(
                "catalog:eu",
                catalog,
                new CacheEntryOptions(Expiration),
                token
            )
        );
        Assert.Equal(
            catalog,
            await cache.GetOrCreateAsync(
                "catalog:apac",
                _ => ValueTask.FromResult(catalog),
                new CacheEntryOptions(Expiration),
                token
            )
        );
        Assert.False((await cache.TryGetAsync<Catalog>("catalog:us", token)).Found);
        Assert.Empty(await cache.GetManyAsync<Catalog>(["catalog:us"], token));
        await cache.SetAsync("catalog:us", catalog, new CacheEntryOptions(Expiration), token);
        await cache.RemoveAsync("catalog:us", token);
        await cache.RemoveByTagAsync("catalog", token);
    }

    [Fact]
    public async Task An_invalidation_that_cannot_be_published_leaves_the_other_instance_stale_until_its_local_expiry()
    {
        var token = TestContext.Current.CancellationToken;
        var local = TimeSpan.FromSeconds(30);
        var options = new CacheEntryOptions(Expiration) { LocalExpiration = local };
        var store = new InMemoryDistributedCacheStore(_clock);
        var channel = new FlakyChannel(store);
        var logger = new RecordingLogger<TieredCache>();
        await using var writer = new TieredCache(
            Options(),
            store,
            _serializer,
            channel,
            _clock,
            logger
        );
        await using var reader = new TieredCache(Options(), store, _serializer, store, _clock);

        await writer.SetAsync("catalog:eu", new Catalog("catalog:eu", 1), options, token);
        var seen = await reader.GetOrCreateAsync<Catalog>(
            "catalog:eu",
            _ => throw new UnreachableException(),
            options,
            token
        );
        Assert.Equal(1, seen!.Items);

        // Fault: the key is removed everywhere the store knows about, and the message that would
        // have told the other instance never leaves.
        channel.Fail = true;
        await writer.RemoveAsync("catalog:eu", token);
        Assert.False((await reader.TryGetAsync<Catalog>("catalog:nothing", token)).Found);
        var warning = Assert.Single(logger.Entries, entry => entry.Event.Id == 1004);
        Assert.Equal(LogLevel.Warning, warning.Level);

        // The reader keeps the copy it was never told about: staleness, and bounded by the
        // in-process expiry rather than by the message that was lost.
        var stale = await reader.GetOrCreateAsync<Catalog>(
            "catalog:eu",
            _ => throw new UnreachableException(),
            options,
            token
        );
        Assert.Equal(1, stale!.Items);

        _clock.Advance(local + TimeSpan.FromSeconds(1));
        var refreshed = await reader.GetOrCreateAsync(
            "catalog:eu",
            _ => ValueTask.FromResult(new Catalog("catalog:eu", 2)),
            options,
            token
        );
        Assert.Equal(2, refreshed!.Items);

        // Recovery: with the channel back, the next removal is applied on the other instance at
        // once rather than waiting out another local expiry.
        channel.Fail = false;
        await writer.RemoveAsync("catalog:eu", token);
        await SchedulingTests.WaitUntilAsync(() => reader.LocalEntryCount == 0);
    }

    [Fact]
    public async Task Disposal_during_a_factory_is_bounded_and_strands_nobody()
    {
        var token = TestContext.Current.CancellationToken;
        var store = new InMemoryDistributedCacheStore(_clock);
        var cache = new TieredCache(Options(), store, _serializer, store, _clock);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var pending = cache
            .GetOrCreateAsync(
                "catalog:eu",
                async _ =>
                {
                    entered.SetResult();
                    await release.Task;
                    return new Catalog("catalog:eu", 1);
                },
                new CacheEntryOptions(Expiration),
                token
            )
            .AsTask();
        await entered.Task.WaitAsync(Bounded, token);

        // Disposal does not wait for work it cannot cancel: the invalidation loop and the timers
        // stop while the factory is still running.
        await cache.DisposeAsync().AsTask().WaitAsync(Bounded, token);
        release.SetResult();

        // The caller that was already inside is answered rather than left holding a guard on a
        // disposed cache; only calls arriving after the disposal are refused.
        var value = await pending.WaitAsync(Bounded, token);
        Assert.Equal(1, value!.Items);
        await Assert.ThrowsAsync<ObjectDisposedException>(async () =>
            await cache.TryGetAsync<Catalog>("catalog:eu", token)
        );
        await cache.DisposeAsync();
    }

    private static CachingOptions Options() => new() { Namespace = "catalog" };

    public sealed record Catalog(string Region, int Items);

    /// <summary>A channel whose publishes can be made to fail while its deliveries keep working.</summary>
    private sealed class FlakyChannel(ICacheInvalidationChannel inner) : ICacheInvalidationChannel
    {
        public bool Fail { get; set; }

        public ValueTask PublishAsync(
            CacheInvalidation invalidation,
            CancellationToken cancellationToken = default
        ) =>
            Fail
                ? ValueTask.FromException(
                    new TimeoutException("the invalidation channel is unreachable")
                )
                : inner.PublishAsync(invalidation, cancellationToken);

        public IDisposable Subscribe(Action<CacheInvalidation> handler) => inner.Subscribe(handler);

        public override string ToString() => $"{nameof(FlakyChannel)} over {inner}";
    }
}
