using System.Buffers;
using HostLoom.Caching;
using HostLoom.Locking;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class CacheReviewRegressionTests
{
    [Theory]
    [InlineData("read", 6)]
    [InlineData("bulk", 6)]
    [InlineData("null", 6)]
    [InlineData("decode", 6)]
    [InlineData("read", 12)]
    [InlineData("null", 12)]
    public async Task Promotion_never_extends_the_TTL_by_read_or_decode_latency(
        string path,
        int seconds
    )
    {
        var clock = new TestClock();
        var real = new InMemoryDistributedCacheStore(clock);
        var inner = SystemTextJsonCacheValueSerializer.CreateReflectionBased();
        var serializer = new DelayedSerializer(
            inner,
            () =>
            {
                if (path == "decode")
                    clock.Advance(TimeSpan.FromSeconds(seconds));
            }
        );
        await using var writer = new TieredCache(Options(), real, inner, timeProvider: clock);
        var options = new CacheEntryOptions(TimeSpan.FromSeconds(10))
        {
            NullExpiration = TimeSpan.FromSeconds(10),
        };
        if (path == "null")
            await writer.GetOrCreateAsync<CachingTestPayload?>(
                "catalog",
                _ => ValueTask.FromResult<CachingTestPayload?>(null),
                options,
                TestContext.Current.CancellationToken
            );
        else
            await writer.SetAsync(
                "catalog",
                new CachingTestPayload("books", 1),
                options,
                TestContext.Current.CancellationToken
            );
        var stored = (
            await real.GetAsync("catalog:cache:data:catalog", TestContext.Current.CancellationToken)
        )!.Value;
        var store = Substitute.For<IDistributedCacheStore>();
        var reads = 0;
        CacheStoreEntry Read()
        {
            reads++;
            if (path != "decode")
                clock.Advance(TimeSpan.FromSeconds(seconds));
            return stored;
        }
        store
            .GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => ValueTask.FromResult<CacheStoreEntry?>(Read()));
        store
            .GetManyAsync(Arg.Any<IReadOnlyCollection<string>>(), Arg.Any<CancellationToken>())
            .Returns(_ =>
                ValueTask.FromResult<IReadOnlyDictionary<string, CacheStoreEntry>>(
                    new Dictionary<string, CacheStoreEntry>
                    {
                        ["catalog:cache:data:catalog"] = Read(),
                    }
                )
            );
        await using var reader = new TieredCache(Options(), store, serializer, timeProvider: clock);
        if (path == "bulk")
            Assert.Single(
                await reader.GetManyAsync<CachingTestPayload>(
                    ["catalog"],
                    TestContext.Current.CancellationToken
                )
            );
        else
            Assert.True(
                (
                    await reader.TryGetAsync<CachingTestPayload>(
                        "catalog",
                        TestContext.Current.CancellationToken
                    )
                ).Found
            );
        if (seconds < 10)
        {
            clock.Advance(TimeSpan.FromSeconds(3));
            Assert.Equal(
                CacheTier.L1,
                (
                    await reader.TryGetAsync<CachingTestPayload>(
                        "catalog",
                        TestContext.Current.CancellationToken
                    )
                ).Tier
            );
            clock.Advance(TimeSpan.FromSeconds(1));
        }
        var before = reads;
        var fresh = await reader.TryGetAsync<CachingTestPayload>(
            "catalog",
            TestContext.Current.CancellationToken
        );
        Assert.Equal(CacheTier.L2, fresh.Tier);
        Assert.Equal(before + 1, reads);
    }

    [Theory]
    [InlineData("set")]
    [InlineData("conditional")]
    [InlineData("factory")]
    public async Task Tags_cannot_be_written_when_the_same_tag_cannot_be_invalidated(
        string operation
    )
    {
        var options = Options();
        options.MaxKeyLength = 8;
        await using var cache = new TieredCache(options);
        var calls = 0;
        var entry = new CacheEntryOptions(TimeSpan.FromMinutes(1)) { Tags = [new string('x', 9)] };
        await Assert.ThrowsAsync<ArgumentException>(async () =>
        {
            if (operation == "set")
                await cache.SetAsync("catalog", 1, entry, TestContext.Current.CancellationToken);
            else if (operation == "conditional")
                await cache.SetIfAbsentAsync(
                    "catalog",
                    1,
                    entry,
                    TestContext.Current.CancellationToken
                );
            else
                await cache.GetOrCreateAsync(
                    "catalog",
                    _ =>
                    {
                        calls++;
                        return ValueTask.FromResult(1);
                    },
                    entry,
                    TestContext.Current.CancellationToken
                );
        });
        Assert.Equal(0, calls);
        Assert.False(
            (await cache.TryGetAsync<int>("catalog", TestContext.Current.CancellationToken)).Found
        );
    }

    [Fact]
    public async Task In_memory_backends_reclaim_historical_keys_and_tags_while_retaining_live_owners()
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        var locks = new InMemoryLockProvider(clock);
        for (var index = 0; index < 200; index++)
        {
            await store.SetAsync(
                "catalog:" + index,
                new byte[1],
                TimeSpan.FromSeconds(1),
                ["tag:" + index],
                TestContext.Current.CancellationToken
            );
            Assert.True(
                await locks.TryAcquireAsync(
                    "catalog:" + index,
                    "owner",
                    TimeSpan.FromSeconds(1),
                    TestContext.Current.CancellationToken
                )
            );
        }
        await store.SetAsync(
            "live",
            new byte[] { 7 },
            TimeSpan.FromMinutes(1),
            ["live"],
            TestContext.Current.CancellationToken
        );
        Assert.True(
            await locks.TryAcquireAsync(
                "live",
                "owner",
                TimeSpan.FromMinutes(1),
                TestContext.Current.CancellationToken
            )
        );
        clock.Advance(TimeSpan.FromSeconds(2));
        await store.SetAsync(
            "new",
            new byte[1],
            TimeSpan.FromMinutes(1),
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.True(
            await locks.TryAcquireAsync(
                "new",
                "owner",
                TimeSpan.FromMinutes(1),
                TestContext.Current.CancellationToken
            )
        );
        Assert.Equal(2, store.Count);
        Assert.Equal(1, store.TagCount);
        Assert.Equal(2, locks.Count);
        Assert.False(
            await locks.TryAcquireAsync(
                "live",
                "other",
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken
            )
        );
        await store.RemoveByTagAsync("live", TestContext.Current.CancellationToken);
        Assert.Null(await store.GetAsync("live", TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task Live_tag_memberships_remain_monotonic_across_rewrites_and_sweeps()
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        await store.SetAsync(
            "catalog",
            new byte[] { 1 },
            TimeSpan.FromMinutes(1),
            ["books"],
            TestContext.Current.CancellationToken
        );
        clock.Advance(TimeSpan.FromSeconds(2));
        await store.SetAsync(
            "catalog",
            new byte[] { 2 },
            TimeSpan.FromMinutes(1),
            ["music"],
            TestContext.Current.CancellationToken
        );
        await store.RemoveByTagAsync("books", TestContext.Current.CancellationToken);
        Assert.Null(await store.GetAsync("catalog", TestContext.Current.CancellationToken));
    }

    private static CachingOptions Options() =>
        new() { Namespace = "catalog", L1 = { ExpirationJitter = TimeSpan.Zero } };

    private sealed class DelayedSerializer(ICacheValueSerializer inner, Action delay)
        : ICacheValueSerializer
    {
        public void Serialize<T>(IBufferWriter<byte> destination, T value) =>
            inner.Serialize(destination, value);

        public T? Deserialize<T>(ReadOnlySpan<byte> payload)
        {
            delay();
            return inner.Deserialize<T>(payload);
        }
    }
}
