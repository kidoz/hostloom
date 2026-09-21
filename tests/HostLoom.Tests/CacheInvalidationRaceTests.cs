using System.Diagnostics.Metrics;
using HostLoom.Caching;
using HostLoom.Caching.Internal;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class CacheInvalidationRaceTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);
    private static readonly TimeSpan Expiration = TimeSpan.FromMinutes(2);
    private static readonly ICacheValueSerializer Serializer =
        SystemTextJsonCacheValueSerializer.CreateReflectionBased();

    public static TheoryData<string, string> Overlaps { get; } =
        new(
            from operation in new[]
            {
                "read",
                "null_read",
                "bulk_read",
                "set",
                "conditional_set",
                "warmup",
                "factory",
                "null_factory",
                "factory_write",
                "null_write",
            }
            from invalidation in new[]
            {
                "key",
                "tag",
                "flush",
                "remove",
                "remove_many",
                "remove_tag",
            }
            select (operation, invalidation)
        );

    [Theory(Timeout = 30_000)]
    [MemberData(nameof(Overlaps))]
    public async Task An_invalidated_operation_does_not_repopulate_the_local_tier(
        string operation,
        string invalidation
    )
    {
        var options = new CachingOptions { Namespace = "race-" + Guid.NewGuid().ToString("N") };
        var entryOptions = new CacheEntryOptions(Expiration)
        {
            Tags = ["catalog"],
            NullExpiration = Expiration,
        };
        var store = Substitute.For<IDistributedCacheStore>();
        var channel = Substitute.For<ICacheInvalidationChannel>();
        Action<CacheInvalidation>? invalidate = null;
        channel
            .Subscribe(Arg.Any<Action<CacheInvalidation>>())
            .Returns(call =>
            {
                invalidate = call.Arg<Action<CacheInvalidation>>();
                return Substitute.For<IDisposable>();
            });
        var entered = Signal();
        var release = Signal();
        using var payload = new PooledBufferWriter();
        if (operation == "null_read")
        {
            CachePayloadCodec.EncodeNull(["catalog"], payload);
        }
        else
        {
            CachePayloadCodec.Encode(Serializer, "old", ["catalog"], int.MaxValue, payload, out _);
        }
        var snapshot = new CacheStoreEntry(payload.WrittenMemory.ToArray(), Expiration);
        var dataKey = options.Namespace + ":cache:data:catalog:eu";
        if (operation is "read" or "null_read")
        {
            store
                .GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
                .Returns(
                    async ValueTask<CacheStoreEntry?> (_) =>
                    {
                        entered.SetResult();
                        await release.Task;
                        return snapshot;
                    }
                );
        }
        if (operation == "bulk_read")
        {
            store
                .GetManyAsync(Arg.Any<IReadOnlyCollection<string>>(), Arg.Any<CancellationToken>())
                .Returns(
                    async ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> (_) =>
                    {
                        entered.SetResult();
                        await release.Task;
                        return new Dictionary<string, CacheStoreEntry> { [dataKey] = snapshot };
                    }
                );
        }
        if (operation is "set" or "factory_write" or "null_write")
        {
            store
                .SetAsync(
                    Arg.Any<string>(),
                    Arg.Any<ReadOnlyMemory<byte>>(),
                    Arg.Any<TimeSpan>(),
                    Arg.Any<IReadOnlyCollection<string>?>(),
                    Arg.Any<CancellationToken>()
                )
                .Returns(
                    async ValueTask (_) =>
                    {
                        entered.SetResult();
                        await release.Task;
                    }
                );
        }
        if (operation == "conditional_set")
        {
            store
                .SetIfAbsentAsync(
                    Arg.Any<string>(),
                    Arg.Any<ReadOnlyMemory<byte>>(),
                    Arg.Any<TimeSpan>(),
                    Arg.Any<IReadOnlyCollection<string>?>(),
                    Arg.Any<CancellationToken>()
                )
                .Returns(
                    async ValueTask<bool> (_) =>
                    {
                        entered.SetResult();
                        await release.Task;
                        return true;
                    }
                );
        }
        if (operation == "warmup")
        {
            store
                .SetManyAsync(
                    Arg.Any<IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>>>(),
                    Arg.Any<TimeSpan>(),
                    Arg.Any<CancellationToken>()
                )
                .Returns(
                    async ValueTask (_) =>
                    {
                        entered.SetResult();
                        await release.Task;
                    }
                );
        }
        await using var cache = new TieredCache(options, store, Serializer, channel);
        async ValueTask<string?> Factory(CancellationToken _)
        {
            if (operation is "factory" or "null_factory")
            {
                entered.SetResult();
                await release.Task;
            }
            return operation is "null_factory" or "null_write" ? null : "old";
        }
        var pending = operation switch
        {
            "read" or "null_read" => (Task)
                cache
                    .TryGetAsync<string>("catalog:eu", TestContext.Current.CancellationToken)
                    .AsTask(),
            "bulk_read" => cache
                .GetManyAsync<string>(["catalog:eu"], TestContext.Current.CancellationToken)
                .AsTask(),
            "set" => cache
                .SetAsync("catalog:eu", "old", entryOptions, TestContext.Current.CancellationToken)
                .AsTask(),
            "conditional_set" => cache
                .SetIfAbsentAsync(
                    "catalog:eu",
                    "old",
                    entryOptions,
                    TestContext.Current.CancellationToken
                )
                .AsTask(),
            "warmup" => cache
                .WarmupAsync(
                    new Dictionary<string, string> { ["catalog:eu"] = "old" },
                    Expiration,
                    cancellationToken: TestContext.Current.CancellationToken
                )
                .AsTask(),
            _ => cache
                .GetOrCreateAsync(
                    "catalog:eu",
                    Factory,
                    entryOptions,
                    TestContext.Current.CancellationToken
                )
                .AsTask(),
        };
        try
        {
            await entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            if (invalidation == "remove")
            {
                await cache.RemoveAsync("catalog:eu", TestContext.Current.CancellationToken);
            }
            else if (invalidation == "remove_many")
            {
                await cache.RemoveAsync(["catalog:eu"], TestContext.Current.CancellationToken);
            }
            else if (invalidation == "remove_tag")
            {
                await cache.RemoveByTagAsync("catalog", TestContext.Current.CancellationToken);
            }
            else
            {
                var applied = Signal();
                using var listener = new MeterListener();
                listener.InstrumentPublished = (instrument, l) =>
                {
                    if (instrument.Name == "hostloom.cache.invalidations")
                    {
                        l.EnableMeasurementEvents(instrument);
                    }
                };
                listener.SetMeasurementEventCallback<long>(
                    (_, _, tags, _) =>
                    {
                        foreach (var tag in tags)
                        {
                            if (Equals(tag.Value, options.Namespace))
                            {
                                applied.TrySetResult();
                            }
                        }
                    }
                );
                listener.Start();
                invalidate!(
                    invalidation switch
                    {
                        "key" => new CacheInvalidation(["catalog:eu"], []),
                        "tag" => new CacheInvalidation([], ["catalog"]),
                        _ => CacheInvalidation.Flush,
                    }
                );
                await applied.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            }
        }
        finally
        {
            release.TrySetResult();
        }
        await pending.WaitAsync(Bound, TestContext.Current.CancellationToken);
        Assert.Equal(0, cache.LocalEntryCount);
        if (operation is "factory" or "null_factory")
        {
            await store
                .DidNotReceive()
                .SetAsync(
                    Arg.Any<string>(),
                    Arg.Any<ReadOnlyMemory<byte>>(),
                    Arg.Any<TimeSpan>(),
                    Arg.Any<IReadOnlyCollection<string>?>(),
                    Arg.Any<CancellationToken>()
                );
        }
        // A new operation after the invalidation can populate L1 again.
        store
            .SetAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<IReadOnlyCollection<string>?>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(ValueTask.CompletedTask);
        await cache.SetAsync(
            "catalog:eu",
            "new",
            entryOptions,
            TestContext.Current.CancellationToken
        );
        Assert.Equal(
            "new",
            (
                await cache.TryGetAsync<string>("catalog:eu", TestContext.Current.CancellationToken)
            ).Value
        );
        Assert.Equal(1, cache.LocalEntryCount);
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_delayed_same_key_notification_requires_a_fresh_fill_even_when_L2_returned_the_latest_value(
        bool afterFirstRead
    )
    {
        var token = TestContext.Current.CancellationToken;
        var options = new CachingOptions { Namespace = "ordering-" + Guid.NewGuid().ToString("N") };
        var store = Substitute.For<IDistributedCacheStore>();
        var channel = Substitute.For<ICacheInvalidationChannel>();
        Action<CacheInvalidation>? invalidate = null;
        channel
            .Subscribe(Arg.Any<Action<CacheInvalidation>>())
            .Returns(call =>
            {
                invalidate = call.Arg<Action<CacheInvalidation>>();
                return Substitute.For<IDisposable>();
            });
        using var payload = new PooledBufferWriter();
        CachePayloadCodec.Encode(Serializer, "v1", null, int.MaxValue, payload, out _);
        var snapshot = new CacheStoreEntry(payload.WrittenMemory.ToArray(), Expiration);
        var read = new TaskCompletionSource<CacheStoreEntry?>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        store
            .GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<CacheStoreEntry?>(read.Task));
        var applied = Signal();
        using var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == "hostloom.cache.invalidations")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            (_, _, tags, _) =>
            {
                var matchingNamespace = false;
                var received = false;
                foreach (var tag in tags)
                {
                    matchingNamespace |=
                        tag.Key == "hostloom.cache.namespace"
                        && Equals(tag.Value, options.Namespace);
                    received |=
                        tag.Key == "hostloom.cache.direction" && Equals(tag.Value, "received");
                }
                if (matchingNamespace && received)
                {
                    applied.TrySetResult();
                }
            }
        );
        listener.Start();
        await using var cache = new TieredCache(options, store, Serializer, channel);

        // L2 already contains v1; its write's notification travels separately from the read.
        var firstRead = cache.TryGetAsync<string>("price", token).AsTask();
        if (afterFirstRead)
        {
            read.SetResult(snapshot);
            await firstRead.WaitAsync(Bound, token);
            Assert.Equal(1, cache.LocalEntryCount);
        }
        try
        {
            invalidate!(new CacheInvalidation(["price"], []));
            await applied.Task.WaitAsync(Bound, token);
        }
        finally
        {
            read.TrySetResult(snapshot);
        }
        var first = await firstRead.WaitAsync(Bound, token);
        Assert.Equal(CacheTier.L2, first.Tier);
        Assert.Equal("v1", first.Value);
        Assert.Equal(0, cache.LocalEntryCount);

        // Once that notification is applied, a fresh read fills L1 and the next read hits it.
        var refill = await cache.TryGetAsync<string>("price", token);
        Assert.Equal(CacheTier.L2, refill.Tier);
        Assert.Equal("v1", refill.Value);
        var local = await cache.TryGetAsync<string>("price", token);
        Assert.Equal(CacheTier.L1, local.Tier);
        Assert.Equal("v1", local.Value);
        await store.Received(2).GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>());
    }

    [Fact(Timeout = 30_000)]
    public async Task An_invalidation_of_another_key_does_not_suppress_a_fill()
    {
        var token = TestContext.Current.CancellationToken;
        var options = new CachingOptions { Namespace = "stripes-" + Guid.NewGuid().ToString("N") };
        var store = Substitute.For<IDistributedCacheStore>();
        var channel = Substitute.For<ICacheInvalidationChannel>();
        Action<CacheInvalidation>? invalidate = null;
        channel
            .Subscribe(Arg.Any<Action<CacheInvalidation>>())
            .Returns(call =>
            {
                invalidate = call.Arg<Action<CacheInvalidation>>();
                return Substitute.For<IDisposable>();
            });
        store
            .SetAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<IReadOnlyCollection<string>?>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(ValueTask.CompletedTask);
        var entered = Signal();
        var release = Signal();
        using var listener = ReceivedListener(options.Namespace, out var applied);
        await using var cache = new TieredCache(options, store, Serializer, channel);

        // Pick a second key from another stripe: a shared stripe is the documented exception.
        var other = Enumerable
            .Range(0, 100_000)
            .Select(i => "other:" + i)
            .First(candidate =>
                TieredCache.StripeOf(candidate) != TieredCache.StripeOf("catalog:eu")
            );
        var pending = cache
            .GetOrCreateAsync(
                "catalog:eu",
                async _ =>
                {
                    entered.SetResult();
                    await release.Task;
                    return "fresh";
                },
                new CacheEntryOptions(Expiration),
                token
            )
            .AsTask();
        await entered.Task.WaitAsync(Bound, token);
        invalidate!(new CacheInvalidation([other], []));
        await applied.Task.WaitAsync(Bound, token);
        release.SetResult();

        Assert.Equal("fresh", await pending.WaitAsync(Bound, token));
        // The unrelated invalidation left this fill alone: it reached both tiers.
        Assert.Equal(1, cache.LocalEntryCount);
        Assert.Equal(CacheTier.L1, (await cache.TryGetAsync<string>("catalog:eu", token)).Tier);
        await store
            .Received(1)
            .SetAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<IReadOnlyCollection<string>?>(),
                Arg.Any<CancellationToken>()
            );
    }

    [Fact(Timeout = 30_000)]
    public async Task The_echo_of_an_own_removal_does_not_suppress_the_refill()
    {
        var token = TestContext.Current.CancellationToken;
        var options = new CachingOptions { Namespace = "echo-" + Guid.NewGuid().ToString("N") };
        var clock = new TestClock();
        // The in-memory store's channel delivers every publish to its publisher too.
        var store = new InMemoryDistributedCacheStore(clock);
        using var listener = EchoListener(options.Namespace, out var echoed);
        await using var cache = new TieredCache(options, store, Serializer, timeProvider: clock);
        var entryOptions = new CacheEntryOptions(Expiration);
        await cache.SetAsync("catalog:eu", "old", entryOptions, token);

        await cache.RemoveAsync("catalog:eu", token);
        var refill = await cache.GetOrCreateAsync(
            "catalog:eu",
            async _ =>
            {
                // The echo lands while the factory runs.
                await echoed.Task.WaitAsync(Bound, token);
                return "new";
            },
            entryOptions,
            token
        );

        Assert.Equal("new", refill);
        Assert.Equal(1, cache.LocalEntryCount);
        var local = await cache.TryGetAsync<string>("catalog:eu", token);
        Assert.Equal(CacheTier.L1, local.Tier);
        Assert.Equal("new", local.Value);
        Assert.NotNull(await store.GetAsync(options.Namespace + ":cache:data:catalog:eu", token));
    }

    [Fact(Timeout = 30_000)]
    public async Task A_delayed_distributed_read_does_not_replace_a_newer_local_value()
    {
        var token = TestContext.Current.CancellationToken;
        var options = new CachingOptions { Namespace = "late-" + Guid.NewGuid().ToString("N") };
        var clock = new TestClock();
        var store = Substitute.For<IDistributedCacheStore>();
        using var payload = new PooledBufferWriter();
        CachePayloadCodec.Encode(Serializer, "v1", null, int.MaxValue, payload, out _);
        var snapshot = new CacheStoreEntry(payload.WrittenMemory.ToArray(), Expiration);
        var read = new TaskCompletionSource<CacheStoreEntry?>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        store
            .GetAsync(Arg.Any<string>(), Arg.Any<CancellationToken>())
            .Returns(_ => new ValueTask<CacheStoreEntry?>(read.Task));
        store
            .SetAsync(
                Arg.Any<string>(),
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<TimeSpan>(),
                Arg.Any<IReadOnlyCollection<string>?>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(ValueTask.CompletedTask);
        await using var cache = new TieredCache(options, store, Serializer, timeProvider: clock);

        // The read of v1 is in flight when v2 is written to both tiers.
        var slow = cache.TryGetAsync<string>("price", token).AsTask();
        clock.Advance(TimeSpan.FromMilliseconds(1));
        await cache.SetAsync("price", "v2", new CacheEntryOptions(Expiration), token);
        read.SetResult(snapshot);
        var late = await slow.WaitAsync(Bound, token);

        // The caller gets what the distributed tier returned; the newer local entry stays.
        Assert.Equal("v1", late.Value);
        Assert.Equal(CacheTier.L2, late.Tier);
        var local = await cache.TryGetAsync<string>("price", token);
        Assert.Equal(CacheTier.L1, local.Tier);
        Assert.Equal("v2", local.Value);
    }

    private static MeterListener ReceivedListener(
        string @namespace,
        out TaskCompletionSource applied
    ) => DirectionListener(@namespace, "received", out applied);

    private static MeterListener EchoListener(string @namespace, out TaskCompletionSource echoed) =>
        DirectionListener(@namespace, "echoed", out echoed);

    private static MeterListener DirectionListener(
        string @namespace,
        string direction,
        out TaskCompletionSource signal
    )
    {
        var reached = Signal();
        signal = reached;
        var listener = new MeterListener();
        listener.InstrumentPublished = (instrument, l) =>
        {
            if (instrument.Name == "hostloom.cache.invalidations")
            {
                l.EnableMeasurementEvents(instrument);
            }
        };
        listener.SetMeasurementEventCallback<long>(
            (_, _, tags, _) =>
            {
                var matchingNamespace = false;
                var matchingDirection = false;
                foreach (var tag in tags)
                {
                    matchingNamespace |=
                        tag.Key == "hostloom.cache.namespace" && Equals(tag.Value, @namespace);
                    matchingDirection |=
                        tag.Key == "hostloom.cache.direction" && Equals(tag.Value, direction);
                }
                if (matchingNamespace && matchingDirection)
                {
                    reached.TrySetResult();
                }
            }
        );
        listener.Start();
        return listener;
    }

    private static TaskCompletionSource Signal() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
