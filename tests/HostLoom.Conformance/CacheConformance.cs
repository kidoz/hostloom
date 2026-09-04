using System.Diagnostics;
using HostLoom.Caching;
using HostLoom.Caching.Testing;
using Xunit;

namespace HostLoom.Conformance;

/// <summary>What a cache scenario needs: instances over one shared store, a clock, and a fault injector.</summary>
public sealed class CacheConformanceFixture
{
    /// <summary>Creates a new cache instance over the shared store, like a second service instance.</summary>
    public required Func<ICache> CreateCache { get; init; }

    /// <summary>The clock every instance and the store run on.</summary>
    public required ConformanceClock Clock { get; init; }

    /// <summary>The fault injector in front of the shared store, or null for an in-process-only cache.</summary>
    public FaultingCacheStore? Faults { get; init; }

    /// <summary>Whether the composition has a distributed tier.</summary>
    public bool HasDistributedTier => Faults is not null;
}

/// <summary>
/// Backend-neutral cache scenarios. The unit suite runs them on the in-process backends, with and
/// without a container; the integration suite runs the same methods on Redis.
/// </summary>
public static class CacheConformance
{
    /// <summary>Every scenario by name, so a test project can enumerate them as theory data.</summary>
    public static IReadOnlyDictionary<
        string,
        Func<CacheConformanceFixture, Task>
    > Scenarios { get; } =
        new Dictionary<string, Func<CacheConformanceFixture, Task>>(StringComparer.Ordinal)
        {
            [nameof(SingleFlight_100ConcurrentCallers_RunFactoryOnce)] =
                SingleFlight_100ConcurrentCallers_RunFactoryOnce,
            [nameof(L2Hit_PopulatesL1WithRemainingTimeToLive)] =
                L2Hit_PopulatesL1WithRemainingTimeToLive,
            [nameof(Remove_InvalidatesL1OnEveryInstance)] = Remove_InvalidatesL1OnEveryInstance,
            [nameof(RemoveByTag_EvictsTaggedEntriesEverywhere)] =
                RemoveByTag_EvictsTaggedEntriesEverywhere,
            [nameof(RemoveByTag_EvictsAnEntryWrittenBySetIfAbsent)] =
                RemoveByTag_EvictsAnEntryWrittenBySetIfAbsent,
            [nameof(StoreUnavailable_GetOrCreateServesFactoryAndKeepsL1)] =
                StoreUnavailable_GetOrCreateServesFactoryAndKeepsL1,
            [nameof(StoreUnavailable_SetIfAbsentObeysOnUnavailable)] =
                StoreUnavailable_SetIfAbsentObeysOnUnavailable,
            [nameof(StoreTimeout_ReadsAndWritesNeverThrow)] = StoreTimeout_ReadsAndWritesNeverThrow,
            [nameof(FactoryException_PropagatesAndStoresNothing)] =
                FactoryException_PropagatesAndStoresNothing,
            [nameof(NullOrNonPositiveExpiration_IsNotStored)] =
                NullOrNonPositiveExpiration_IsNotStored,
            [nameof(ValueTypes_CachedZeroFalseAndDefaultStruct_AreFound)] =
                ValueTypes_CachedZeroFalseAndDefaultStruct_AreFound,
            [nameof(GetMany_ReadsBothTiersAndIsPartialUnderFailure)] =
                GetMany_ReadsBothTiersAndIsPartialUnderFailure,
            [nameof(Warmup_FillsBothTiersAndIsFailOpen)] = Warmup_FillsBothTiersAndIsFailOpen,
        };

    public static async Task SingleFlight_100ConcurrentCallers_RunFactoryOnce(
        CacheConformanceFixture fixture
    )
    {
        var cache = fixture.CreateCache();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var runs = 0;
        var callers = Enumerable
            .Range(0, 100)
            .Select(_ =>
                cache
                    .GetOrCreateAsync(
                        "single-flight",
                        async token =>
                        {
                            Interlocked.Increment(ref runs);
                            started.TrySetResult();
                            await gate.Task.WaitAsync(token);
                            return new Payload("computed");
                        },
                        TimeSpan.FromMinutes(5)
                    )
                    .AsTask()
            )
            .ToList();

        await started.Task.WaitAsync(TimeSpan.FromSeconds(10));
        gate.SetResult();
        var results = await Task.WhenAll(callers);

        Assert.Equal(1, runs);
        Assert.All(results, result => Assert.Equal("computed", result!.Text));
    }

    public static async Task L2Hit_PopulatesL1WithRemainingTimeToLive(
        CacheConformanceFixture fixture
    )
    {
        if (!fixture.HasDistributedTier)
        {
            return;
        }

        var writer = fixture.CreateCache();
        await writer.SetAsync(
            "ttl",
            new Payload("v"),
            new CacheEntryOptions(TimeSpan.FromSeconds(12))
        );
        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(4));

        var reader = fixture.CreateCache();
        var fromL2 = await reader.TryGetAsync<Payload>("ttl");
        Assert.True(fromL2.Found);
        Assert.Equal(CacheTier.L2, fromL2.Tier);

        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(6));
        var fromL1 = await reader.TryGetAsync<Payload>("ttl");
        Assert.True(fromL1.Found);
        Assert.Equal(CacheTier.L1, fromL1.Tier);

        await fixture.Clock.AdvanceAsync(TimeSpan.FromSeconds(4));
        var expired = await reader.TryGetAsync<Payload>("ttl");
        Assert.False(expired.Found);
    }

    public static async Task Remove_InvalidatesL1OnEveryInstance(CacheConformanceFixture fixture)
    {
        if (!fixture.HasDistributedTier)
        {
            return;
        }

        var a = fixture.CreateCache();
        var b = fixture.CreateCache();
        await a.SetAsync(
            "shared",
            new Payload("v1"),
            new CacheEntryOptions(TimeSpan.FromMinutes(5))
        );
        var warm = await b.TryGetAsync<Payload>("shared");
        Assert.Equal(CacheTier.L2, warm.Tier);
        Assert.Equal(CacheTier.L1, (await b.TryGetAsync<Payload>("shared")).Tier);

        await a.RemoveAsync("shared");

        await WaitUntilAsync(async () => !(await b.TryGetAsync<Payload>("shared")).Found);
        Assert.False((await a.TryGetAsync<Payload>("shared")).Found);
    }

    public static async Task RemoveByTag_EvictsTaggedEntriesEverywhere(
        CacheConformanceFixture fixture
    )
    {
        var a = fixture.CreateCache();
        var b = fixture.CreateCache();
        var tagged = new CacheEntryOptions(TimeSpan.FromMinutes(5)) { Tags = ["catalog"] };
        await a.SetAsync("tagged-1", new Payload("1"), tagged);
        await a.SetAsync("tagged-2", new Payload("2"), tagged);
        await a.SetAsync(
            "untagged",
            new Payload("3"),
            new CacheEntryOptions(TimeSpan.FromMinutes(5))
        );
        if (fixture.HasDistributedTier)
        {
            Assert.True((await b.TryGetAsync<Payload>("tagged-1")).Found);
        }

        await a.RemoveByTagAsync("catalog");

        Assert.False((await a.TryGetAsync<Payload>("tagged-1")).Found);
        Assert.False((await a.TryGetAsync<Payload>("tagged-2")).Found);
        Assert.True((await a.TryGetAsync<Payload>("untagged")).Found);
        if (fixture.HasDistributedTier)
        {
            await WaitUntilAsync(async () => !(await b.TryGetAsync<Payload>("tagged-1")).Found);
        }
    }

    public static async Task RemoveByTag_EvictsAnEntryWrittenBySetIfAbsent(
        CacheConformanceFixture fixture
    )
    {
        var a = fixture.CreateCache();
        var b = fixture.CreateCache();
        var tagged = new CacheEntryOptions(TimeSpan.FromMinutes(5)) { Tags = ["inventory"] };
        Assert.True(await a.SetIfAbsentAsync("inventory:eu", new Payload("1"), tagged));
        if (fixture.HasDistributedTier)
        {
            Assert.True((await b.TryGetAsync<Payload>("inventory:eu")).Found);
        }

        await a.RemoveByTagAsync("inventory");

        Assert.False((await a.TryGetAsync<Payload>("inventory:eu")).Found);
        if (fixture.HasDistributedTier)
        {
            await WaitUntilAsync(async () => !(await b.TryGetAsync<Payload>("inventory:eu")).Found);
        }

        // The entry left the distributed tier too, so the key is free to claim again.
        Assert.True(await a.SetIfAbsentAsync("inventory:eu", new Payload("2"), tagged));
    }

    public static async Task StoreUnavailable_GetOrCreateServesFactoryAndKeepsL1(
        CacheConformanceFixture fixture
    )
    {
        if (fixture.Faults is null)
        {
            return;
        }

        var cache = fixture.CreateCache();
        fixture.Faults.FailAll(CacheFailureKind.Unavailable);
        var runs = 0;

        var first = await cache.GetOrCreateAsync(
            "degraded",
            _ =>
            {
                runs++;
                return ValueTask.FromResult(new Payload("from-factory"));
            },
            TimeSpan.FromMinutes(5)
        );
        Assert.Equal("from-factory", first!.Text);

        var local = await cache.TryGetAsync<Payload>("degraded");
        Assert.True(local.Found);
        Assert.Equal(CacheTier.L1, local.Tier);

        fixture.Faults.Heal();
        var second = await cache.GetOrCreateAsync(
            "degraded",
            _ =>
            {
                runs++;
                return ValueTask.FromResult(new Payload("again"));
            },
            TimeSpan.FromMinutes(5)
        );
        Assert.Equal("from-factory", second!.Text);
        Assert.Equal(1, runs);
    }

    public static async Task StoreUnavailable_SetIfAbsentObeysOnUnavailable(
        CacheConformanceFixture fixture
    )
    {
        if (fixture.Faults is null)
        {
            return;
        }

        var cache = fixture.CreateCache();
        fixture.Faults.FailAll(CacheFailureKind.Unavailable);

        var denied = await cache.SetIfAbsentAsync(
            "limiter",
            new Payload("x"),
            TimeSpan.FromSeconds(30)
        );
        Assert.False(denied);

        var thrown = await Assert.ThrowsAsync<CacheUnavailableException>(async () =>
            await cache.SetIfAbsentAsync(
                "limiter",
                new Payload("x"),
                new CacheEntryOptions(TimeSpan.FromSeconds(30))
                {
                    OnUnavailable = UnavailableBehavior.Throw,
                }
            )
        );
        Assert.Equal("limiter", thrown.Key);
        Assert.Equal(CacheFailureKind.Unavailable, thrown.Kind);

        fixture.Faults.Heal();
        Assert.True(
            await cache.SetIfAbsentAsync("limiter", new Payload("x"), TimeSpan.FromSeconds(30))
        );
        Assert.False(
            await cache.SetIfAbsentAsync("limiter", new Payload("y"), TimeSpan.FromSeconds(30))
        );
    }

    public static async Task StoreTimeout_ReadsAndWritesNeverThrow(CacheConformanceFixture fixture)
    {
        if (fixture.Faults is null)
        {
            return;
        }

        var cache = fixture.CreateCache();
        fixture.Faults.FailAll(CacheFailureKind.Timeout);

        await cache.SetAsync("t", new Payload("v"), new CacheEntryOptions(TimeSpan.FromMinutes(1)));
        Assert.Equal("v", (await cache.GetAsync<Payload>("t"))!.Text);
        await cache.RemoveAsync("t");
        await cache.RemoveAsync(["t", "u"]);
        await cache.RemoveByTagAsync("tag");
        var many = await cache.GetManyAsync<Payload>(["t", "u"]);
        Assert.Empty(many);
        await cache.WarmupAsync(
            new Dictionary<string, Payload> { ["w"] = new("w") },
            TimeSpan.FromMinutes(1)
        );
        fixture.Faults.Heal();
    }

    public static async Task FactoryException_PropagatesAndStoresNothing(
        CacheConformanceFixture fixture
    )
    {
        var cache = fixture.CreateCache();
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await cache.GetOrCreateAsync<Payload>(
                "boom",
                _ => throw new InvalidOperationException("factory failed"),
                TimeSpan.FromMinutes(5)
            )
        );
        Assert.False((await cache.TryGetAsync<Payload>("boom")).Found);

        var recovered = await cache.GetOrCreateAsync(
            "boom",
            _ => ValueTask.FromResult(new Payload("ok")),
            TimeSpan.FromMinutes(5)
        );
        Assert.Equal("ok", recovered!.Text);
    }

    public static async Task NullOrNonPositiveExpiration_IsNotStored(
        CacheConformanceFixture fixture
    )
    {
        var cache = fixture.CreateCache();
        var runs = 0;
        Payload? Factory()
        {
            runs++;
            return null;
        }

        Assert.Null(
            await cache.GetOrCreateAsync(
                "null",
                _ => ValueTask.FromResult(Factory()),
                TimeSpan.FromMinutes(5)
            )
        );
        Assert.Null(
            await cache.GetOrCreateAsync(
                "null",
                _ => ValueTask.FromResult(Factory()),
                TimeSpan.FromMinutes(5)
            )
        );
        Assert.Equal(2, runs);

        var zeroRuns = 0;
        for (var i = 0; i < 2; i++)
        {
            await cache.GetOrCreateAsync(
                "zero-ttl",
                _ =>
                {
                    zeroRuns++;
                    return ValueTask.FromResult(new Payload("z"));
                },
                TimeSpan.Zero
            );
        }

        Assert.Equal(2, zeroRuns);
        Assert.False((await cache.TryGetAsync<Payload>("zero-ttl")).Found);
    }

    public static async Task ValueTypes_CachedZeroFalseAndDefaultStruct_AreFound(
        CacheConformanceFixture fixture
    )
    {
        var cache = fixture.CreateCache();
        var options = new CacheEntryOptions(TimeSpan.FromMinutes(5));
        await cache.SetAsync("zero", 0, options);
        await cache.SetAsync("false", false, options);
        await cache.SetAsync("struct", default(Point), options);

        foreach (var instance in new[] { cache, fixture.CreateCache() })
        {
            var zero = await instance.TryGetAsync<int>("zero");
            Assert.True(zero.Found);
            Assert.Equal(0, zero.Value);
            var no = await instance.TryGetAsync<bool>("false");
            Assert.True(no.Found);
            Assert.False(no.Value);
            var point = await instance.TryGetAsync<Point>("struct");
            Assert.True(point.Found);
            Assert.Equal(default, point.Value);

            var runs = 0;
            var served = await instance.GetOrCreateAsync(
                "zero",
                _ =>
                {
                    runs++;
                    return ValueTask.FromResult(42);
                },
                TimeSpan.FromMinutes(5)
            );
            Assert.Equal(0, served);
            Assert.Equal(0, runs);

            if (!fixture.HasDistributedTier)
            {
                break;
            }
        }

        Assert.Equal(0, await cache.GetAsync<int>("zero"));
        Assert.Equal(0, await cache.GetAsync<int>("absent"));
        Assert.False((await cache.TryGetAsync<int>("absent")).Found);
    }

    public static async Task GetMany_ReadsBothTiersAndIsPartialUnderFailure(
        CacheConformanceFixture fixture
    )
    {
        var writer = fixture.CreateCache();
        var options = new CacheEntryOptions(TimeSpan.FromMinutes(5));
        for (var i = 0; i < 10; i++)
        {
            await writer.SetAsync(
                $"many-{i}",
                new Payload(i.ToString(System.Globalization.CultureInfo.InvariantCulture)),
                options
            );
        }

        var reader = fixture.HasDistributedTier ? fixture.CreateCache() : writer;
        var keys = Enumerable.Range(0, 12).Select(i => $"many-{i}").ToList();
        var found = await reader.GetManyAsync<Payload>(keys);
        Assert.Equal(10, found.Count);
        Assert.Equal("7", found["many-7"].Text);

        if (fixture.Faults is null)
        {
            return;
        }

        // Half the keys are now in the reader's in-process tier; a failing store still returns them.
        var third = fixture.CreateCache();
        Assert.Equal(1, (await third.GetManyAsync<Payload>(["many-1"])).Count);
        fixture.Faults.FailAll(CacheFailureKind.Unavailable);
        var partial = await third.GetManyAsync<Payload>(keys);
        Assert.Single(partial);
        Assert.Equal("1", partial["many-1"].Text);
        fixture.Faults.Heal();
    }

    public static async Task Warmup_FillsBothTiersAndIsFailOpen(CacheConformanceFixture fixture)
    {
        var cache = fixture.CreateCache();
        var entries = Enumerable
            .Range(0, 250)
            .ToDictionary(
                i => $"warm-{i}",
                i => new Payload(i.ToString(System.Globalization.CultureInfo.InvariantCulture))
            );
        var reports = new List<CacheWarmupProgress>();
        var progress = new SynchronousProgress(reports.Add);

        await cache.WarmupAsync(entries, TimeSpan.FromMinutes(5), progress);

        Assert.Equal(250, reports[^1].Written);
        Assert.Equal(250, reports[^1].Total);
        Assert.True((await cache.TryGetAsync<Payload>("warm-249")).Found);
        if (fixture.HasDistributedTier)
        {
            var other = fixture.CreateCache();
            Assert.Equal(CacheTier.L2, (await other.TryGetAsync<Payload>("warm-0")).Tier);
        }

        if (fixture.Faults is null)
        {
            return;
        }

        fixture.Faults.FailAll(CacheFailureKind.Unavailable);
        var failedReports = new List<CacheWarmupProgress>();
        await cache.WarmupAsync(
            new Dictionary<string, Payload> { ["warm-x"] = new("x") },
            TimeSpan.FromMinutes(5),
            new SynchronousProgress(failedReports.Add)
        );
        Assert.Empty(failedReports);
        fixture.Faults.Heal();
    }

    /// <summary>
    /// Waits for background work (the invalidation loop) without sleeping: yields to the thread
    /// pool until the condition holds, bounded by wall-clock time so a regression fails loudly.
    /// </summary>
    public static async Task WaitUntilAsync(Func<Task<bool>> condition, int timeoutSeconds = 10)
    {
        var start = Stopwatch.GetTimestamp();
        while (!await condition())
        {
            if (Stopwatch.GetElapsedTime(start) > TimeSpan.FromSeconds(timeoutSeconds))
            {
                Assert.Fail("Condition did not hold within the allowed time.");
            }

            await Task.Yield();
        }
    }

    /// <summary>The value type every scenario caches.</summary>
    public sealed record Payload(string Text);

    /// <summary>A struct whose default value must be distinguishable from a miss.</summary>
    public readonly record struct Point(int X, int Y);

    private sealed class SynchronousProgress(Action<CacheWarmupProgress> report)
        : IProgress<CacheWarmupProgress>
    {
        public void Report(CacheWarmupProgress value) => report(value);
    }
}
