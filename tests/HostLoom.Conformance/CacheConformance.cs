using System.Diagnostics;
using System.Diagnostics.Metrics;
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

    /// <summary>
    /// The namespace every instance uses, unique to the fixture, so a scenario can read the
    /// instances' own <c>hostloom.cache.invalidations</c> measurements.
    /// </summary>
    public required string Namespace { get; init; }

    /// <summary>The fault injector in front of the shared store, or null for an in-process-only cache.</summary>
    public FaultingCacheStore? Faults { get; init; }

    /// <summary>Whether the composition has a distributed tier.</summary>
    public bool HasDistributedTier => Faults is not null;

    /// <summary>
    /// Whether a store write by one instance evicts another instance's in-process copy of the key:
    /// true where the backend reports writes to the other instances (Redis keyspace broadcast, or
    /// Redis tracking between instances on separate connections). An explicit channel alone
    /// carries only removals, so after an overwrite another instance serves its old in-process
    /// copy until that copy expires.
    /// </summary>
    public bool ReportsStoreWrites { get; init; }

    /// <summary>
    /// Whether an instance also receives its own store writes and removals as invalidations it
    /// cannot tell from another instance's: Redis keyspace broadcast, whose notifications have no
    /// <c>NOLOOP</c>, so every write evicts the writer's own in-process entry.
    /// </summary>
    public bool ReportsOwnStoreWrites { get; init; }

    /// <summary>The <see cref="CacheInvalidationOptions.FlushLocalOnReconnect"/> the instances' channel was built with.</summary>
    public bool FlushLocalOnReconnect { get; init; } = true;

    /// <summary>
    /// Interrupts the subscription behind every instance's channel and returns once it has been
    /// restored, such that a message published afterwards reaches the instances after any flush
    /// the restoration hands them; null where the runner cannot interrupt a subscription.
    /// </summary>
    public Func<Task>? RestoreSubscriptionAsync { get; init; }
}

/// <summary>
/// Backend-neutral cache scenarios. The unit suite runs them on the in-process backends, with and
/// without a container; the integration suite runs the same methods on Redis and Valkey.
/// </summary>
public static class CacheConformance
{
    /// <summary>
    /// The cross-instance invalidation scenarios. Unlike the rest, they state what each channel
    /// capability promises through the fixture's flags, so a runner can also run them on a
    /// composition whose backend reports store writes, or whose channel does not flush on
    /// reconnect, without the other scenarios' assumption that a write stays in the writer's
    /// in-process tier.
    /// </summary>
    public static IReadOnlyDictionary<
        string,
        Func<CacheConformanceFixture, Task>
    > InvalidationScenarios { get; } =
        new Dictionary<string, Func<CacheConformanceFixture, Task>>(StringComparer.Ordinal)
        {
            [nameof(RemoveMany_EvictsL1OnEveryInstance)] = RemoveMany_EvictsL1OnEveryInstance,
            [nameof(Overwrite_ReachesAnotherInstanceAsTheChannelReportsIt)] =
                Overwrite_ReachesAnotherInstanceAsTheChannelReportsIt,
            [nameof(OwnRemoval_IsRecognisedAsAnEchoAndTheRefillStaysLocal)] =
                OwnRemoval_IsRecognisedAsAnEchoAndTheRefillStaysLocal,
            [nameof(DeliveryGap_FlushesL1OnlyWhenFlushLocalOnReconnect)] =
                DeliveryGap_FlushesL1OnlyWhenFlushLocalOnReconnect,
            [nameof(RemoveByTag_EvictsTaggedCopiesOnAnotherInstanceAndKeepsTheRest)] =
                RemoveByTag_EvictsTaggedCopiesOnAnotherInstanceAndKeepsTheRest,
        };

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
            [nameof(RemoveMany_EvictsL1OnEveryInstance)] = RemoveMany_EvictsL1OnEveryInstance,
            [nameof(Overwrite_ReachesAnotherInstanceAsTheChannelReportsIt)] =
                Overwrite_ReachesAnotherInstanceAsTheChannelReportsIt,
            [nameof(OwnRemoval_IsRecognisedAsAnEchoAndTheRefillStaysLocal)] =
                OwnRemoval_IsRecognisedAsAnEchoAndTheRefillStaysLocal,
            [nameof(DeliveryGap_FlushesL1OnlyWhenFlushLocalOnReconnect)] =
                DeliveryGap_FlushesL1OnlyWhenFlushLocalOnReconnect,
            [nameof(RemoveByTag_EvictsTaggedCopiesOnAnotherInstanceAndKeepsTheRest)] =
                RemoveByTag_EvictsTaggedCopiesOnAnotherInstanceAndKeepsTheRest,
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

    public static async Task RemoveMany_EvictsL1OnEveryInstance(CacheConformanceFixture fixture)
    {
        if (!fixture.HasDistributedTier)
        {
            return;
        }

        var a = fixture.CreateCache();
        var b = fixture.CreateCache();
        var entry = new CacheEntryOptions(TimeSpan.FromMinutes(5));
        string[] keys = ["orders:1", "orders:2", "orders:3"];
        foreach (var key in keys)
        {
            await a.SetAsync(key, new Payload(key), entry);
        }

        foreach (var key in keys)
        {
            await HoldLocallyAsync(fixture, b, key, () => b.TryGetAsync<Payload>(key).AsTask());
        }

        await a.RemoveAsync(["orders:1", "orders:2"]);

        // Both keys travel in one message and left the distributed tier first, so neither is found
        // anywhere once the other instance has applied it.
        await WaitUntilAsync(async () =>
            !(await b.TryGetAsync<Payload>("orders:1")).Found
            && !(await b.TryGetAsync<Payload>("orders:2")).Found
        );
        Assert.False((await a.TryGetAsync<Payload>("orders:1")).Found);
        Assert.False((await a.TryGetAsync<Payload>("orders:2")).Found);
        var kept = await b.TryGetAsync<Payload>("orders:3");
        Assert.Equal("orders:3", kept.Value!.Text);
        if (!fixture.ReportsStoreWrites)
        {
            // Nothing but the removal reached the other instance, and it named other keys.
            Assert.Equal(CacheTier.L1, kept.Tier);
        }
    }

    public static async Task Overwrite_ReachesAnotherInstanceAsTheChannelReportsIt(
        CacheConformanceFixture fixture
    )
    {
        if (!fixture.HasDistributedTier)
        {
            return;
        }

        var a = fixture.CreateCache();
        var b = fixture.CreateCache();
        var write = new CacheEntryOptions(TimeSpan.FromMinutes(5));
        await a.SetAsync("invoices:7", new Payload("v1"), write);

        // Where nothing reports the overwrite, the other instance's copy lives briefly so the
        // scenario can wait it out; where something does, it outlives the scenario, so only the
        // report can remove it.
        var localLife = fixture.ReportsStoreWrites
            ? TimeSpan.FromMinutes(5)
            : TimeSpan.FromSeconds(2);
        var read = new CacheEntryOptions(TimeSpan.FromMinutes(5)) { LocalExpiration = localLife };
        var runs = 0;
        ValueTask<Payload> Factory(CancellationToken _)
        {
            Interlocked.Increment(ref runs);
            return ValueTask.FromResult(new Payload("factory"));
        }

        await HoldLocallyAsync(
            fixture,
            b,
            "invoices:7",
            () => b.GetOrCreateAsync("invoices:7", Factory, read).AsTask()
        );

        await a.SetAsync("invoices:7", new Payload("v2"), write);
        Assert.Equal("v2", (await a.GetAsync<Payload>("invoices:7"))!.Text);

        if (fixture.ReportsStoreWrites)
        {
            // The backend reports the write, so the other instance drops its copy long before
            // the copy would expire and reads the new value.
            await WaitUntilAsync(async () =>
                (await b.TryGetAsync<Payload>("invoices:7")).Value?.Text == "v2"
            );
        }
        else
        {
            // Only removals are published: the other instance serves its copy until the copy
            // expires, then reads the new value.
            var stale = await b.TryGetAsync<Payload>("invoices:7");
            Assert.Equal(CacheTier.L1, stale.Tier);
            Assert.Equal("v1", stale.Value!.Text);
            await fixture.Clock.AdvanceAsync(localLife);
            var fresh = await b.TryGetAsync<Payload>("invoices:7");
            Assert.Equal(CacheTier.L2, fresh.Tier);
            Assert.Equal("v2", fresh.Value!.Text);
        }

        Assert.Equal(0, runs);
    }

    public static async Task OwnRemoval_IsRecognisedAsAnEchoAndTheRefillStaysLocal(
        CacheConformanceFixture fixture
    )
    {
        if (!fixture.HasDistributedTier)
        {
            return;
        }

        using var invalidations = new InvalidationCounter(fixture.Namespace);
        var cache = fixture.CreateCache();
        var entry = new CacheEntryOptions(TimeSpan.FromMinutes(5));
        await cache.SetAsync("customers:4", new Payload("v1"), entry);
        await cache.RemoveAsync("customers:4");

        if (fixture.ReportsOwnStoreWrites)
        {
            // The removal and the refill's own write come back as reports the instance cannot
            // tell from another instance's, so the refill is served from the distributed tier.
            var written = await cache.GetOrCreateAsync(
                "customers:4",
                _ => ValueTask.FromResult(new Payload("v2")),
                entry
            );
            Assert.Equal("v2", written!.Text);
            await WaitUntilAsync(async () =>
            {
                var lookup = await cache.TryGetAsync<Payload>("customers:4");
                return lookup.Tier == CacheTier.L2 && lookup.Value?.Text == "v2";
            });
            return;
        }

        var refill = await cache.GetOrCreateAsync(
            "customers:4",
            async token =>
            {
                // The echo is recognised while this refill is in flight, or was before it began.
                await invalidations.WaitForAsync("echoed", 1, token);
                return new Payload("v2");
            },
            entry
        );

        Assert.Equal("v2", refill!.Text);
        var local = await cache.TryGetAsync<Payload>("customers:4");
        Assert.Equal(CacheTier.L1, local.Tier);
        Assert.Equal("v2", local.Value!.Text);
        // Applied once, when it was removed; its echo was skipped rather than applied again.
        Assert.Equal(1, invalidations.Count("echoed"));
        Assert.Equal(0, invalidations.Count("received"));
    }

    public static async Task DeliveryGap_FlushesL1OnlyWhenFlushLocalOnReconnect(
        CacheConformanceFixture fixture
    )
    {
        if (fixture.RestoreSubscriptionAsync is not { } restoreSubscription)
        {
            // The runner cannot interrupt its channel's subscription; see its fixture.
            return;
        }

        if (fixture.ReportsStoreWrites)
        {
            // A report of the write that stored an entry can evict it at any time, so neither a
            // flush nor its absence can be told apart from one.
            return;
        }

        using var invalidations = new InvalidationCounter(fixture.Namespace);
        var a = fixture.CreateCache();
        var b = fixture.CreateCache();
        var entry = new CacheEntryOptions(TimeSpan.FromMinutes(5));
        await a.SetAsync("inventory:gap", new Payload("v1"), entry);
        await HoldLocallyAsync(
            fixture,
            b,
            "inventory:gap",
            () => b.TryGetAsync<Payload>("inventory:gap").AsTask()
        );
        await b.SetAsync("inventory:marker", new Payload("marker"), entry);

        await restoreSubscription();

        if (fixture.FlushLocalOnReconnect)
        {
            // Whatever was published while the subscription was down never arrived, so every
            // instance drops its in-process tier and reads the distributed tier again.
            await WaitUntilAsync(async () =>
                (await b.TryGetAsync<Payload>("inventory:gap")).Tier == CacheTier.L2
            );
            await WaitUntilAsync(async () =>
                (await a.TryGetAsync<Payload>("inventory:gap")).Tier == CacheTier.L2
            );
            await invalidations.WaitForAsync("flushed", 2, CancellationToken.None);
        }
        else
        {
            // Published after the restoration, so each instance applies it after anything the
            // restoration handed over: once the other instance has evicted the marker and this one
            // has skipped its echo, a flush would already have happened on both.
            await a.RemoveAsync("inventory:marker");
            await WaitUntilAsync(async () =>
                !(await b.TryGetAsync<Payload>("inventory:marker")).Found
            );
            await invalidations.WaitForAsync("echoed", 1, CancellationToken.None);
            Assert.Equal(CacheTier.L1, (await b.TryGetAsync<Payload>("inventory:gap")).Tier);
            Assert.Equal(CacheTier.L1, (await a.TryGetAsync<Payload>("inventory:gap")).Tier);
            Assert.Equal(0, invalidations.Count("flushed"));
        }

        // Either way the restored subscription delivers again.
        await a.RemoveAsync("inventory:gap");
        await WaitUntilAsync(async () => !(await b.TryGetAsync<Payload>("inventory:gap")).Found);
    }

    public static async Task RemoveByTag_EvictsTaggedCopiesOnAnotherInstanceAndKeepsTheRest(
        CacheConformanceFixture fixture
    )
    {
        if (!fixture.HasDistributedTier)
        {
            return;
        }

        var a = fixture.CreateCache();
        var b = fixture.CreateCache();
        var life = TimeSpan.FromMinutes(5);
        await a.SetAsync(
            "catalog:1",
            new Payload("c1"),
            new CacheEntryOptions(life) { Tags = ["catalog"] }
        );
        await a.SetAsync(
            "catalog:2",
            new Payload("c2"),
            new CacheEntryOptions(life) { Tags = ["catalog", "invoices"] }
        );
        await a.SetAsync(
            "customers:1",
            new Payload("u1"),
            new CacheEntryOptions(life) { Tags = ["customers"] }
        );
        await a.SetAsync("plain", new Payload("p"), new CacheEntryOptions(life));
        // The other instance learns each entry's tags from the payload it reads.
        foreach (var key in new[] { "catalog:1", "catalog:2", "customers:1", "plain" })
        {
            await HoldLocallyAsync(fixture, b, key, () => b.TryGetAsync<Payload>(key).AsTask());
        }

        await a.RemoveByTagAsync("catalog");

        await WaitUntilAsync(async () =>
            !(await b.TryGetAsync<Payload>("catalog:1")).Found
            && !(await b.TryGetAsync<Payload>("catalog:2")).Found
        );
        Assert.False((await a.TryGetAsync<Payload>("catalog:2")).Found);
        foreach (var (key, text) in new[] { ("customers:1", "u1"), ("plain", "p") })
        {
            var remote = await b.TryGetAsync<Payload>(key);
            Assert.Equal(text, remote.Value!.Text);
            if (!fixture.ReportsStoreWrites)
            {
                Assert.Equal(CacheTier.L1, remote.Tier);
            }

            var own = await a.TryGetAsync<Payload>(key);
            Assert.Equal(text, own.Value!.Text);
            if (!fixture.ReportsOwnStoreWrites)
            {
                Assert.Equal(CacheTier.L1, own.Tier);
            }
        }
    }

    /// <summary>
    /// Fills <paramref name="instance"/>'s in-process tier with <paramref name="key"/> through
    /// <paramref name="fill"/>, a read that finds it in the distributed tier. Where the backend
    /// reports store writes, the report of the write that stored the key can arrive after the
    /// fill and evict it, so the fill repeats until the in-process tier serves the key.
    /// </summary>
    private static async Task HoldLocallyAsync(
        CacheConformanceFixture fixture,
        ICache instance,
        string key,
        Func<Task> fill
    )
    {
        if (fixture.ReportsStoreWrites)
        {
            await WaitUntilAsync(async () =>
            {
                await fill();
                return (await instance.TryGetAsync<Payload>(key)).Tier == CacheTier.L1;
            });
            return;
        }

        await fill();
        Assert.Equal(CacheTier.L1, (await instance.TryGetAsync<Payload>(key)).Tier);
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

    /// <summary>
    /// Counts one namespace's <c>hostloom.cache.invalidations</c> by direction, summed over every
    /// instance using the namespace. A cache records a direction after it has applied, skipped,
    /// or flushed for the message, so a count is evidence that the work is done.
    /// </summary>
    private sealed class InvalidationCounter : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly string _namespace;
        private readonly Lock _gate = new();
        private readonly Dictionary<string, long> _directions = new(StringComparer.Ordinal);
        private TaskCompletionSource _changed = NewSignal();

        public InvalidationCounter(string @namespace)
        {
            _namespace = @namespace;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (
                    instrument.Meter.Name == CachingDiagnostics.MeterName
                    && instrument.Name == "hostloom.cache.invalidations"
                )
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>(Record);
            _listener.Start();
        }

        public long Count(string direction)
        {
            lock (_gate)
            {
                return _directions.GetValueOrDefault(direction);
            }
        }

        /// <summary>Completes once <paramref name="direction"/> has been counted <paramref name="atLeast"/> times; fails after ten seconds.</summary>
        public async Task WaitForAsync(
            string direction,
            long atLeast,
            CancellationToken cancellationToken
        )
        {
            using var bound = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
            bound.CancelAfter(TimeSpan.FromSeconds(10));
            while (true)
            {
                Task changed;
                lock (_gate)
                {
                    if (_directions.GetValueOrDefault(direction) >= atLeast)
                    {
                        return;
                    }

                    changed = _changed.Task;
                }

                try
                {
                    await changed.WaitAsync(bound.Token);
                }
                catch (OperationCanceledException) when (!cancellationToken.IsCancellationRequested)
                {
                    Assert.Fail(
                        $"Fewer than {atLeast} '{direction}' invalidations in '{_namespace}' within the allowed time."
                    );
                }
            }
        }

        public void Dispose() => _listener.Dispose();

        private static TaskCompletionSource NewSignal() =>
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        private void Record(
            Instrument instrument,
            long value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags,
            object? state
        )
        {
            string? ns = null;
            string? direction = null;
            foreach (var tag in tags)
            {
                if (tag.Key == "hostloom.cache.namespace")
                {
                    ns = tag.Value as string;
                }
                else if (tag.Key == "hostloom.cache.direction")
                {
                    direction = tag.Value as string;
                }
            }

            if (direction is null || !string.Equals(ns, _namespace, StringComparison.Ordinal))
            {
                return;
            }

            TaskCompletionSource signal;
            lock (_gate)
            {
                _directions[direction] = _directions.GetValueOrDefault(direction) + value;
                signal = _changed;
                _changed = NewSignal();
            }

            signal.TrySetResult();
        }
    }
}
