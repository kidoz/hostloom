using System.Collections.Concurrent;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Caching.Testing;
using HostLoom.Conformance;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Caching;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridCacheStoreTlsTests
{
    [Theory(Timeout = 45000)]
    [InlineData(1)]
    [InlineData(3)]
    public async Task StorePreservesBuffersTtlAtomicInsertionAndBulkResults(int memberCount)
    {
        RequireLinux();
        var token = TestContext.Current.CancellationToken;
        await using var fixture = new DataGridClientTlsTests.ClientCluster(
            memberCount: memberCount
        );
        var leader = await fixture.Cluster.Leader(token);
        await using var writer = fixture.Client(leader);
        await using var reader = fixture.Client(leader, otherWriter: true);
        var store = new DataGridCacheStore(writer, DataGridPeerFixture.Map);
        var second = new DataGridCacheStore(reader, DataGridPeerFixture.Map);
        var ttl = TimeSpan.FromSeconds(60);
        var input = new byte[] { 1, 2 };
        await store.SetAsync("catalog:eu", input, ttl, cancellationToken: token);
        input[0] = 99;
        var read = Assert.IsType<CacheStoreEntry>(await second.GetAsync("catalog:eu", token));
        Assert.Equal(new byte[] { 1, 2 }, read.Payload.ToArray());
        Assert.InRange(read.RemainingTimeToLive!.Value.Ticks, 1, ttl.Ticks);
        await store.SetAsync(
            "catalog:eu",
            new byte[] { 88 },
            TimeSpan.Zero,
            cancellationToken: token
        );
        var unchanged = await second.GetAsync("catalog:eu", token);
        Assert.Equal(new byte[] { 1, 2 }, unchanged!.Value.Payload.ToArray());

        var inserted = await Task.WhenAll(
            store
                .SetIfAbsentAsync("catalog:us", new byte[] { 3 }, ttl, cancellationToken: token)
                .AsTask(),
            second
                .SetIfAbsentAsync("catalog:us", new byte[] { 4 }, ttl, cancellationToken: token)
                .AsTask()
        );
        Assert.Single(inserted, result => result);
        await store.SetManyAsync(
            [new("catalog:ap", new byte[] { 5 }), new("catalog:ap", new byte[] { 6 })],
            ttl,
            token
        );
        var many = await second.GetManyAsync(
            ["catalog:eu", "catalog:us", "catalog:ap", "catalog:none", "catalog:eu"],
            token
        );
        Assert.Equal(3, many.Count);
        Assert.Equal(new byte[] { 6 }, many["catalog:ap"].Payload.ToArray());
        Assert.Equal(
            new[] { inserted[0] ? (byte)3 : (byte)4 },
            many["catalog:us"].Payload.ToArray()
        );
        Assert.InRange(many["catalog:ap"].RemainingTimeToLive!.Value.Ticks, 1, ttl.Ticks);
        await store.RemoveAsync(["catalog:eu", "catalog:none", "catalog:eu"], token);
        Assert.Null(await second.GetAsync("catalog:eu", token));
        Assert.Equal(new byte[] { 1, 2 }, read.Payload.ToArray());
        Assert.Equal(CacheStoreCapabilities.None, store.Capabilities);
        Assert.Throws<NotSupportedException>(() =>
        {
            _ = store.RemoveByTagAsync("catalog", token);
        });
        await Assert.ThrowsAsync<ArgumentOutOfRangeException>(() =>
            store
                .GetManyAsync(
                    Enumerable
                        .Range(0, DataGridCacheStore.MaxReadKeys + 1)
                        .Select(i => $"catalog:{i}")
                        .ToArray(),
                    token
                )
                .AsTask()
        );
        using var canceled = new CancellationTokenSource();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            store.RemoveAsync(["catalog:us"], canceled.Token).AsTask()
        );
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 35000)]
    public async Task UnknownMutationKeepsItsIdentityAndDoesNotConsumeNextCall()
    {
        RequireLinux();
        var token = TestContext.Current.CancellationToken;
        await using var fixture = new DataGridClientTlsTests.ClientCluster(memberCount: 1);
        var leader = await fixture.Cluster.Leader(token);
        await using var client = fixture.Client(leader);
        var store = new DataGridCacheStore(client, DataGridPeerFixture.Map);
        var attempts = new ConcurrentQueue<(long Session, ulong Request, string Digest)>();
        fixture.Override = async (runtime, request, cancellation) =>
        {
            var reply = await runtime.ExecuteAsync(
                DataGridPeerFixture.Principal,
                request,
                cancellation
            );
            if (request.Mutation is not { } mutation)
                return reply;
            attempts.Enqueue(
                (
                    request.SessionNumber,
                    request.RequestNumber,
                    Convert.ToHexString(mutation.ComputeDigest())
                )
            );
            return DataGridClientReply.Failed(new(DataGridClientFailureCode.OutcomeUnknown, true));
        };
        var error = await Assert.ThrowsAsync<CacheStoreException>(() =>
            store
                .SetAsync(
                    "catalog:eu",
                    new byte[] { 1 },
                    TimeSpan.FromMinutes(1),
                    cancellationToken: token
                )
                .AsTask()
        );
        Assert.Equal(CacheFailureKind.Unavailable, error.Kind);
        Assert.NotEmpty(attempts);
        Assert.Single(attempts.Distinct());
        fixture.Override = null;
        await store.SetAsync(
            "catalog:us",
            new byte[] { 2 },
            TimeSpan.FromMinutes(1),
            cancellationToken: token
        );
        var read = await store.GetManyAsync(["catalog:eu", "catalog:us"], token);
        Assert.Equal(new byte[] { 1 }, read["catalog:eu"].Payload.ToArray());
        Assert.Equal(new byte[] { 2 }, read["catalog:us"].Payload.ToArray());
        fixture.AssertHealthy();
    }

    [Theory(Timeout = 45000)]
    [InlineData(nameof(CacheConformance.ValueTypes_CachedZeroFalseAndDefaultStruct_AreFound))]
    [InlineData(nameof(CacheConformance.NullOrNonPositiveExpiration_IsNotStored))]
    public async Task SharedConformanceRunsAcrossTheDistributedTier(string scenario)
    {
        RequireLinux();
        var token = TestContext.Current.CancellationToken;
        await using var fixture = new DataGridClientTlsTests.ClientCluster(memberCount: 1);
        var leader = await fixture.Cluster.Leader(token);
        await using var client = fixture.Client(leader);
        var store = new DataGridCacheStore(client, DataGridPeerFixture.Map);
        var faults = new FaultingCacheStore(store);
        var caches = new List<TieredCache>();
        var serializer = new SystemTextJsonCacheValueSerializer(
            new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }
        );
        try
        {
            await CacheConformance.Scenarios[scenario](
                new()
                {
                    Namespace = "grid-cache-conformance",
                    Clock = new ManualConformanceClock(new ManualTimeProvider()),
                    Faults = faults,
                    CreateCache = () =>
                    {
                        var options = new CachingOptions { Namespace = "grid-cache-conformance" };
                        options.L1.Enabled = false;
                        var cache = new TieredCache(options, faults, serializer);
                        caches.Add(cache);
                        return cache;
                    },
                }
            );
        }
        finally
        {
            foreach (var cache in caches)
                await cache.DisposeAsync();
        }
    }

    [Fact(Timeout = 35000)]
    public async Task NamedProfileReadsFromTheGridAndUsesItsExpiry()
    {
        RequireLinux();
        var token = TestContext.Current.CancellationToken;
        var clock = new DataGridSessionPressureTlsTests.LogicalClock();
        await using var fixture = new DataGridClientTlsTests.ClientCluster(
            timeProvider: clock,
            memberCount: 1
        );
        var leader = await fixture.Cluster.Leader(token);
        await using var client = fixture.Client(leader);
        var store = new DataGridCacheStore(client, DataGridPeerFixture.Map, clock);
        const string profile = "catalog";
        var services = new ServiceCollection();
        services.AddKeyedSingleton(profile, store);
        services
            .AddHostLoomCaching(
                profile,
                options =>
                {
                    options.Namespace = "grid-catalog";
                    options.L1.Enabled = false;
                }
            )
            .UseStore<DataGridCacheStore>("DataGrid")
            .UseSystemTextJson(
                new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }
            );

        await using var provider = services.BuildServiceProvider();
        Assert.Same(store, provider.GetRequiredKeyedService<IDistributedCacheStore>(profile));
        Assert.Null(provider.GetService<ICache>());
        var cache = provider.GetRequiredKeyedService<ICache>(profile);
        await cache.SetAsync("eu", 42, new CacheEntryOptions(TimeSpan.FromMinutes(2)), token);
        clock.Advance(TimeSpan.FromMinutes(1));
        Assert.Equal(42, await cache.GetAsync<int>("eu", cancellationToken: token));
        clock.Advance(TimeSpan.FromMinutes(1));
        Assert.False((await cache.TryGetAsync<int>("eu", cancellationToken: token)).Found);
        clock.Advance(TimeSpan.FromMinutes(10));
        await cache.SetAsync("eu", 43, new CacheEntryOptions(TimeSpan.FromMinutes(1)), token);
        Assert.Equal(43, await cache.GetAsync<int>("eu", cancellationToken: token));
        fixture.AssertHealthy();
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "DataGrid cache store conformance requires Linux TLS 1.3."
        );
}
