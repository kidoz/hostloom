using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Locking;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

[Collection(nameof(RedisClusterFailoverTests))]
[CollectionDefinition(nameof(RedisClusterFailoverTests), DisableParallelization = true)]
public sealed class RedisClusterFailoverTests
{
    public static bool Enabled =>
        !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(RedisClusterFixture.Variable));
    private const string Skip =
        "Run scripts/test-redis-cluster.py for an owned three-primary, three-replica cluster.";

    [Theory(Timeout = 30_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(false, 0)]
    [InlineData(true, 1)]
    public async Task Cluster_rejects_incompatible_settings_before_cache_commands(
        bool hashTags,
        int database
    )
    {
        var fixture = new RedisClusterFixture();
        var options = fixture.Options();
        options.UseHashTags = hashTags;
        options.DatabaseIndex = database;
        await using var connection = new RedisConnection(options);
        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            connection.GetDatabaseAsync(TestContext.Current.CancellationToken).AsTask()
        );
        Assert.Contains(
            "UseHashTags = true and DatabaseIndex = 0",
            exception.Message,
            StringComparison.Ordinal
        );
    }

    [Theory(Timeout = 120_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Cache_RecoversRoutingTagsConditionalAndBulkOperationsAfterPrimaryCrash(
        int shard
    )
    {
        var token = TestContext.Current.CancellationToken;
        var fixture = new RedisClusterFixture();
        await using var connection = new RedisConnection(fixture.Options());
        await using var store = new RedisCacheStore(connection);
        var (ns, primary, replica) = await fixture.SelectShardAsync(connection, shard, token);
        var db = await connection.GetDatabaseAsync(token);
        var key = ns + ":cache:data:catalog";
        var tag = ns + ":cache:tag:books";
        byte[] value = [0, 255, 1, 13];
        var ttl = TimeSpan.FromMinutes(2);
        await store.SetAsync(key, value, ttl, [tag], token);
        await RedisClusterFixture.WaitReplicatedAsync(
            db,
            Physical(ns, "cache:data:catalog"),
            value,
            token
        );
        // Also wait for the tag index, since its commands follow the value write.
        await RedisClusterFixture.PollAsync(
            async () =>
                await db.SetContainsAsync(
                        Physical(ns, "cache:tag:books"),
                        Physical(ns, "cache:data:catalog"),
                        CommandFlags.DemandReplica
                    )
                    .WaitAsync(token),
            token
        );
        Assert.Equal(
            primary.EndPoint,
            await db.IdentifyEndpointAsync(
                Physical(ns, "cache:data:catalog"),
                CommandFlags.DemandMaster
            )
        );

        await fixture.CrashAsync(primary, token);
        try
        {
            await fixture.WaitForPromotionAsync(replica, token);
            await RedisClusterFixture.WaitForClientRoutingAsync(
                connection,
                Physical(ns, "cache:data:catalog"),
                replica,
                token
            );
            await WaitCacheAsync(async () => (await store.GetAsync(key, token)) is not null, token);
            Assert.Equal(
                replica.EndPoint,
                await db.IdentifyEndpointAsync(
                    Physical(ns, "cache:data:catalog"),
                    CommandFlags.DemandMaster
                )
            );
            Assert.Equal(value, (await store.GetAsync(key, token))!.Value.Payload.ToArray());
            Assert.True((await store.CheckHealthAsync(token)).IsHealthy);
            Assert.False(
                await store.SetIfAbsentAsync(key, new byte[] { 9 }, ttl, cancellationToken: token)
            );
            Assert.True(
                await store.SetIfAbsentAsync(key + "-new", new byte[] { 7 }, ttl, [tag], token)
            );
            await store.RemoveByTagAsync(tag, token);
            Assert.Empty(await store.GetManyAsync([key, key + "-new"], token));
            await store.SetManyAsync(
                [
                    new KeyValuePair<string, ReadOnlyMemory<byte>>(key, value),
                    new KeyValuePair<string, ReadOnlyMemory<byte>>(key + "-bulk", new byte[] { 8 }),
                ],
                ttl,
                token
            );
            var bulk = await store.GetManyAsync([key, key + "-bulk", key + "-missing"], token);
            Assert.Equal(2, bulk.Count);
            Assert.Equal(value, bulk[key].Payload.ToArray());
            Assert.Equal(new byte[] { 8 }, bulk[key + "-bulk"].Payload.ToArray());
            Assert.InRange(bulk[key].RemainingTimeToLive!.Value, TimeSpan.FromSeconds(1), ttl);
        }
        finally
        {
            using var recovery = new CancellationTokenSource(TimeSpan.FromSeconds(40));
            await fixture.RestartAsync(primary, recovery.Token);
        }
    }

    [Theory(Timeout = 120_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task ReplicatedLock_PreservesOwnerAndRejectsStaleReleaseAfterPrimaryCrash(
        int shard
    )
    {
        var token = TestContext.Current.CancellationToken;
        var fixture = new RedisClusterFixture();
        await using var connection = new RedisConnection(fixture.Options());
        await using var otherConnection = new RedisConnection(fixture.Options());
        await using var provider = new RedisLockProvider(connection);
        await using var otherProvider = new RedisLockProvider(otherConnection);
        var (ns, primary, replica) = await fixture.SelectShardAsync(connection, shard, token);
        await using var mutex = new DistributedLock(
            new LockingOptions { Namespace = ns },
            provider
        );
        await using var competitor = new DistributedLock(
            new LockingOptions { Namespace = ns },
            otherProvider
        );
        await using var held = await mutex.TryAcquireAsync(
            "inventory",
            new LockOptions { Lease = TimeSpan.FromSeconds(60) },
            token
        );
        Assert.NotNull(held);
        var db = await connection.GetDatabaseAsync(token);
        var physicalKey = Physical(ns, "lock:inventory");
        var owner = await db.StringGetAsync(physicalKey);
        Assert.False(owner.IsNullOrEmpty);
        // This is a replicated-lease guarantee; it deliberately does not claim that Redis
        // asynchronous replication preserves an unreplicated acquisition across failover.
        await RedisClusterFixture.WaitReplicatedAsync(db, physicalKey, owner, token);
        Assert.Null(await competitor.TryAcquireAsync("inventory", cancellationToken: token));

        await fixture.CrashAsync(primary, token);
        try
        {
            await fixture.WaitForPromotionAsync(replica, token);
            await RedisClusterFixture.WaitForClientRoutingAsync(
                connection,
                physicalKey,
                replica,
                token
            );
            await RedisClusterFixture.WaitForClientRoutingAsync(
                otherConnection,
                physicalKey,
                replica,
                token
            );
            await RedisClusterFixture.PollAsync(
                async () =>
                {
                    try
                    {
                        return await held.ExtendAsync(TimeSpan.FromSeconds(60), token);
                    }
                    catch (LockProviderUnavailableException)
                    {
                        return false;
                    }
                },
                token
            );
            Assert.True(held.IsHeld);
            Assert.False(held.LostToken.IsCancellationRequested);
            Assert.Equal(owner, await db.StringGetAsync(physicalKey));
            Assert.Null(await competitor.TryAcquireAsync("inventory", cancellationToken: token));
            Assert.False(
                await otherProvider.ReleaseAsync(ns + ":lock:inventory", "intruder", token)
            );
            Assert.False(
                await otherProvider.ExtendAsync(
                    ns + ":lock:inventory",
                    "intruder",
                    TimeSpan.FromSeconds(60),
                    token
                )
            );
            Assert.True(await held.ExtendAsync(TimeSpan.FromMilliseconds(300), token));
            await RedisClusterFixture.PollAsync(
                () => Task.FromResult(held.LostToken.IsCancellationRequested),
                token
            );
            await using var successor = await competitor.TryAcquireAsync(
                "inventory",
                new LockOptions
                {
                    MaxWait = TimeSpan.FromSeconds(5),
                    Retry = LockRetryPolicy.Linear(50, TimeSpan.FromMilliseconds(10)),
                },
                token
            );
            Assert.NotNull(successor);
            await held.DisposeAsync();
            Assert.Null(await mutex.TryAcquireAsync("inventory", cancellationToken: token));
            Assert.True(successor.IsHeld);
            await successor.DisposeAsync();
            Assert.Equal(
                42,
                await mutex.ExecuteWithLockAsync(
                    "inventory",
                    _ => ValueTask.FromResult(42),
                    cancellationToken: token
                )
            );
        }
        finally
        {
            using var recovery = new CancellationTokenSource(TimeSpan.FromSeconds(40));
            await fixture.RestartAsync(primary, recovery.Token);
        }
    }

    [Theory(Timeout = 120_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task Tracking_EvictsRemoteL1BeforeAndAfterPrimaryPromotion(int shard)
    {
        var token = TestContext.Current.CancellationToken;
        var fixture = new RedisClusterFixture();
        await using var readerConnection = new RedisConnection(fixture.Options());
        await using var writerConnection = new RedisConnection(fixture.Options());
        await using var readerStore = new RedisCacheStore(readerConnection);
        await using var writerStore = new RedisCacheStore(writerConnection);
        var (ns, primary, replica) = await fixture.SelectShardAsync(readerConnection, shard, token);
        var options = new CachingOptions { Namespace = ns };
        options.Invalidation.Mode = CacheInvalidationMode.Tracking;
        await using var channel = new RedisCacheInvalidationChannel(readerConnection, options);
        await using var reader = new TieredCache(options, readerStore, Serializer(), channel);
        await using var writer = new TieredCache(
            new CachingOptions { Namespace = ns },
            writerStore,
            Serializer()
        );
        await RedisClusterFixture.PollAsync(
            () => Task.FromResult(channel.Transport != RedisInvalidationTransport.Pending),
            token
        );
        Assert.Equal(RedisInvalidationTransport.Tracking, channel.Transport);
        var entry = new CacheEntryOptions(TimeSpan.FromMinutes(2));
        await reader.SetAsync("catalog", 1, entry, token);
        Assert.Equal(CacheTier.L1, (await reader.TryGetAsync<int>("catalog", token)).Tier);
        await writer.SetAsync("catalog", 2, entry, token);
        await AssertTrackingAsync(
            reader,
            writerStore,
            readerConnection,
            channel,
            ns,
            2,
            "before primary crash",
            token
        );

        await fixture.CrashAsync(primary, token);
        try
        {
            await fixture.WaitForPromotionAsync(replica, token);
            await RedisClusterFixture.WaitForClientRoutingAsync(
                readerConnection,
                Physical(ns, "cache:data:catalog"),
                replica,
                token
            );
            await RedisClusterFixture.WaitForClientRoutingAsync(
                writerConnection,
                Physical(ns, "cache:data:catalog"),
                replica,
                token
            );
            await WaitCacheAsync(
                async () =>
                    await writerStore.SetIfAbsentAsync(
                        ns + ":cache:data:ready",
                        new byte[] { 1 },
                        entry.Expiration,
                        cancellationToken: token
                    ),
                token
            );
            await writer.SetAsync("catalog", 3, entry, token);
            await AssertTrackingAsync(
                reader,
                writerStore,
                readerConnection,
                channel,
                ns,
                3,
                "after promotion",
                token
            );
        }
        finally
        {
            using var recovery = new CancellationTokenSource(TimeSpan.FromSeconds(40));
            await fixture.RestartAsync(primary, recovery.Token);
        }
    }

    [Fact(Timeout = 120_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task ExplicitRemoval_InvalidatesRemoteL1AfterPrimaryPromotion()
    {
        var token = TestContext.Current.CancellationToken;
        var fixture = new RedisClusterFixture();
        await using var readerConnection = new RedisConnection(fixture.Options());
        await using var writerConnection = new RedisConnection(fixture.Options());
        await using var readerStore = new RedisCacheStore(readerConnection);
        await using var writerStore = new RedisCacheStore(writerConnection);
        var (ns, primary, replica) = await fixture.SelectShardAsync(readerConnection, 0, token);
        var options = new CachingOptions { Namespace = ns };
        // Broadcast has no configured keyspace events in this fixture, so removal must use
        // the explicit channel; tracking cannot accidentally make this assertion pass.
        options.Invalidation.Mode = CacheInvalidationMode.Broadcast;
        await using var readerChannel = new RedisCacheInvalidationChannel(
            readerConnection,
            options
        );
        await using var writerChannel = new RedisCacheInvalidationChannel(
            writerConnection,
            options
        );
        await using var reader = new TieredCache(options, readerStore, Serializer(), readerChannel);
        await using var writer = new TieredCache(options, writerStore, Serializer(), writerChannel);
        await RedisClusterFixture.PollAsync(
            () =>
                Task.FromResult(
                    readerChannel.Transport != RedisInvalidationTransport.Pending
                        && writerChannel.Transport != RedisInvalidationTransport.Pending
                ),
            token
        );
        var entry = new CacheEntryOptions(TimeSpan.FromMinutes(2));
        await writer.SetAsync("catalog", 7, entry, token);
        Assert.Equal(7, (await reader.TryGetAsync<int>("catalog", token)).Value);
        var db = await writerConnection.GetDatabaseAsync(token);
        await RedisClusterFixture.WaitReplicatedAsync(
            db,
            Physical(ns, "cache:data:catalog"),
            await db.StringGetAsync(Physical(ns, "cache:data:catalog")),
            token
        );
        await fixture.CrashAsync(primary, token);
        try
        {
            await fixture.WaitForPromotionAsync(replica, token);
            await RedisClusterFixture.WaitForClientRoutingAsync(
                readerConnection,
                Physical(ns, "cache:data:catalog"),
                replica,
                token
            );
            await RedisClusterFixture.WaitForClientRoutingAsync(
                writerConnection,
                Physical(ns, "cache:data:catalog"),
                replica,
                token
            );
            await WaitCacheAsync(
                async () =>
                    (await writerStore.GetAsync(ns + ":cache:data:catalog", token)) is not null,
                token
            );
            await writer.RemoveAsync("catalog", token);
            await RedisClusterFixture.PollAsync(
                async () => !(await reader.TryGetAsync<int>("catalog", token)).Found,
                token
            );
            Assert.Null(await readerStore.GetAsync(ns + ":cache:data:catalog", token));
        }
        finally
        {
            using var recovery = new CancellationTokenSource(TimeSpan.FromSeconds(40));
            await fixture.RestartAsync(primary, recovery.Token);
        }
    }

    private static async Task AssertTrackingAsync(
        TieredCache reader,
        RedisCacheStore writerStore,
        RedisConnection readerConnection,
        RedisCacheInvalidationChannel channel,
        string ns,
        int expected,
        string phase,
        CancellationToken token
    )
    {
        await using var probe = new TieredCache(
            new CachingOptions { Namespace = ns },
            writerStore,
            Serializer()
        );
        var persisted = await probe.TryGetAsync<int>("catalog", token);
        Assert.Equal(CacheTier.L2, persisted.Tier);
        Assert.Equal(expected, persisted.Value);
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"{phase}: L2 confirmed {expected}; transport={channel.Transport}; tracking registrations={channel.TrackingInitialisations}; key={Physical(ns, "cache:data:catalog")}."
        );
        try
        {
            await RedisClusterFixture.PollAsync(
                async () => (await reader.TryGetAsync<int>("catalog", token)).Value == expected,
                token
            );
        }
        catch (TimeoutException)
        {
            var mux = await readerConnection.GetMultiplexerAsync(token);
            foreach (var endpoint in mux.GetEndPoints())
            {
                try
                {
                    var clients = (string?)
                        await mux.GetServer(endpoint)
                            .ExecuteAsync("CLIENT", "LIST")
                            .WaitAsync(token);
                    foreach (
                        var client in (clients ?? "")
                            .Split('\n')
                            .Where(line =>
                                line.Contains(
                                    "name=" + readerConnection.ClientName + " ",
                                    StringComparison.Ordinal
                                )
                            )
                    )
                    {
                        var fields = client
                            .Split(' ')
                            .Where(field =>
                                field.StartsWith("id=", StringComparison.Ordinal)
                                || field.StartsWith("flags=", StringComparison.Ordinal)
                                || field.StartsWith("redir=", StringComparison.Ordinal)
                            );
                        TestContext.Current.TestOutputHelper!.WriteLine(
                            $"Reader at {endpoint}: {string.Join(' ', fields)}"
                        );
                    }
                }
                catch (RedisException exception)
                {
                    TestContext.Current.TestOutputHelper!.WriteLine(
                        $"Node {endpoint} unavailable: {exception.GetType().Name}."
                    );
                }
            }
            var stale = await reader.TryGetAsync<int>("catalog", token);
            Assert.Fail(
                $"Tracking did not evict the reader {phase} within 30 seconds: Redis L2 contains {expected}, reader still returns {stale.Value} from {stale.Tier}; transport={channel.Transport}."
            );
        }
    }

    private static string Physical(string ns, string suffix) => "{" + ns + "}:" + suffix;

    private static SystemTextJsonCacheValueSerializer Serializer() =>
        new(new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() });

    private static Task WaitCacheAsync(Func<Task<bool>> condition, CancellationToken token) =>
        RedisClusterFixture.PollAsync(
            async () =>
            {
                try
                {
                    return await condition();
                }
                catch (CacheStoreException)
                {
                    return false;
                }
            },
            token
        );
}
