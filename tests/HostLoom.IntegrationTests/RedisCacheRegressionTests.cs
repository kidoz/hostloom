using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Conformance;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

[Collection(nameof(RedisCacheRegressionTests))]
[CollectionDefinition(nameof(RedisCacheRegressionTests), DisableParallelization = true)]
public sealed class RedisCacheRegressionTests
{
    public static bool Available => RedisAvailability.Redis;
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Theory(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData("set", false)]
    [InlineData("factory", false)]
    [InlineData("absent", false)]
    [InlineData("warmup", false)]
    [InlineData("rewrite", false)]
    [InlineData("set", true)]
    [InlineData("factory", true)]
    [InlineData("absent", true)]
    [InlineData("warmup", true)]
    [InlineData("rewrite", true)]
    public async Task Tracking_CoversEntriesPopulatedLocally(string population, bool hashTags)
    {
        var ns = "regression-" + Guid.NewGuid().ToString("N");
        await using var connectionA = Connection(hashTags);
        await using var connectionB = Connection(hashTags);
        await using var storeA = new RedisCacheStore(connectionA);
        await using var storeB = new RedisCacheStore(connectionB);
        var options = new CachingOptions { Namespace = ns, PayloadVersion = "2" };
        await using var channel = new RedisCacheInvalidationChannel(connectionA, options);
        var serializer = new SystemTextJsonCacheValueSerializer(
            new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }
        );
        await using var a = new TieredCache(options, storeA, serializer, channel);
        await using var b = new TieredCache(
            new CachingOptions { Namespace = ns, PayloadVersion = "2" },
            storeB,
            serializer
        );
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport == RedisInvalidationTransport.Tracking)
        );
        var entry = new CacheEntryOptions(TimeSpan.FromMinutes(1));
        try
        {
            switch (population)
            {
                case "factory":
                    Assert.Equal(
                        1,
                        await a.GetOrCreateAsync(
                            "catalog",
                            _ => ValueTask.FromResult(1),
                            entry.Expiration,
                            Token
                        )
                    );
                    break;
                case "absent":
                    Assert.True(await a.SetIfAbsentAsync("catalog", 1, entry, Token));
                    break;
                case "warmup":
                    await a.WarmupAsync(
                        new Dictionary<string, int> { ["catalog"] = 1 },
                        entry.Expiration,
                        cancellationToken: Token
                    );
                    break;
                case "rewrite":
                    await a.SetAsync("catalog", 0, entry, Token);
                    Assert.NotNull(await storeA.GetAsync($"{ns}:cache:data:catalog:2", Token));
                    await a.SetAsync("catalog", 1, entry, Token);
                    break;
                default:
                    await a.SetAsync("catalog", 1, entry, Token);
                    break;
            }

            Assert.Equal(CacheTier.L1, (await a.TryGetAsync<int>("catalog", Token)).Tier);
            await b.SetAsync("catalog", 2, entry, Token);

            await CacheConformance.WaitUntilAsync(async () =>
                (await a.TryGetAsync<int>("catalog", Token)).Value == 2
            );
        }
        finally
        {
            await storeA.RemoveAsync([$"{ns}:cache:data:catalog:2"], Token);
        }
    }

    [Theory(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(false, false)]
    [InlineData(true, false)]
    [InlineData(false, true)]
    [InlineData(true, true)]
    public async Task TagRemoval_PreservesConcurrentMembersAndCanRetryPartialFailure(
        bool hashTags,
        bool failSecondBatch
    )
    {
        var ns = "regression-" + Guid.NewGuid().ToString("N");
        await using var connection = Connection(hashTags);
        await using var writer = new RedisCacheStore(connection);
        var db = await connection.GetDatabaseAsync(Token);
        var tag = $"{ns}:cache:tag:catalog";
        var added = $"{ns}:cache:data:added";
        var keys = Enumerable.Range(0, 501).Select(i => $"{ns}:cache:data:catalog-{i}").ToArray();
        var ttl = TimeSpan.FromMinutes(1);
        await writer.SetManyAsync(
            keys.Select(key => new KeyValuePair<string, ReadOnlyMemory<byte>>(
                    key,
                    new byte[] { 1 }
                ))
                .ToArray(),
            ttl,
            Token
        );
        await db.SetAddAsync(
            RedisKeys.ToRedisKey(tag, hashTags),
            keys.Select(key => (RedisValue)(byte[])RedisKeys.ToRedisKey(key, hashTags)!).ToArray()
        );
        await db.KeyExpireAsync(RedisKeys.ToRedisKey(tag, hashTags), ttl);

        // Forward to the real server, inserting a completed writer after the remover's snapshot.
        var proxy = Substitute.For<IDatabase>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(proxy);
        proxy
            .SetMembersAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>())
            .Returns(async call =>
            {
                var members = await db.SetMembersAsync(call.ArgAt<RedisKey>(0));
                await writer.SetAsync(added, new byte[] { 2 }, ttl, [tag], Token);
                return members;
            });
        var scripts = 0;
        proxy
            .ScriptEvaluateAsync(
                Arg.Any<string>(),
                Arg.Any<RedisKey[]>(),
                Arg.Any<RedisValue[]>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(call =>
            {
                if (++scripts == 2 && failSecondBatch)
                    return Task.FromException<RedisResult>(
                        new TimeoutException("injected batch failure")
                    );
                return db.ScriptEvaluateAsync(
                    call.ArgAt<string>(0),
                    call.ArgAt<RedisKey[]>(1),
                    call.ArgAt<RedisValue[]>(2)
                );
            });
        // The previous implementation uses these calls; forwarding makes the regression fail
        // on the lost real membership rather than merely on an unexpected mock invocation.
        proxy
            .KeyDeleteAsync(Arg.Any<RedisKey[]>(), Arg.Any<CommandFlags>())
            .Returns(call => db.KeyDeleteAsync(call.ArgAt<RedisKey[]>(0)));
        proxy
            .KeyDeleteAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>())
            .Returns(call => db.KeyDeleteAsync(call.ArgAt<RedisKey>(0)));
        await using var remover = new RedisCacheStore(
            mux,
            new RedisOptions { UseHashTags = hashTags }
        );
        try
        {
            if (failSecondBatch)
            {
                await Assert.ThrowsAsync<CacheStoreException>(() =>
                    remover.RemoveByTagAsync(tag, Token).AsTask()
                );
            }
            else
            {
                await remover.RemoveByTagAsync(tag, Token);
            }

            Assert.NotNull(await writer.GetAsync(added, Token));
            Assert.True(
                await db.SetContainsAsync(
                    RedisKeys.ToRedisKey(tag, hashTags),
                    (byte[])RedisKeys.ToRedisKey(added, hashTags)!
                )
            );
            Assert.Equal(
                failSecondBatch ? 2 : 1,
                await db.SetLengthAsync(RedisKeys.ToRedisKey(tag, hashTags))
            );
            await writer.RemoveByTagAsync(tag, Token);
            Assert.Null(await writer.GetAsync(added, Token));
            Assert.False(await db.KeyExistsAsync(RedisKeys.ToRedisKey(tag, hashTags)));
            Assert.Empty(await writer.GetManyAsync(keys, Token));
        }
        finally
        {
            await writer.RemoveAsync([.. keys, added, tag], Token);
        }
    }

    [Theory(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AtomicReads_PreserveBinaryValuesTtlAndMisses(bool hashTags)
    {
        var ns = "regression-" + Guid.NewGuid().ToString("N");
        var key = $"{ns}:cache:data:catalog";
        var persistent = $"{ns}:cache:data:persistent";
        var expired = $"{ns}:cache:data:expired";
        await using var connection = Connection(hashTags);
        await using var store = new RedisCacheStore(connection);
        var db = await connection.GetDatabaseAsync(Token);
        byte[] payload = [0, 255, 1, 0];
        try
        {
            await store.SetAsync(key, payload, TimeSpan.FromSeconds(10), cancellationToken: Token);
            await db.StringSetAsync(
                RedisKeys.ToRedisKey(persistent, hashTags),
                Array.Empty<byte>()
            );
            await store.SetAsync(
                expired,
                payload,
                TimeSpan.FromSeconds(10),
                cancellationToken: Token
            );
            await db.KeyExpireAsync(RedisKeys.ToRedisKey(expired, hashTags), TimeSpan.Zero);
            var single = await store.GetAsync(key, Token);
            var bulk = await store.GetManyAsync([key, persistent, expired], Token);

            Assert.NotNull(single);
            Assert.Equal(payload, single.Value.Payload.ToArray());
            Assert.Equal(payload, bulk[key].Payload.ToArray());
            Assert.InRange(
                bulk[key].RemainingTimeToLive!.Value,
                TimeSpan.FromMilliseconds(1),
                TimeSpan.FromSeconds(10)
            );
            Assert.Null(bulk[persistent].RemainingTimeToLive);
            Assert.Empty(bulk[persistent].Payload.ToArray());
            Assert.False(bulk.ContainsKey(expired));
            Assert.Null(await store.GetAsync(expired, Token));
        }
        finally
        {
            await store.RemoveAsync([key, persistent, expired], Token);
        }
    }

    private static RedisConnection Connection(bool hashTags) =>
        new(
            new RedisOptions
            {
                Configuration = RedisAvailability.Configuration,
                UseHashTags = hashTags,
            }
        );
}
