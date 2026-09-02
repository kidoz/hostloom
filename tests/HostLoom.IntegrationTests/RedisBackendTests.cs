using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Conformance;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Redis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// What only a real Redis can prove about the backend package: the key layout on the server, the
/// shared connection through the builders, readiness, cross-connection invalidation, and
/// re-subscription after the server drops the pub/sub connection.
/// </summary>
[Collection(nameof(RedisBackendTests))]
[CollectionDefinition(nameof(RedisBackendTests), DisableParallelization = true)]
public sealed class RedisBackendTests
{
    public static bool Available => RedisAvailability.Redis;

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static JsonSerializerOptions Json() =>
        new() { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    private static string Namespace() => "it-" + Guid.NewGuid().ToString("N")[..8];

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task UseRedis_OnBothBuilders_SharesOneConnectionAndReportsReady()
    {
        var ns = Namespace();
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoomCaching(caching => caching.Namespace = ns)
            .UseRedis(options => options.Configuration = RedisAvailability.Configuration)
            .UseSystemTextJson(Json())
            .AddHealthChecks();
        builder
            .Services.AddHostLoomLocking(locking => locking.Namespace = ns)
            .UseRedis()
            .AddHealthChecks();
        using var host = builder.Build();

        await host.StartAsync(Token);
        var cache = host.Services.GetRequiredService<ICache>();
        var value = await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(7),
            TimeSpan.FromMinutes(1),
            Token
        );
        var locked = await host
            .Services.GetRequiredService<IDistributedLock>()
            .ExecuteWithLockAsync(
                "k",
                static _ => ValueTask.FromResult(true),
                cancellationToken: Token
            );
        var readiness = await host
            .Services.GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(r => r.Tags.Contains("ready"), Token);
        var connection = host.Services.GetRequiredService<RedisConnection>();
        var description = CachingProbe.Describe(cache);
        await host.StopAsync(Token);

        Assert.Equal(7, value);
        Assert.True(locked);
        Assert.True(connection.IsConnected);
        Assert.Equal(HealthStatus.Healthy, readiness.Status);
        Assert.Equal(2, readiness.Entries.Count);
        Assert.Equal(nameof(RedisCacheStore), description.Store);
        Assert.StartsWith("channel", description.Invalidation, StringComparison.Ordinal);
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Store_UsesTheKeyDomainsWithTimeToLiveAndTagIndexes()
    {
        var ns = Namespace();
        await using var connection = Connection();
        await using var store = new RedisCacheStore(connection);
        await using var cache = new TieredCache(
            new CachingOptions { Namespace = ns },
            store,
            Serializer()
        );
        var db = (await connection.GetMultiplexerAsync(Token)).GetDatabase();

        await cache.SetAsync(
            "k",
            new Payload("v"),
            new CacheEntryOptions(TimeSpan.FromSeconds(60)) { Tags = ["t"] },
            Token
        );

        var dataTtl = await db.KeyTimeToLiveAsync($"{ns}:cache:data:k");
        var tagTtl = await db.KeyTimeToLiveAsync($"{ns}:cache:tag:t");
        var members = await db.SetMembersAsync($"{ns}:cache:tag:t");
        Assert.NotNull(dataTtl);
        Assert.InRange(dataTtl.Value, TimeSpan.FromSeconds(50), TimeSpan.FromSeconds(60));
        Assert.NotNull(tagTtl);
        Assert.InRange(tagTtl.Value, TimeSpan.FromSeconds(50), TimeSpan.FromSeconds(60));
        Assert.Equal($"{ns}:cache:data:k", (string?)Assert.Single(members));

        await cache.RemoveByTagAsync("t", Token);

        Assert.False(await db.KeyExistsAsync($"{ns}:cache:data:k"));
        Assert.False(await db.KeyExistsAsync($"{ns}:cache:tag:t"));
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task HashTags_WrapTheNamespaceForClusterSlotAffinity()
    {
        var ns = Namespace();
        await using var connection = Connection(options => options.UseHashTags = true);
        await using var store = new RedisCacheStore(connection);
        await using var cache = new TieredCache(
            new CachingOptions { Namespace = ns },
            store,
            Serializer()
        );
        var db = (await connection.GetMultiplexerAsync(Token)).GetDatabase();

        await cache.SetAsync(
            "k",
            new Payload("v"),
            new CacheEntryOptions(TimeSpan.FromSeconds(30)),
            Token
        );

        Assert.True(await db.KeyExistsAsync($"{{{ns}}}:cache:data:k"));
        Assert.False(await db.KeyExistsAsync($"{ns}:cache:data:k"));
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Lock_UsesTheLockDomainWithAnOwnerTokenAndLease()
    {
        var ns = Namespace();
        await using var connection = Connection();
        await using var provider = new RedisLockProvider(connection);
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = ns },
            provider
        );
        var db = (await connection.GetMultiplexerAsync(Token)).GetDatabase();

        var handle = await locks.TryAcquireAsync(
            "job",
            new LockOptions { Lease = TimeSpan.FromSeconds(20) },
            Token
        );
        Assert.NotNull(handle);
        var owner = await db.StringGetAsync($"{ns}:lock:job");
        var ttl = await db.KeyTimeToLiveAsync($"{ns}:lock:job");

        Assert.False(owner.IsNull);
        Assert.NotNull(ttl);
        Assert.InRange(ttl.Value, TimeSpan.FromSeconds(10), TimeSpan.FromSeconds(20));
        Assert.False(await provider.ReleaseAsync($"{ns}:lock:job", "someone-else", Token));
        Assert.True(await db.KeyExistsAsync($"{ns}:lock:job"));

        await handle.DisposeAsync();

        Assert.False(await db.KeyExistsAsync($"{ns}:lock:job"));
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Invalidation_CrossesConnectionsOverPubSub()
    {
        var ns = Namespace();
        await using var connectionA = Connection();
        await using var connectionB = Connection();
        var channelA = new RedisCacheInvalidationChannel(
            connectionA,
            new CachingOptions { Namespace = ns }
        );
        var channelB = new RedisCacheInvalidationChannel(
            connectionB,
            new CachingOptions { Namespace = ns }
        );
        await using (channelA)
        await using (channelB)
        {
            await using var storeA = new RedisCacheStore(connectionA);
            await using var storeB = new RedisCacheStore(connectionB);
            await using var a = new TieredCache(
                new CachingOptions { Namespace = ns },
                storeA,
                Serializer(),
                channelA
            );
            await using var b = new TieredCache(
                new CachingOptions { Namespace = ns },
                storeB,
                Serializer(),
                channelB
            );
            await CacheConformance.WaitUntilAsync(() => Task.FromResult(channelB.IsSubscribed));

            await a.SetAsync(
                "shared",
                new Payload("v1"),
                new CacheEntryOptions(TimeSpan.FromMinutes(1)),
                Token
            );
            Assert.Equal(CacheTier.L2, (await b.TryGetAsync<Payload>("shared", Token)).Tier);
            Assert.Equal(CacheTier.L1, (await b.TryGetAsync<Payload>("shared", Token)).Tier);

            await a.RemoveAsync("shared", Token);

            await CacheConformance.WaitUntilAsync(async () =>
                !(await b.TryGetAsync<Payload>("shared", Token)).Found
            );
        }
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Subscription_SurvivesTheServerKillingThePubSubConnection()
    {
        var ns = Namespace();
        await using var publisher = Connection();
        await using var subscriber = Connection(options =>
            options.ClientName = "hostloom-it-victim"
        );
        var publisherChannel = new RedisCacheInvalidationChannel(
            publisher,
            new CachingOptions { Namespace = ns }
        );
        var subscriberChannel = new RedisCacheInvalidationChannel(
            subscriber,
            new CachingOptions { Namespace = ns }
        );
        await using (publisherChannel)
        await using (subscriberChannel)
        {
            var received = 0;
            using var subscription = subscriberChannel.Subscribe(_ =>
                Interlocked.Increment(ref received)
            );
            await CacheConformance.WaitUntilAsync(() =>
                Task.FromResult(subscriberChannel.IsSubscribed)
            );
            await publisherChannel.PublishAsync(new CacheInvalidation(["before"], []), Token);
            await CacheConformance.WaitUntilAsync(() =>
                Task.FromResult(Volatile.Read(ref received) >= 1)
            );

            // An admin connection tells the server to drop the victim's pub/sub connection.
            await using var admin = new RedisConnection(
                new RedisOptions
                {
                    Configuration = RedisAvailability.Configuration + ",allowAdmin=true",
                }
            );
            var multiplexer = await admin.GetMultiplexerAsync(Token);
            var server = multiplexer.GetServer(multiplexer.GetEndPoints()[0]);
            var killed = await server.ClientKillAsync(
                new ClientKillFilter().WithClientType(ClientType.PubSub)
            );
            Assert.True(killed >= 1);

            await CacheConformance.WaitUntilAsync(
                () => Task.FromResult(subscriber.Reconnects >= 1),
                30
            );
            await CacheConformance.WaitUntilAsync(
                async () =>
                {
                    try
                    {
                        await publisherChannel.PublishAsync(
                            new CacheInvalidation(["after"], []),
                            Token
                        );
                    }
                    catch (RedisException)
                    {
                        return false;
                    }

                    return Volatile.Read(ref received) >= 2;
                },
                30
            );
        }
    }

    private static RedisConnection Connection(Action<RedisOptions>? configure = null)
    {
        var options = new RedisOptions
        {
            Configuration = RedisAvailability.Configuration,
            ClientName = "hostloom-integration",
        };
        configure?.Invoke(options);
        return new RedisConnection(options);
    }

    private static SystemTextJsonCacheValueSerializer Serializer() => new(Json());

    private sealed record Payload(string Text);
}
