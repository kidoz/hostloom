using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// What the writing instance sees of its own writes on the compose Redis. Keyspace notifications
/// have no <c>NOLOOP</c>: under broadcast a write comes back to its writer as an invalidation and
/// evicts the in-process entry it has just written, and a removal reaches its remover twice, as the
/// keyspace event and as the explicit echo, of which only one is recognised as the echo. Tracking
/// registers <c>NOLOOP</c>, so the writer keeps its entry.
/// </summary>
[Collection(nameof(RedisCacheWriteVisibilityTests))]
[CollectionDefinition(nameof(RedisCacheWriteVisibilityTests), DisableParallelization = true)]
public sealed class RedisCacheWriteVisibilityTests
{
    private const string Invalidations = "hostloom.cache.invalidations";
    private static readonly (string, string) Received = ("hostloom.cache.direction", "received");
    private static readonly (string, string) Echoed = ("hostloom.cache.direction", "echoed");

    public static bool Available => RedisAvailability.Redis;

    [Fact(Timeout = 30_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Broadcast_EvictsTheWritersOwnInProcessEntryOnEveryWrite()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = Namespace();
        using var metrics = Metrics(ns);
        await using var connection = Connection();
        var options = Options(ns, CacheInvalidationMode.Broadcast);
        await using var channel = new RedisCacheInvalidationChannel(connection, options);
        await using var store = new RedisCacheStore(connection);
        await using var cache = new TieredCache(options, store, Serializer(), channel);
        await ReadyAsync(channel, RedisInvalidationTransport.Broadcast);

        for (var write = 1; write <= 3; write++)
        {
            var before = metrics.Sum(Invalidations, Received);
            await cache.SetAsync("catalog:eu", new Payload($"v{write}"), Entry(), token);

            // The write's own keyspace "set" event arrives as someone else's invalidation...
            await metrics.WaitForAsync(Invalidations, before + 1, token, Received);
            // ...so the writer reads its own value back from Redis rather than from process memory.
            var own = await cache.TryGetAsync<Payload>("catalog:eu", token);
            Assert.Equal(CacheTier.L2, own.Tier);
            Assert.Equal($"v{write}", own.Value!.Text);
            // A read raises no keyspace event, so the promotion it made stays.
            Assert.Equal(
                CacheTier.L1,
                (await cache.TryGetAsync<Payload>("catalog:eu", token)).Tier
            );
        }

        Assert.Equal(0, metrics.Sum(Invalidations, Echoed));
    }

    [Fact(Timeout = 30_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Broadcast_AppliesTheRemoversOwnRemovalOnceBesidesItsRecognisedEcho()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = Namespace();
        using var metrics = Metrics(ns);
        await using var connection = Connection();
        var options = Options(ns, CacheInvalidationMode.Broadcast);
        await using var channel = new RedisCacheInvalidationChannel(connection, options);
        await using var store = new RedisCacheStore(connection);
        await using var cache = new TieredCache(options, store, Serializer(), channel);
        await ReadyAsync(channel, RedisInvalidationTransport.Broadcast);
        await cache.SetAsync("catalog:eu", new Payload("v1"), Entry(), token);
        await metrics.WaitForAsync(Invalidations, 1, token, Received);

        await cache.RemoveAsync("catalog:eu", token);

        // The keyspace "del" event and the explicit echo name the same key. Whichever reaches the
        // queue first while the publish is remembered is taken for the echo; the other is applied.
        await metrics.WaitForAsync(Invalidations, 1, token, Echoed);
        await metrics.WaitForAsync(Invalidations, 2, token, Received);
        Assert.Equal(1, metrics.Sum(Invalidations, Echoed));
        Assert.False((await cache.TryGetAsync<Payload>("catalog:eu", token)).Found);
    }

    [Fact(Timeout = 30_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Tracking_KeepsTheWritersOwnInProcessEntry()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = Namespace();
        using var metrics = Metrics(ns);
        await using var connection = Connection();
        await using var other = Connection();
        var options = Options(ns, CacheInvalidationMode.Tracking);
        await using var channel = new RedisCacheInvalidationChannel(connection, options);
        await using var store = new RedisCacheStore(connection);
        await using var otherStore = new RedisCacheStore(other);
        await using var cache = new TieredCache(options, store, Serializer(), channel);
        await ReadyAsync(channel, RedisInvalidationTransport.Tracking);

        await cache.SetAsync("catalog:eu", new Payload("v1"), Entry(), token);
        // A write through another connection is reported on the same redirected stream after
        // anything this connection's own write could have produced, so once it is applied
        // nothing about the own write is still in flight.
        await otherStore.SetAsync(
            ns + ":cache:data:catalog:us",
            new byte[] { 1 },
            TimeSpan.FromMinutes(1),
            cancellationToken: token
        );
        await metrics.WaitForAsync(Invalidations, 1, token, Received);

        Assert.Equal(1, metrics.Sum(Invalidations, Received));
        var own = await cache.TryGetAsync<Payload>("catalog:eu", token);
        Assert.Equal(CacheTier.L1, own.Tier);
        Assert.Equal("v1", own.Value!.Text);
    }

    private static string Namespace() => "visibility-" + Guid.NewGuid().ToString("N")[..8];

    private static MeterCapture Metrics(string ns) =>
        new(CachingDiagnostics.MeterName, "hostloom.cache.namespace", ns);

    private static CacheEntryOptions Entry() => new(TimeSpan.FromMinutes(5));

    private static CachingOptions Options(string ns, CacheInvalidationMode mode) =>
        new CachingOptions { Namespace = ns }.WithMode(mode);

    private static RedisConnection Connection() =>
        new(
            new RedisOptions
            {
                Configuration = RedisAvailability.Configuration,
                ClientName = "hostloom-visibility",
            }
        );

    private static async Task ReadyAsync(
        RedisCacheInvalidationChannel channel,
        RedisInvalidationTransport transport
    )
    {
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport != RedisInvalidationTransport.Pending)
        );
        Assert.Equal(transport, channel.Transport);
    }

    private static SystemTextJsonCacheValueSerializer Serializer() =>
        new(new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() });

    private sealed record Payload(string Text);
}
