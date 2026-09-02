using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Conformance;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// The server-side invalidation transports against a real Redis: automatic mode selection,
/// client tracking reporting another connection's writes while ignoring this connection's own,
/// keyspace notifications reporting expiry, re-initialisation of tracking after the server drops
/// the connections, and the end-to-end effect on a second instance's in-process tier.
/// </summary>
[Collection(nameof(RedisInvalidationModeTests))]
[CollectionDefinition(nameof(RedisInvalidationModeTests), DisableParallelization = true)]
public sealed class RedisInvalidationModeTests
{
    public static bool Available => RedisAvailability.Redis;

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static string Namespace() => "mode-" + Guid.NewGuid().ToString("N")[..8];

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Auto_OnRedis7_ResolvesToTracking()
    {
        var ns = Namespace();
        await using var connection = Connection();
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = ns }
        );

        using var subscription = channel.Subscribe(_ => { });
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport != RedisInvalidationTransport.Pending)
        );

        Assert.Equal(RedisInvalidationTransport.Tracking, channel.Transport);
        Assert.Equal(1, channel.TrackingInitialisations);
        Assert.Contains("tracking", channel.ToString(), StringComparison.Ordinal);
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Tracking_ReportsAnotherConnectionsWriteAndIgnoresItsOwn()
    {
        var ns = Namespace();
        await using var writer = Connection();
        await using var reader = Connection();
        await using var writerStore = new RedisCacheStore(writer);
        await using var readerStore = new RedisCacheStore(reader);
        await using var channel = new RedisCacheInvalidationChannel(
            reader,
            new CachingOptions { Namespace = ns }.WithMode(CacheInvalidationMode.Tracking)
        );
        var received = new List<string>();
        using var subscription = channel.Subscribe(invalidation =>
        {
            lock (received)
            {
                received.AddRange(invalidation.Keys);
            }
        });
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport == RedisInvalidationTransport.Tracking)
        );
        var dataKey = $"{ns}:cache:data:k";
        await writerStore.SetAsync(dataKey, new byte[] { 1 }, TimeSpan.FromMinutes(1), null, Token);

        // The reader's own read registers the key; its own write must not report back (NOLOOP).
        Assert.NotNull(await readerStore.GetAsync(dataKey, Token));
        await readerStore.SetAsync(dataKey, new byte[] { 2 }, TimeSpan.FromMinutes(1), null, Token);
        await Task.Delay(500, Token);
        int afterOwnWrite;
        lock (received)
        {
            afterOwnWrite = received.Count;
        }

        Assert.NotNull(await readerStore.GetAsync(dataKey, Token));
        await writerStore.SetAsync(dataKey, new byte[] { 3 }, TimeSpan.FromMinutes(1), null, Token);
        await CacheConformance.WaitUntilAsync(() =>
        {
            lock (received)
            {
                return Task.FromResult(received.Contains("k"));
            }
        });

        Assert.Equal(0, afterOwnWrite);
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Broadcast_ReportsExpiryThroughKeyspaceNotifications()
    {
        var ns = Namespace();
        await using var writer = Connection();
        await using var reader = Connection();
        await using var writerStore = new RedisCacheStore(writer);
        await using var channel = new RedisCacheInvalidationChannel(
            reader,
            new CachingOptions { Namespace = ns }.WithMode(CacheInvalidationMode.Broadcast)
        );
        var received = new List<string>();
        using var subscription = channel.Subscribe(invalidation =>
        {
            lock (received)
            {
                received.AddRange(invalidation.Keys);
            }
        });
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport != RedisInvalidationTransport.Pending)
        );
        Assert.Equal(RedisInvalidationTransport.Broadcast, channel.Transport);

        await writerStore.SetAsync(
            $"{ns}:cache:data:short",
            new byte[] { 1 },
            TimeSpan.FromMilliseconds(500),
            null,
            Token
        );

        await CacheConformance.WaitUntilAsync(
            () =>
            {
                lock (received)
                {
                    return Task.FromResult(received.Contains("short"));
                }
            },
            15
        );
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Tracking_IsReinitialisedAfterTheServerDropsTheConnections()
    {
        var ns = Namespace();
        await using var writer = Connection();
        await using var reader = Connection(options => options.ClientName = "hostloom-it-tracked");
        await using var writerStore = new RedisCacheStore(writer);
        await using var readerStore = new RedisCacheStore(reader);
        await using var channel = new RedisCacheInvalidationChannel(
            reader,
            new CachingOptions { Namespace = ns }.WithMode(CacheInvalidationMode.Tracking)
        );
        var received = new List<string>();
        using var subscription = channel.Subscribe(invalidation =>
        {
            lock (received)
            {
                received.AddRange(invalidation.Keys);
            }
        });
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport == RedisInvalidationTransport.Tracking)
        );
        var dataKey = $"{ns}:cache:data:k";
        await writerStore.SetAsync(dataKey, new byte[] { 1 }, TimeSpan.FromMinutes(1), null, Token);

        // Kill both of the reader's connections; StackExchange.Redis reconnects, and tracking
        // has to be registered again on the new interactive connection.
        await using var admin = new RedisConnection(
            new RedisOptions
            {
                Configuration = RedisAvailability.Configuration + ",allowAdmin=true",
            }
        );
        var multiplexer = await admin.GetMultiplexerAsync(Token);
        var server = multiplexer.GetServer(multiplexer.GetEndPoints()[0]);
        var list = (string?)await multiplexer.GetDatabase().ExecuteAsync("CLIENT", "LIST");
        var victims = list!
            .Split('\n', StringSplitOptions.RemoveEmptyEntries)
            .Where(line => line.Contains($"name={reader.ClientName} ", StringComparison.Ordinal))
            .Select(line =>
                long.Parse(
                    line.Split(' ')[0]["id=".Length..],
                    System.Globalization.CultureInfo.InvariantCulture
                )
            )
            .ToList();
        Assert.True(victims.Count >= 2);
        foreach (var id in victims)
        {
            await server.ClientKillAsync(new ClientKillFilter().WithId(id));
        }

        await CacheConformance.WaitUntilAsync(
            () => Task.FromResult(channel.TrackingInitialisations >= 2),
            30
        );

        // Tracked reads on the new connection report another connection's write again.
        await CacheConformance.WaitUntilAsync(
            async () =>
            {
                try
                {
                    await readerStore.GetAsync(dataKey, Token);
                    await writerStore.SetAsync(
                        dataKey,
                        new byte[] { 9 },
                        TimeSpan.FromMinutes(1),
                        null,
                        Token
                    );
                }
                catch (CacheStoreException)
                {
                    return false;
                }

                await Task.Delay(100, Token);
                lock (received)
                {
                    return received.Contains("k");
                }
            },
            30
        );
    }

    [Fact(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task Tracking_EvictsASecondInstancesInProcessEntryWhenTheFirstOverwritesIt()
    {
        var ns = Namespace();
        await using var connectionA = Connection();
        await using var connectionB = Connection();
        await using var storeA = new RedisCacheStore(connectionA);
        await using var storeB = new RedisCacheStore(connectionB);
        var options = () =>
            new CachingOptions { Namespace = ns }.WithMode(CacheInvalidationMode.Tracking);
        await using var channelA = new RedisCacheInvalidationChannel(connectionA, options());
        await using var channelB = new RedisCacheInvalidationChannel(connectionB, options());
        await using var a = new TieredCache(options(), storeA, Serializer(), channelA);
        await using var b = new TieredCache(options(), storeB, Serializer(), channelB);
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channelB.Transport == RedisInvalidationTransport.Tracking)
        );

        await a.SetAsync(
            "price",
            new Payload("v1"),
            new CacheEntryOptions(TimeSpan.FromMinutes(1)),
            Token
        );
        Assert.Equal(CacheTier.L2, (await b.TryGetAsync<Payload>("price", Token)).Tier);
        Assert.Equal(CacheTier.L1, (await b.TryGetAsync<Payload>("price", Token)).Tier);

        // A plain overwrite publishes nothing on the explicit channel; only tracking can tell B.
        await a.SetAsync(
            "price",
            new Payload("v2"),
            new CacheEntryOptions(TimeSpan.FromMinutes(1)),
            Token
        );

        await CacheConformance.WaitUntilAsync(async () =>
        {
            var lookup = await b.TryGetAsync<Payload>("price", Token);
            return lookup.Tier == CacheTier.L2 && lookup.Value!.Text == "v2";
        });
    }

    private static RedisConnection Connection(Action<RedisOptions>? configure = null)
    {
        var options = new RedisOptions
        {
            Configuration = RedisAvailability.Configuration,
            ClientName = "hostloom-modes",
        };
        configure?.Invoke(options);
        return new RedisConnection(options);
    }

    private static SystemTextJsonCacheValueSerializer Serializer() =>
        new(new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() });

    private sealed record Payload(string Text);
}

internal static class CachingOptionsExtensions
{
    public static CachingOptions WithMode(this CachingOptions options, CacheInvalidationMode mode)
    {
        options.Invalidation.Mode = mode;
        return options;
    }
}
