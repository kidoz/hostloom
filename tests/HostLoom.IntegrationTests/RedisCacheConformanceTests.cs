using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Caching.Testing;
using HostLoom.Conformance;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
using Microsoft.Extensions.DependencyInjection;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Runs the same cache scenarios the unit suite runs on the in-process backends against a real
/// Redis, composed with <c>new</c> and through the container, on the wall clock. The invalidation
/// scenarios also run with a channel that does not flush on reconnect, and under keyspace
/// broadcast.
/// </summary>
/// <remarks>
/// Every instance of a composition shares one connection and one channel, as the caches of one
/// process do. Tracking registers <c>NOLOOP</c> on that connection, so under <c>Auto</c> it reports
/// none of these instances' writes to the others and they see only removals, as with the explicit
/// channel alone; tracking between separate connections is covered by
/// <see cref="RedisInvalidationModeTests"/>. Broadcast has no <c>NOLOOP</c> and reports every write
/// to every instance, the writer included. A restored subscription is simulated by killing the
/// channel's own pub/sub client, which the multiplexer reconnects.
/// </remarks>
[Collection(nameof(RedisCacheConformanceTests))]
[CollectionDefinition(nameof(RedisCacheConformanceTests), DisableParallelization = true)]
public sealed class RedisCacheConformanceTests
{
    public static bool Available => RedisAvailability.Redis;

    public static TheoryData<string, string> Scenarios
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (var scenario in CacheConformance.Scenarios.Keys)
            {
                data.Add(scenario, "new");
                data.Add(scenario, "container");
            }

            foreach (var scenario in CacheConformance.InvalidationScenarios.Keys)
            {
                data.Add(scenario, "no-flush");
                data.Add(scenario, "broadcast");
            }

            return data;
        }
    }

    [Theory(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnRedis(string scenario, string composition)
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "conf-" + Guid.NewGuid().ToString("N")[..8];
        var broadcast = composition == "broadcast";
        var channelOptions = new CachingOptions { Namespace = ns };
        channelOptions.Invalidation.FlushLocalOnReconnect = composition != "no-flush";
        if (broadcast)
        {
            channelOptions.Invalidation.Mode = CacheInvalidationMode.Broadcast;
        }

        await using var connection = new RedisConnection(
            new RedisOptions
            {
                Configuration = RedisAvailability.Configuration,
                // Unique, so tracking finds this connection's own subscriber and a simulated
                // delivery gap kills no other test's connection.
                ClientName = "hostloom-conformance-" + ns,
            }
        );
        var channel = new RedisCacheInvalidationChannel(connection, channelOptions);
        await using (channel)
        {
            // Establish the subscription and its transport before any instance publishes.
            using (channel.Subscribe(static _ => { }))
            {
                await CacheConformance.WaitUntilAsync(
                    () => Task.FromResult(channel.Transport != RedisInvalidationTransport.Pending),
                    30
                );
            }

            if (broadcast)
            {
                Assert.Equal(RedisInvalidationTransport.Broadcast, channel.Transport);
            }

            await using var store = new RedisCacheStore(connection);
            var faults = new FaultingCacheStore(store, channel);
            var containers = new List<ServiceProvider>();
            var instances = new List<IAsyncDisposable>();
            var fixture = new CacheConformanceFixture
            {
                Clock = new RealConformanceClock(),
                Namespace = ns,
                Faults = faults,
                ReportsStoreWrites = broadcast,
                ReportsOwnStoreWrites = broadcast,
                FlushLocalOnReconnect = channelOptions.Invalidation.FlushLocalOnReconnect,
                RestoreSubscriptionAsync = () =>
                    RestoreSubscriptionAsync(connection, channel, token),
                CreateCache =
                    composition == "container"
                        ? () => FromContainer(ns, faults, containers)
                        : () =>
                            Track(new TieredCache(Options(ns), faults, Serializer()), instances),
            };

            try
            {
                await CacheConformance
                    .Scenarios[scenario](fixture)
                    .WaitAsync(TimeSpan.FromSeconds(90), token);
            }
            finally
            {
                foreach (var instance in instances)
                {
                    await instance.DisposeAsync();
                }

                foreach (var container in containers)
                {
                    await container.DisposeAsync();
                }
            }
        }
    }

    /// <summary>
    /// Kills the connection's pub/sub client and returns once the multiplexer has reported the
    /// restored subscription connection and the explicit channel is subscribed on the server
    /// again. The channel hands its flush to the instances in the same restore notification,
    /// ahead of this method's handler, so anything published afterwards arrives after it.
    /// </summary>
    private static async Task RestoreSubscriptionAsync(
        RedisConnection connection,
        RedisCacheInvalidationChannel channel,
        CancellationToken token
    )
    {
        var restored = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        void OnRestored(object? sender, EventArgs args) => restored.TrySetResult();
        connection.SubscriptionRestored += OnRestored;
        try
        {
            var multiplexer = await connection.GetMultiplexerAsync(token);
            var server = multiplexer.GetServer(multiplexer.GetEndPoints()[0]);
            var clients = await server
                .ExecuteAsync("CLIENT", "LIST", "TYPE", "pubsub")
                .WaitAsync(token);
            var subscriber = RedisInvalidationDecoder.FindSubscriberClientId(
                (string?)clients,
                connection.ClientName,
                out var matches
            );
            Assert.Equal(1, matches);
            await server.ClientKillAsync(new ClientKillFilter().WithId(subscriber!.Value));

            await restored.Task.WaitAsync(TimeSpan.FromSeconds(30), token);
            var explicitChannel = RedisChannel.Literal(channel.ChannelName);
            await CacheConformance.WaitUntilAsync(
                async () => await server.SubscriptionSubscriberCountAsync(explicitChannel) > 0,
                30
            );
        }
        finally
        {
            connection.SubscriptionRestored -= OnRestored;
        }
    }

    private static TieredCache Track(TieredCache cache, List<IAsyncDisposable> instances)
    {
        instances.Add(cache);
        return cache;
    }

    private static CachingOptions Options(string ns)
    {
        var options = new CachingOptions { Namespace = ns };
        // The stampede lease is a real round trip here; keep the re-check pause short.
        options.Stampede.WaitBeforeFallback = TimeSpan.FromMilliseconds(20);
        return options;
    }

    private static SystemTextJsonCacheValueSerializer Serializer() =>
        new(new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() });

    private static ICache FromContainer(
        string ns,
        FaultingCacheStore faults,
        List<ServiceProvider> containers
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services
            .AddHostLoomCaching(caching =>
            {
                caching.Namespace = ns;
                caching.Stampede.WaitBeforeFallback = TimeSpan.FromMilliseconds(20);
            })
            .UseStore<FaultingCacheStore>("Faulting")
            .UseSystemTextJson(
                new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }
            );
        var container = services.BuildServiceProvider();
        containers.Add(container);
        return container.GetRequiredService<ICache>();
    }
}
