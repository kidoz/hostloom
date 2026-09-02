using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Caching.Testing;
using HostLoom.Conformance;
using HostLoom.Redis;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Runs the same cache scenarios the unit suite runs on the in-process backends against a real
/// Redis, composed with <c>new</c> and through the container, on the wall clock.
/// </summary>
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

            return data;
        }
    }

    [Theory(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnRedis(string scenario, string composition)
    {
        var ns = "conf-" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new RedisConnection(
            new RedisOptions
            {
                Configuration = RedisAvailability.Configuration,
                ClientName = "hostloom-conformance",
            }
        );
        var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = ns }
        );
        await using (channel)
        {
            await using var store = new RedisCacheStore(connection);
            var faults = new FaultingCacheStore(store, channel);
            var fixture = new CacheConformanceFixture
            {
                Clock = new RealConformanceClock(),
                Faults = faults,
                CreateCache =
                    composition == "new"
                        ? () => new TieredCache(Options(ns), faults, Serializer())
                        : () => FromContainer(ns, faults),
            };

            await CacheConformance.Scenarios[scenario](fixture);
        }
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

    private static ICache FromContainer(string ns, FaultingCacheStore faults)
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
        return services.BuildServiceProvider().GetRequiredService<ICache>();
    }
}
