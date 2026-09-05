using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Caching.Testing;
using HostLoom.Conformance;
using HostLoom.Valkey;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Runs the same cache scenarios the unit suite runs on the in-process backends against a real
/// Valkey, composed with <c>new</c> and through the container, on the wall clock.
/// </summary>
[Collection(nameof(ValkeyCacheConformanceTests))]
[CollectionDefinition(nameof(ValkeyCacheConformanceTests), DisableParallelization = true)]
public sealed class ValkeyCacheConformanceTests
{
    public static bool Available => ValkeyAvailability.Available;

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

    [Theory(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnValkey(string scenario, string composition)
    {
        var ns = "conf-" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new ValkeyConnection(ValkeyAvailability.Options());
        var channel = new ValkeyCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = ns }
        );
        await using (channel)
        {
            var store = new ValkeyCacheStore(connection);
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await channel.StartAsync(timeout.Token);
            var faults = new FaultingCacheStore(store, channel);
            var containers = new List<ServiceProvider>();
            var instances = new List<IAsyncDisposable>();
            var fixture = new CacheConformanceFixture
            {
                Clock = new RealConformanceClock(),
                Faults = faults,
                CreateCache =
                    composition == "new"
                        ? () => Track(new TieredCache(Options(ns), faults, Serializer()), instances)
                        : () => FromContainer(ns, faults, containers),
            };

            try
            {
                await CacheConformance
                    .Scenarios[scenario](fixture)
                    .WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);
            }
            finally
            {
                foreach (var instance in instances)
                    await instance.DisposeAsync();
                foreach (var container in containers)
                    await container.DisposeAsync();
            }
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
