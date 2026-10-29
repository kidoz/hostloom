using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Caching.Testing;
using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Valkey;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.IntegrationTests;

[Collection(nameof(ValkeyDistributedCacheConformanceTests))]
[CollectionDefinition(
    nameof(ValkeyDistributedCacheConformanceTests),
    DisableParallelization = true
)]
public sealed class ValkeyDistributedCacheConformanceTests
{
    public static bool Available => ValkeyAvailability.Available;

    public static TheoryData<string> Scenarios
    {
        get
        {
            var data = new TheoryData<string>();
            foreach (var scenario in DistributedCacheConformance.Scenarios.Keys)
            {
                data.Add(scenario);
            }

            return data;
        }
    }

    [Theory(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnValkey(string scenario)
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "adapter-valkey-" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new ValkeyConnection(ValkeyAvailability.Options());
        var store = new ValkeyCacheStore(connection);
        var faults = new FaultingCacheStore(store);
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services.AddSingleton(TimeProvider.System);
        services
            .AddHostLoomCaching(caching => caching.Namespace = ns)
            .UseStore<FaultingCacheStore>("Faulting")
            .UseSystemTextJson(
                new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }
            )
            .AddDistributedCacheAdapter(enableSlidingExpiration: true);
        await using var provider = services.BuildServiceProvider();
        var fixture = new DistributedCacheConformanceFixture
        {
            Cache = provider.GetRequiredService<IDistributedCache>(),
            BufferedCache = provider.GetRequiredService<IBufferDistributedCache>(),
            Faults = faults,
            Clock = new RealConformanceClock(),
            Namespace = ns,
        };

        await DistributedCacheConformance
            .Scenarios[scenario](fixture)
            .WaitAsync(TimeSpan.FromSeconds(60), token);
    }
}
