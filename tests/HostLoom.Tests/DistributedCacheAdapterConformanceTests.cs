using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Caching.Testing;
using HostLoom.Conformance;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

public sealed class DistributedCacheAdapterConformanceTests
{
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

    [Theory]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnTheInMemoryStore(string scenario)
    {
        var clock = new ManualTimeProvider();
        var store = new InMemoryDistributedCacheStore(clock);
        var faults = new FaultingCacheStore(store);
        var ns = "adapter-conformance-" + Guid.NewGuid().ToString("N")[..8];
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services.AddSingleton<TimeProvider>(clock);
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
            Cache =
                provider.GetRequiredService<Microsoft.Extensions.Caching.Distributed.IDistributedCache>(),
            BufferedCache =
                provider.GetRequiredService<Microsoft.Extensions.Caching.Distributed.IBufferDistributedCache>(),
            Faults = faults,
            Clock = new ManualConformanceClock(clock),
            Namespace = ns,
        };

        await DistributedCacheConformance
            .Scenarios[scenario](fixture)
            .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
    }
}
