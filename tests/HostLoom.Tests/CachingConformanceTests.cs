using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Caching.Testing;
using HostLoom.Conformance;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Runs the backend-neutral cache scenarios on the in-process backends: composed with
/// <c>new</c>, composed through the container, and in-process only, so the
/// compositions are proven to behave the same.
/// </summary>
public sealed class CachingConformanceTests
{
    public static TheoryData<string, string> Scenarios
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (var scenario in CacheConformance.Scenarios.Keys)
            {
                data.Add(scenario, "new");
                data.Add(scenario, "container");
                data.Add(scenario, "l1-only");
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnTheInProcessBackends(string scenario, string composition)
    {
        var fixture = composition switch
        {
            "new" => ContainerFree(),
            "container" => Container(),
            _ => InProcessOnly(),
        };

        await CacheConformance.Scenarios[scenario](fixture);
    }

    private static CachingOptions Options() => new() { Namespace = "conformance" };

    private static JsonSerializerOptions Json() =>
        new() { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    private static CacheConformanceFixture ContainerFree()
    {
        var clock = new ManualTimeProvider();
        var faults = new FaultingCacheStore(new InMemoryDistributedCacheStore(clock));
        var serializer = new SystemTextJsonCacheValueSerializer(Json());
        return new CacheConformanceFixture
        {
            Clock = new ManualConformanceClock(clock),
            Faults = faults,
            CreateCache = () => new TieredCache(Options(), faults, serializer, timeProvider: clock),
        };
    }

    private static CacheConformanceFixture Container()
    {
        var clock = new ManualTimeProvider();
        var faults = new FaultingCacheStore(new InMemoryDistributedCacheStore(clock));
        return new CacheConformanceFixture
        {
            Clock = new ManualConformanceClock(clock),
            Faults = faults,
            CreateCache = () =>
            {
                // A fresh provider per instance is a fresh service instance over the shared store.
                var services = new ServiceCollection();
                services.AddSingleton(faults);
                services.AddSingleton<TimeProvider>(clock);
                services
                    .AddHostLoomCaching(caching => caching.Namespace = "conformance")
                    .UseStore<FaultingCacheStore>("Faulting")
                    .UseSystemTextJson(Json());
                return services.BuildServiceProvider().GetRequiredService<ICache>();
            },
        };
    }

    private static CacheConformanceFixture InProcessOnly()
    {
        var clock = new ManualTimeProvider();
        TieredCache? shared = null;
        return new CacheConformanceFixture
        {
            Clock = new ManualConformanceClock(clock),
            Faults = null,
            // Without a distributed tier every "instance" is the same process-local cache.
            CreateCache = () => shared ??= new TieredCache(Options(), timeProvider: clock),
        };
    }
}
