using HostLoom.Conformance;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Xunit;

namespace HostLoom.Tests;

/// <summary>Runs shared token-bucket scenarios on the in-process store.</summary>
public sealed class RateLimitingConformanceTests
{
    public static TheoryData<string, string> Scenarios
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (var scenario in RateLimitConformance.Scenarios.Keys)
            {
                data.Add(scenario, "new");
                data.Add(scenario, "container");
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnTheInProcessStore(string scenario, string composition)
    {
        var clock = new ManualTimeProvider();
        var store = new InMemoryRateLimitStore(clock);
        var containers = new List<ServiceProvider>();
        var fixture = new RateLimitConformanceFixture
        {
            Clock = new ManualConformanceClock(clock),
            CreateLimiter =
                composition == "new"
                    ? (key, policy) =>
                        new DistributedRateLimiter(
                            new RateLimitingOptions
                            {
                                Namespace = "conformance",
                                TokenBucket = policy,
                            },
                            key,
                            store,
                            clock
                        )
                    : (key, policy) =>
                    {
                        var services = new ServiceCollection();
                        services.AddSingleton(store);
                        services.AddSingleton<TimeProvider>(clock);
                        services
                            .AddHostLoomRateLimiting(
                                key,
                                options =>
                                {
                                    options.Namespace = "conformance";
                                    options.TokenBucket = policy;
                                }
                            )
                            .UseInMemory();
                        services.Replace(
                            ServiceDescriptor.KeyedSingleton<InMemoryRateLimitStore>(key, store)
                        );
                        var container = services.BuildServiceProvider();
                        containers.Add(container);
                        return container.GetRequiredKeyedService<DistributedRateLimiter>(key);
                    },
        };

        try
        {
            await RateLimitConformance.Scenarios[scenario](fixture);
        }
        finally
        {
            foreach (var container in containers)
            {
                await container.DisposeAsync();
            }
        }
    }
}
