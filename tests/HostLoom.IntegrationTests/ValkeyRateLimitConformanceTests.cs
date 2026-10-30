using HostLoom.Conformance;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using HostLoom.Valkey;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>Runs shared token-bucket scenarios against real standalone Valkey.</summary>
[Collection(nameof(ValkeyRateLimitConformanceTests))]
[CollectionDefinition(nameof(ValkeyRateLimitConformanceTests), DisableParallelization = true)]
public sealed class ValkeyRateLimitConformanceTests
{
    public static bool Available => ValkeyAvailability.Available;

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

    [Theory(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnValkey(string scenario, string composition)
    {
        var ns = "rate-conf-" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new ValkeyConnection(ValkeyAvailability.Options());
        var store = new ValkeyRateLimitStore(connection);
        var containers = new List<ServiceProvider>();
        var fixture = new RateLimitConformanceFixture
        {
            Clock = new RealConformanceClock(),
            CreateLimiter =
                composition == "new"
                    ? (key, policy) =>
                        new DistributedRateLimiter(
                            new RateLimitingOptions { Namespace = ns, TokenBucket = policy },
                            key,
                            store
                        )
                    : (key, policy) =>
                    {
                        var services = new ServiceCollection();
                        services.AddKeyedSingleton(key, connection);
                        services
                            .AddHostLoomRateLimiting(
                                key,
                                options =>
                                {
                                    options.Namespace = ns;
                                    options.TokenBucket = policy;
                                }
                            )
                            .UseValkey();
                        var container = services.BuildServiceProvider();
                        containers.Add(container);
                        return container.GetRequiredKeyedService<DistributedRateLimiter>(key);
                    },
        };

        try
        {
            await RateLimitConformance
                .Scenarios[scenario](fixture)
                .WaitAsync(TimeSpan.FromSeconds(60), TestContext.Current.CancellationToken);
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
