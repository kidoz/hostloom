using HostLoom.Conformance;
using HostLoom.RateLimiting;
using HostLoom.RateLimiting.DependencyInjection;
using HostLoom.Redis;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>Runs shared token-bucket scenarios against real Redis.</summary>
[Collection(nameof(RedisRateLimitConformanceTests))]
[CollectionDefinition(nameof(RedisRateLimitConformanceTests), DisableParallelization = true)]
public sealed class RedisRateLimitConformanceTests
{
    public static bool Available => RedisAvailability.Redis;

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

    [Theory(Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnRedis(string scenario, string composition)
    {
        var ns = "rate-conf-" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new RedisConnection(
            new RedisOptions
            {
                Configuration = RedisAvailability.Configuration,
                ClientName = "hostloom-rate-conformance",
            }
        );
        await using var store = new RedisRateLimitStore(connection);
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
                            .UseRedis();
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
