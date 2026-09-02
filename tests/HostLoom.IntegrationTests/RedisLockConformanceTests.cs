using HostLoom.Conformance;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Locking.Testing;
using HostLoom.Redis;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Runs the same lock scenarios the unit suite runs on the in-process provider against a real
/// Redis, composed with <c>new</c> and through the container, on the wall clock.
/// </summary>
[Collection(nameof(RedisLockConformanceTests))]
[CollectionDefinition(nameof(RedisLockConformanceTests), DisableParallelization = true)]
public sealed class RedisLockConformanceTests
{
    public static bool Available => RedisAvailability.Redis;

    public static TheoryData<string, string> Scenarios
    {
        get
        {
            var data = new TheoryData<string, string>();
            foreach (var scenario in LockConformance.Scenarios.Keys)
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
        await using var provider = new RedisLockProvider(connection);
        var faults = new FaultingLockProvider(provider);
        var fixture = new LockConformanceFixture
        {
            Clock = new RealConformanceClock(),
            Faults = faults,
            CreateLock =
                composition == "new"
                    ? () => new DistributedLock(new LockingOptions { Namespace = ns }, faults)
                    : () => FromContainer(ns, faults),
        };

        await LockConformance.Scenarios[scenario](fixture);
    }

    private static IDistributedLock FromContainer(string ns, FaultingLockProvider faults)
    {
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services
            .AddHostLoomLocking(locking => locking.Namespace = ns)
            .UseProvider<FaultingLockProvider>("Faulting");
        return services.BuildServiceProvider().GetRequiredService<IDistributedLock>();
    }
}
