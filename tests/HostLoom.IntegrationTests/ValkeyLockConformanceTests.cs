using HostLoom.Conformance;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Locking.Testing;
using HostLoom.Valkey;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Runs the same lock scenarios the unit suite runs on the in-process provider against a real
/// Valkey, composed with <c>new</c> and through the container, on the wall clock.
/// </summary>
[Collection(nameof(ValkeyLockConformanceTests))]
[CollectionDefinition(nameof(ValkeyLockConformanceTests), DisableParallelization = true)]
public sealed class ValkeyLockConformanceTests
{
    public static bool Available => ValkeyAvailability.Available;

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

    [Theory(Skip = ValkeyAvailability.Skip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnValkey(string scenario, string composition)
    {
        var ns = "conf-" + Guid.NewGuid().ToString("N")[..8];
        await using var connection = new ValkeyConnection(ValkeyAvailability.Options());
        var provider = new ValkeyLockProvider(connection);
        var faults = new FaultingLockProvider(provider);
        var containers = new List<ServiceProvider>();
        var instances = new List<IAsyncDisposable>();
        var fixture = new LockConformanceFixture
        {
            Clock = new RealConformanceClock(),
            Faults = faults,
            CreateLock =
                composition == "new"
                    ? () => new DistributedLock(new LockingOptions { Namespace = ns }, faults)
                    : () => FromContainer(ns, faults, containers),
        };

        try
        {
            await LockConformance
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

    private static IDistributedLock FromContainer(
        string ns,
        FaultingLockProvider faults,
        List<ServiceProvider> containers
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services
            .AddHostLoomLocking(locking => locking.Namespace = ns)
            .UseProvider<FaultingLockProvider>("Faulting");
        var container = services.BuildServiceProvider();
        containers.Add(container);
        return container.GetRequiredService<IDistributedLock>();
    }
}
