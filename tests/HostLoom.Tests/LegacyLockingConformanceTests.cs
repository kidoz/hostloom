using HostLoom.Conformance;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Xunit;
using TestFaultingLockProvider = HostLoom.Locking.Testing.FaultingLockProvider;

namespace HostLoom.Tests;

public sealed class LegacyLockingConformanceTests
{
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

    [Theory]
    [InlineData("catalog:lock:inventory", "lock:inventory")]
    [InlineData("catalog:lock:orders:eu", "lock:orders:eu")]
    public void LegacyKey_PreservesTheConsumerIdentity(string canonical, string expected)
    {
        Assert.Equal(expected, LegacyLockIdentityProvider.LegacyKey(canonical));
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesWithTheLegacyIdentity(string scenario, string composition)
    {
        var clock = new ManualTimeProvider();
        var inner = new InMemoryLockProvider(clock);
        var legacy = new LegacyLockIdentityProvider(inner);
        var faults = new TestFaultingLockProvider(legacy);
        var containers = new List<ServiceProvider>();
        var fixture = new LockConformanceFixture
        {
            Clock = new ManualConformanceClock(clock),
            Faults = faults,
            CreateLock = () =>
                composition == "new"
                    ? new DistributedLock(
                        new LockingOptions { Namespace = "migrated" },
                        faults,
                        clock
                    )
                    : FromContainer(faults, clock, containers),
        };

        try
        {
            await LockConformance.Scenarios[scenario](fixture);
        }
        finally
        {
            foreach (var container in containers)
                await container.DisposeAsync();
        }
    }

    [Fact]
    public async Task RawLegacyHolder_BlocksMigratedAcquisitionAndRollbackRetainsIdentity()
    {
        var token = TestContext.Current.CancellationToken;
        var inner = new InMemoryLockProvider();
        await using var migrated = new DistributedLock(
            new LockingOptions { Namespace = "migrated" },
            new LegacyLockIdentityProvider(inner)
        );

        Assert.True(
            await inner.TryAcquireAsync("lock:inventory", "legacy", TimeSpan.FromMinutes(1), token)
        );
        Assert.Null(await migrated.TryAcquireAsync("inventory", cancellationToken: token));
        Assert.True(await inner.ReleaseAsync("lock:inventory", "legacy", token));

        var held = await migrated.TryAcquireAsync("inventory", cancellationToken: token);
        Assert.NotNull(held);
        Assert.False(
            await inner.TryAcquireAsync(
                "lock:inventory",
                "rollback",
                TimeSpan.FromMinutes(1),
                token
            )
        );
        await held.DisposeAsync();
        Assert.True(
            await inner.TryAcquireAsync(
                "lock:inventory",
                "rollback",
                TimeSpan.FromMinutes(1),
                token
            )
        );
    }

    private static IDistributedLock FromContainer(
        TestFaultingLockProvider faults,
        ManualTimeProvider clock,
        List<ServiceProvider> containers
    )
    {
        var services = new ServiceCollection();
        services.AddSingleton(faults);
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddHostLoomLocking(locking => locking.Namespace = "migrated")
            .UseProvider<TestFaultingLockProvider>("Faulting");
        var container = services.BuildServiceProvider();
        containers.Add(container);
        return container.GetRequiredService<IDistributedLock>();
    }
}
