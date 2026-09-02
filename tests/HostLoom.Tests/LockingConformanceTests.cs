using HostLoom.Conformance;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using HostLoom.Locking.Testing;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Runs the backend-neutral lock scenarios on the in-process provider, composed with <c>new</c>
/// and through the container.
/// </summary>
public sealed class LockingConformanceTests
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
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnTheInProcessProvider(string scenario, string composition)
    {
        var fixture = composition == "new" ? ContainerFree() : Container();

        await LockConformance.Scenarios[scenario](fixture);
    }

    private static LockConformanceFixture ContainerFree()
    {
        var clock = new ManualTimeProvider();
        var faults = new Locking.Testing.FaultingLockProvider(new InMemoryLockProvider(clock));
        return new LockConformanceFixture
        {
            Clock = new ManualConformanceClock(clock),
            Faults = faults,
            CreateLock = () =>
                new DistributedLock(
                    new LockingOptions { Namespace = "conformance" },
                    faults,
                    clock
                ),
        };
    }

    private static LockConformanceFixture Container()
    {
        var clock = new ManualTimeProvider();
        var faults = new Locking.Testing.FaultingLockProvider(new InMemoryLockProvider(clock));
        return new LockConformanceFixture
        {
            Clock = new ManualConformanceClock(clock),
            Faults = faults,
            CreateLock = () =>
            {
                var services = new ServiceCollection();
                services.AddSingleton(faults);
                services.AddSingleton<TimeProvider>(clock);
                services
                    .AddHostLoomLocking(locking => locking.Namespace = "conformance")
                    .UseProvider<Locking.Testing.FaultingLockProvider>("Faulting");
                return services.BuildServiceProvider().GetRequiredService<IDistributedLock>();
            },
        };
    }
}
