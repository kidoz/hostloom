using HostLoom.Conformance;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Runs the transport conformance scenarios on the in-memory transport. Its request timeouts run
/// on a <see cref="TestClock"/> handed to the broker's constructor, so a scenario that waits for a
/// timeout drives the clock instead of the wall clock.
/// </summary>
public sealed class InMemoryTransportConformanceTests
{
    /// <summary>
    /// How the in-memory transport differs from a broker-backed one, and why each difference is
    /// by design rather than a defect.
    /// </summary>
    private static readonly TransportProfile Profile = new()
    {
        Name = "in-memory",
        PublishSubscribe = true,
        HealthProbe = true,
        // Every instance is its own process-local world.
        SharedAcrossInstances = false,
        // Publishing awaits the subscription's delivery before it returns.
        OrderedWithinSubscription = true,
        // Direct dispatch has one handler per address; a second listen is refused.
        CompetingListeners = false,
    };

    public static TheoryData<string> Scenarios
    {
        get
        {
            var data = new TheoryData<string>();
            foreach (var scenario in TransportConformance.Scenarios.Keys)
            {
                data.Add(scenario);
            }

            return data;
        }
    }

    [Theory]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnTheInMemoryTransport(string scenario)
    {
        var clock = new TestClock();
        var runId = Guid.NewGuid().ToString("N")[..12];
        var sequence = 0;
        await using var fixture = new TransportConformanceFixture
        {
            Profile = Profile,
            RunId = runId,
            BrokerFactory = () => new InMemoryRequestBroker(logger: null, timeProvider: clock),
            AddressFactory = (prefix, _) => ValueTask.FromResult(Name(runId, prefix, ref sequence)),
            TopicFactory = (prefix, _, _) =>
                ValueTask.FromResult(Name(runId, prefix, ref sequence)),
            UseTransport = hostLoom =>
            {
                // Registered before the transport, so the container hands the broker this clock.
                hostLoom.Services.AddSingleton<TimeProvider>(clock);
                hostLoom.UseInMemory();
            },
            // Nothing times out on the manual clock unless a scenario advances it, so these only
            // have to be distinguishable; the wall-clock bound catches a hang.
            RequestTimeout = TimeSpan.FromSeconds(30),
            TimeoutBudget = TimeSpan.FromMinutes(5),
            Slack = TimeSpan.Zero,
            Bound = TimeSpan.FromSeconds(30),
            Clock = new ManualTransportClock(clock),
            CancellationToken = TestContext.Current.CancellationToken,
        };

        await TransportConformance.Scenarios[scenario](fixture);
    }

    private static RequestAddress Name(string runId, string prefix, ref int sequence) =>
        $"conformance-{prefix}-{runId}-{Interlocked.Increment(ref sequence)}";

    /// <summary>
    /// The test clock as a transport clock. A timer armed after an advance would never fire, so
    /// advancing waits until the operation under test has armed one.
    /// </summary>
    private sealed class ManualTransportClock(TestClock clock) : TransportClock
    {
        public override TimeProvider Provider => clock;

        public override TimeSpan Resolution => TimeSpan.Zero;

        public override async Task AdvanceAsync(TimeSpan delta, CancellationToken cancellationToken)
        {
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers > 0);
            clock.Advance(delta);
        }
    }
}
