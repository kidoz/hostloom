using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Runs the transport conformance scenarios the unit suite runs in memory against the RabbitMQ
/// broker from <c>docker-compose.yml</c>, on the wall clock. Every address, topic, and
/// subscription a scenario mints is recorded in a topology scope, so the durable queues and
/// exchanges it declares are deleted once its brokers and hosts are gone.
/// </summary>
[Collection(nameof(RabbitMqTransportConformanceTests))]
[CollectionDefinition(nameof(RabbitMqTransportConformanceTests), DisableParallelization = true)]
public sealed class RabbitMqTransportConformanceTests
{
    private static readonly Uri BrokerUri = new("amqp://guest:guest@localhost:5672/");

    private static readonly TransportProfile Profile = new()
    {
        Name = "RabbitMQ",
        PublishSubscribe = true,
        // "Broker health probe: not yet" (docs/reference/transports.md).
        HealthProbe = false,
        SharedAcrossInstances = true,
        // EventDispatchConcurrency defaults to 1, which keeps a subscription in queue order.
        OrderedWithinSubscription = true,
        // Listeners on one address consume the same request queue.
        CompetingListeners = true,
        // RequestAsync lets the cancellation of the token it links the caller's to escape
        // unchanged, so the exception carries that linked token rather than the caller's.
        CancellationCarriesCallerToken = false,
        DisposalFailsPendingRequests = true,
    };

    public static bool Available => BrokerAvailability.RabbitMq;

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

    [Theory(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnRabbitMq(string scenario)
    {
        var runId = Guid.NewGuid().ToString("N")[..12];
        var clientName = $"hostloom-conformance-{runId}";
        var sequence = 0;
        // Declared first so it is disposed last, after the fixture has released every broker.
        await using var topology = new RabbitMqTopologyScope(uri: BrokerUri);
        await using var fixture = new TransportConformanceFixture
        {
            Profile = Profile,
            RunId = runId,
            BrokerFactory = () =>
                new RabbitMqRequestBroker(
                    Options.Create(
                        new RabbitMqOptions { Uri = BrokerUri, ClientProvidedName = clientName }
                    )
                ),
            AddressFactory = (prefix, _) =>
                ValueTask.FromResult<RequestAddress>(
                    topology.Request(Name(runId, prefix, ref sequence))
                ),
            TopicFactory = (prefix, subscriptions, _) =>
            {
                var topic = topology.Topic(Name(runId, prefix, ref sequence));
                foreach (var subscription in subscriptions)
                {
                    topology.Subscription(topic, subscription);
                }

                return ValueTask.FromResult<RequestAddress>(topic);
            },
            UseTransport = hostLoom =>
                hostLoom.UseRabbitMq(options =>
                {
                    options.Uri = BrokerUri;
                    options.ClientProvidedName = clientName;
                }),
            RequestTimeout = TimeSpan.FromSeconds(20),
            TimeoutBudget = TimeSpan.FromSeconds(2),
            Slack = TimeSpan.FromSeconds(5),
            Bound = TimeSpan.FromSeconds(30),
            Clock = SystemTransportClock.Instance,
            CancellationToken = TestContext.Current.CancellationToken,
        };

        await TransportConformance.Scenarios[scenario](fixture);
    }

    private static string Name(string runId, string prefix, ref int sequence) =>
        $"conformance-{prefix}-{runId}-{Interlocked.Increment(ref sequence)}";
}
