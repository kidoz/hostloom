using Confluent.Kafka;
using Confluent.Kafka.Admin;
using HostLoom.Conformance;
using HostLoom.Transport.Kafka;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Runs the transport conformance scenarios the unit suite runs in memory against the Kafka broker
/// from <c>docker-compose.yml</c>, on the wall clock. Every scenario gets its own consumer-group
/// prefix and response topic, and every topic it creates is deleted once its brokers and hosts are
/// gone. Budgets are generous because a fresh group must be assigned partitions before anything
/// is delivered.
/// </summary>
[Collection(nameof(KafkaTransportConformanceTests))]
[CollectionDefinition(nameof(KafkaTransportConformanceTests), DisableParallelization = true)]
public sealed class KafkaTransportConformanceTests
{
    private const string BootstrapServers = "localhost:9092";

    private static readonly TransportProfile Profile = new()
    {
        Name = "Kafka",
        PublishSubscribe = true,
        HealthProbe = true,
        SharedAcrossInstances = true,
        // Kafka orders within a partition only; every topic this runner creates has one.
        OrderedWithinSubscription = true,
        // Listeners on one address join one consumer group, and the partition moves to whichever
        // member remains.
        CompetingListeners = true,
        CancellationCarriesCallerToken = true,
        DisposalFailsPendingRequests = true,
    };

    public static bool Available => BrokerAvailability.Kafka;

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

    [Theory(Skip = BrokerAvailability.KafkaSkip, SkipUnless = nameof(Available))]
    [MemberData(nameof(Scenarios))]
    public async Task Scenario_PassesOnKafka(string scenario)
    {
        var runId = Guid.NewGuid().ToString("N")[..12];
        var group = $"conformance-{runId}";
        var clients = 0;
        // Declared first so it is disposed last, after the fixture has released every consumer.
        await using var topics = new KafkaTopics(runId);
        var responses = await topics.CreateAsync(
            "responses",
            TestContext.Current.CancellationToken
        );
        await using var fixture = new TransportConformanceFixture
        {
            Profile = Profile,
            RunId = runId,
            // Instances of one run share the group prefix and the response topic, like instances
            // of one calling service; each has its own client id and so its own reply group.
            BrokerFactory = () =>
                new KafkaRequestBroker(
                    Options.Create(
                        new KafkaOptions
                        {
                            BootstrapServers = BootstrapServers,
                            ConsumerGroup = group,
                            ResponseTopic = responses,
                            ClientId = $"{group}-{Interlocked.Increment(ref clients)}",
                        }
                    )
                ),
            AddressFactory = async (prefix, cancellationToken) =>
                await topics.CreateAsync(prefix, cancellationToken),
            TopicFactory = async (prefix, _, cancellationToken) =>
                await topics.CreateAsync(prefix, cancellationToken),
            UseTransport = hostLoom =>
                hostLoom.UseKafka(options =>
                {
                    options.BootstrapServers = BootstrapServers;
                    options.ConsumerGroup = group;
                    options.ResponseTopic = responses;
                    options.ClientId = $"{group}-host-{Interlocked.Increment(ref clients)}";
                }),
            RequestTimeout = TimeSpan.FromSeconds(45),
            TimeoutBudget = TimeSpan.FromSeconds(5),
            Slack = TimeSpan.FromSeconds(10),
            Bound = TimeSpan.FromSeconds(60),
            Clock = SystemTransportClock.Instance,
            CancellationToken = TestContext.Current.CancellationToken,
        };

        await TransportConformance.Scenarios[scenario](fixture);
    }

    /// <summary>
    /// Creates single-partition topics under the run's names and deletes them all on disposal,
    /// the way <see cref="KafkaTransportTests"/> does.
    /// </summary>
    private sealed class KafkaTopics(string runId) : IAsyncDisposable
    {
        private readonly Lock _gate = new();
        private readonly List<string> _created = [];
        private int _sequence;

        public async Task<string> CreateAsync(string prefix, CancellationToken cancellationToken)
        {
            var topic = $"conformance-{prefix}-{runId}-{Interlocked.Increment(ref _sequence)}";
            using var admin = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = BootstrapServers }
            ).Build();
            // Recorded before creating, so a creation that timed out after succeeding is removed too.
            lock (_gate)
            {
                _created.Add(topic);
            }

            await admin
                .CreateTopicsAsync(
                    [
                        new TopicSpecification
                        {
                            Name = topic,
                            NumPartitions = 1,
                            ReplicationFactor = 1,
                        },
                    ],
                    new CreateTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                )
                .WaitAsync(TimeSpan.FromSeconds(15), cancellationToken);
            return topic;
        }

        public async ValueTask DisposeAsync()
        {
            string[] topics;
            lock (_gate)
            {
                topics = [.. _created];
                _created.Clear();
            }

            if (topics.Length == 0)
            {
                return;
            }

            using var admin = new AdminClientBuilder(
                new AdminClientConfig { BootstrapServers = BootstrapServers }
            ).Build();
            try
            {
                await admin
                    .DeleteTopicsAsync(
                        topics,
                        new DeleteTopicsOptions { RequestTimeout = TimeSpan.FromSeconds(10) }
                    )
                    .WaitAsync(TimeSpan.FromSeconds(15));
            }
            catch (DeleteTopicsException exception)
                when (exception.Results.All(result =>
                        result.Error.Code is ErrorCode.NoError or ErrorCode.UnknownTopicOrPart
                    )
                )
            {
                // A topic whose creation failed was never there to delete.
            }
        }
    }
}
