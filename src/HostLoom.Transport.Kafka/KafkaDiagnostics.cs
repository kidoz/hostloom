using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace HostLoom.Transport.Kafka;

/// <summary>
/// Fixed-cardinality signals for the Kafka transport: what the producer and the consumer loops
/// do with records, not how handlers fare, which the <c>HostLoom</c> meter already records.
/// Producer-side instruments are tagged <c>hostloom.kafka.client</c> with the configured
/// <see cref="KafkaOptions.ClientId"/>; consumer-loop instruments are tagged
/// <c>messaging.destination.name</c> with the topic the loop consumes.
/// </summary>
public static class KafkaDiagnostics
{
    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.Transport.Kafka";

    internal const string ClientTag = "hostloom.kafka.client";
    internal const string DestinationTag = "messaging.destination.name";
    internal const string KindTag = "hostloom.kafka.kind";
    internal const string ReasonTag = "hostloom.kafka.reason";
    internal const string StageTag = "hostloom.kafka.stage";
    internal const string OutcomeTag = "hostloom.kafka.outcome";

    private static readonly Meter Meter = new(MeterName);

    private static readonly ConcurrentDictionary<KafkaRequestBroker, byte> LiveBrokers = new(
        ReferenceEqualityComparer.Instance
    );

    internal static readonly Counter<long> Produced = Meter.CreateCounter<long>(
        "hostloom.kafka.produced",
        "{record}",
        "Records the producer delivered, tagged by kind: request, reply, or event."
    );

    internal static readonly Counter<long> Consumed = Meter.CreateCounter<long>(
        "hostloom.kafka.consumed",
        "{record}",
        "Records a consumer loop received and handed to its handler."
    );

    internal static readonly Counter<long> Committed = Meter.CreateCounter<long>(
        "hostloom.kafka.committed",
        "{commit}",
        "Offsets a consumer loop committed."
    );

    internal static readonly Counter<long> RecordsSkipped = Meter.CreateCounter<long>(
        "hostloom.kafka.records.skipped",
        "{record}",
        "Records committed past without being handled to completion, tagged by reason."
    );

    internal static readonly Counter<long> RecordsRewound = Meter.CreateCounter<long>(
        "hostloom.kafka.records.rewound",
        "{record}",
        "Records a consumer loop rewound to so a transient failure is retried."
    );

    internal static readonly Counter<long> LoopFaults = Meter.CreateCounter<long>(
        "hostloom.kafka.loop.faults",
        "{fault}",
        "Client-library failures a consumer loop survived or reported, tagged by stage."
    );

    internal static readonly Counter<long> ReplyConsumerInitializations = Meter.CreateCounter<long>(
        "hostloom.kafka.reply_consumer.initializations",
        "{initialization}",
        "Attempts to start the reply consumer and wait for its assignment, tagged by outcome."
    );

#pragma warning disable CA1823 // observable instruments are kept alive by the meter, not read.
    private static readonly ObservableGauge<long> PendingRequests = Meter.CreateObservableGauge(
        "hostloom.kafka.requests.pending",
        static () => Observe(),
        "{request}",
        "Requests this client has produced and is still awaiting a reply for."
    );
#pragma warning restore CA1823

    internal static void Register(KafkaRequestBroker broker) => LiveBrokers[broker] = 0;

    internal static void Unregister(KafkaRequestBroker broker) =>
        LiveBrokers.TryRemove(broker, out _);

    private static IEnumerable<Measurement<long>> Observe()
    {
        foreach (var broker in LiveBrokers.Keys)
        {
            yield return new Measurement<long>(
                broker.PendingRequestCount,
                new KeyValuePair<string, object?>(ClientTag, broker.ClientId)
            );
        }
    }
}
