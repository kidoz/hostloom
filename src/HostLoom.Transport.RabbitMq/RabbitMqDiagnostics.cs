using System.Collections.Concurrent;
using System.Diagnostics.Metrics;

namespace HostLoom.Transport.RabbitMq;

/// <summary>
/// Fixed-cardinality signals for the RabbitMQ transport: what the adapter does on the wire, not
/// how handlers fare, which the <c>HostLoom</c> meter already records per request. Every
/// instrument is tagged <c>hostloom.rabbitmq.client</c> with the configured
/// <see cref="RabbitMqOptions.ClientProvidedName"/>, so one dashboard serves every client.
/// </summary>
public static class RabbitMqDiagnostics
{
    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.Transport.RabbitMq";

    internal const string ClientTag = "hostloom.rabbitmq.client";
    internal const string OutcomeTag = "hostloom.rabbitmq.outcome";
    internal const string ReasonTag = "hostloom.rabbitmq.reason";
    internal const string EventTag = "hostloom.rabbitmq.event";
    internal const string RoleTag = "hostloom.rabbitmq.role";

    private static readonly Meter Meter = new(MeterName);

    private static readonly ConcurrentDictionary<RabbitMqRequestBroker, byte> LiveBrokers = new(
        ReferenceEqualityComparer.Instance
    );

    internal static readonly Counter<long> Publishes = Meter.CreateCounter<long>(
        "hostloom.rabbitmq.publishes",
        "{publish}",
        "Request and event publications, tagged by the outcome the broker gave them."
    );

    internal static readonly Histogram<double> PublishDuration = Meter.CreateHistogram<double>(
        "hostloom.rabbitmq.publish.duration",
        "s",
        "Time one publication took, from waiting for a channel to the broker's confirmation.",
        tags: null,
        advice: new InstrumentAdvice<double>
        {
            // Operations: sub-millisecond hits up to slow calls of tens of seconds.
            HistogramBucketBoundaries =
            [
                0.0001,
                0.00025,
                0.0005,
                0.001,
                0.0025,
                0.005,
                0.01,
                0.025,
                0.05,
                0.1,
                0.25,
                0.5,
                1,
                2.5,
                5,
                10,
                30,
            ],
        }
    );

    internal static readonly Counter<long> DeliveriesRejected = Meter.CreateCounter<long>(
        "hostloom.rabbitmq.deliveries.rejected",
        "{delivery}",
        "Deliveries rejected without requeue, tagged by reason."
    );

    internal static readonly Counter<long> DeliveriesRequeued = Meter.CreateCounter<long>(
        "hostloom.rabbitmq.deliveries.requeued",
        "{delivery}",
        "Deliveries handed back to their queue because the listener was stopping or the delivery was cancelled."
    );

    internal static readonly Counter<long> Connections = Meter.CreateCounter<long>(
        "hostloom.rabbitmq.connections",
        "{connection}",
        "Connections opened, and recoveries the client library completed on them."
    );

    internal static readonly Counter<long> Consumers = Meter.CreateCounter<long>(
        "hostloom.rabbitmq.consumers",
        "{consumer}",
        "Consumers the broker cancelled, as it does when their queue is deleted, and consumption resumed after it, tagged by event and role."
    );

#pragma warning disable CA1823 // observable instruments are kept alive by the meter, not read.
    private static readonly ObservableGauge<long> PendingRequests = Meter.CreateObservableGauge(
        "hostloom.rabbitmq.requests.pending",
        static () => Observe(static broker => broker.PendingRequestCount),
        "{request}",
        "Requests this client has published and is still awaiting a reply for."
    );

    private static readonly ObservableGauge<long> ClosingChannels = Meter.CreateObservableGauge(
        "hostloom.rabbitmq.channels.closing",
        static () => Observe(static broker => broker.ClosingChannelCount),
        "{channel}",
        "Channels closing in the background, such as a publisher channel given up after a failed publication or the channel of a stopped listener or subscription."
    );
#pragma warning restore CA1823

    internal static void Register(RabbitMqRequestBroker broker) => LiveBrokers[broker] = 0;

    internal static void Unregister(RabbitMqRequestBroker broker) =>
        LiveBrokers.TryRemove(broker, out _);

    private static IEnumerable<Measurement<long>> Observe(Func<RabbitMqRequestBroker, int> read)
    {
        foreach (var broker in LiveBrokers.Keys)
        {
            yield return new Measurement<long>(
                read(broker),
                new KeyValuePair<string, object?>(ClientTag, broker.ClientName)
            );
        }
    }
}
