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
        "Time one publication took, from waiting for a channel to the broker's confirmation."
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

#pragma warning disable CA1823 // observable instruments are kept alive by the meter, not read.
    private static readonly ObservableGauge<long> PendingRequests = Meter.CreateObservableGauge(
        "hostloom.rabbitmq.requests.pending",
        static () => Observe(),
        "{request}",
        "Requests this client has published and is still awaiting a reply for."
    );
#pragma warning restore CA1823

    internal static void Register(RabbitMqRequestBroker broker) => LiveBrokers[broker] = 0;

    internal static void Unregister(RabbitMqRequestBroker broker) =>
        LiveBrokers.TryRemove(broker, out _);

    private static IEnumerable<Measurement<long>> Observe()
    {
        foreach (var broker in LiveBrokers.Keys)
        {
            yield return new Measurement<long>(
                broker.PendingRequestCount,
                new KeyValuePair<string, object?>(ClientTag, broker.ClientName)
            );
        }
    }
}
