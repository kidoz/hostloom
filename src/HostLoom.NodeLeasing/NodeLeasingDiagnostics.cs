using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.NodeLeasing;

/// <summary>Meter and activity source for <c>HostLoom.NodeLeasing</c>. The pool travels as a tag.</summary>
public static class NodeLeasingDiagnostics
{
    /// <summary>Activity source name to enable when configuring OpenTelemetry tracing.</summary>
    public const string ActivitySourceName = "HostLoom.NodeLeasing";

    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.NodeLeasing";

    /// <summary>Tag carrying the pool on every instrument.</summary>
    public const string PoolTag = "hostloom.node.pool";

    /// <summary>Tag on <c>hostloom.node.assignment.changes</c>: the lower-case <see cref="NodeLeaseChangeReason"/>.</summary>
    public const string ReasonTag = "hostloom.node.reason";

    /// <summary>
    /// Tag on <c>hostloom.node.renew.duration</c>: <c>renewed</c>; <c>refused</c> when the
    /// lease is gone, which ends the assignment; or <c>failed</c> when the renewal failed while
    /// the lease still runs, which is retried.
    /// </summary>
    public const string OutcomeTag = "hostloom.node.outcome";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    private static readonly ConcurrentDictionary<NodeIdElector, byte> Electors = new();

    internal static readonly Counter<long> Changes = Meter.CreateCounter<long>(
        "hostloom.node.assignment.changes",
        "{change}",
        "Node-ID assignment transitions, by reason."
    );

    internal static readonly Histogram<double> RenewDuration = Meter.CreateHistogram<double>(
        "hostloom.node.renew.duration",
        "s",
        "Time a lease renewal took, by outcome.",
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

    internal static readonly Counter<long> LoopFaults = Meter.CreateCounter<long>(
        "hostloom.node.loop.faults",
        "{fault}",
        "Unexpected exceptions the elector loop caught and backed off from."
    );

    // Declared after Meter and Electors on purpose: static initialisers run in textual order.
    private static readonly ObservableGauge<int> Assigned = Meter.CreateObservableGauge(
        "hostloom.node.assigned",
        ObserveAssignments,
        "{state}",
        "1 while this instance holds an ID in the pool, 0 otherwise."
    );

    internal static void Register(NodeIdElector elector) => Electors.TryAdd(elector, 0);

    internal static void Unregister(NodeIdElector elector) => Electors.TryRemove(elector, out _);

    internal static string ReasonName(NodeLeaseChangeReason reason) =>
        reason switch
        {
            NodeLeaseChangeReason.Acquired => "acquired",
            NodeLeaseChangeReason.Lost => "lost",
            NodeLeaseChangeReason.Released => "released",
            _ => "stopped",
        };

    private static IEnumerable<Measurement<int>> ObserveAssignments()
    {
        _ = Assigned;
        foreach (var elector in Electors.Keys)
        {
            yield return new Measurement<int>(
                elector.NodeId is null ? 0 : 1,
                new KeyValuePair<string, object?>(PoolTag, elector.Pool)
            );
        }
    }
}
