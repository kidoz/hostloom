using System.Diagnostics.Metrics;
using HostLoom.DataGrid.Consensus;

namespace HostLoom.DataGrid.Runtime;

/// <summary>Bounded, process-aggregate measurements for the experimental grid actor.</summary>
public static class DataGridRuntimeDiagnostics
{
    public const string MeterName = "HostLoom.DataGrid.Runtime";
    public const string LaneTag = "hostloom.datagrid.lane";
    public const string ReasonTag = "hostloom.datagrid.reason";
    public const string OperationTag = "hostloom.datagrid.operation";
    public const string OutcomeTag = "hostloom.datagrid.outcome";

    private static readonly Meter Meter = new(MeterName);
    private static readonly UpDownCounter<long> QueueItems = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.runtime.queue.items",
        "{item}",
        "Work waiting in actor lanes; excludes executing work."
    );
    private static readonly UpDownCounter<long> QueueBytes = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.runtime.queue.bytes",
        "By",
        "Reserved encoded bytes waiting in actor lanes, not managed heap size."
    );
    private static readonly Counter<long> Rejections = Meter.CreateCounter<long>(
        "hostloom.datagrid.runtime.queue.rejected",
        "{item}",
        "Work refused before queue admission."
    );
    private static readonly Counter<long> Operations = Meter.CreateCounter<long>(
        "hostloom.datagrid.runtime.operations",
        "{operation}",
        "Local actor submission outcomes; not a count of unique committed mutations."
    );
    private static readonly UpDownCounter<long> Active = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.runtime.operations.active",
        "{operation}",
        "Local callers awaiting actor completion."
    );
    private static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(
        "hostloom.datagrid.runtime.operation.duration",
        "s",
        "Local submission through caller completion, including queue and persistence waits.",
        advice: DurationAdvice()
    );
    private static readonly Histogram<double> PersistenceDuration = Meter.CreateHistogram<double>(
        "hostloom.datagrid.runtime.persistence.duration",
        "s",
        "One synchronous file-store Persist call, including flush/publication; excludes dependent actor application.",
        advice: DurationAdvice()
    );

    private static InstrumentAdvice<double> DurationAdvice() =>
        new()
        {
            HistogramBucketBoundaries =
            [
                0.0001,
                0.0005,
                0.001,
                0.005,
                0.01,
                0.05,
                0.1,
                0.5,
                1,
                5,
                10,
                30,
                60,
            ],
        };

    internal static void QueueChange(bool control, int items, long bytes)
    {
        KeyValuePair<string, object?> lane = new(LaneTag, control ? "control" : "client");
        QueueItems.Add(items, lane);
        QueueBytes.Add(bytes, lane);
    }

    internal static void Reject(bool control, string reason) =>
        Rejections.Add(1, new(LaneTag, control ? "control" : "client"), new(ReasonTag, reason));

    internal static string Operation(DataGridCommittedCommand? command) =>
        command?.Kind switch
        {
            null => "read",
            DataGridCommittedCommandKind.Mutation => "mutation",
            DataGridCommittedCommandKind.OpenSession => "open_session",
            DataGridCommittedCommandKind.CloseSession => "close_session",
            DataGridCommittedCommandKind.TimeAdvance => "time_advance",
            DataGridCommittedCommandKind.Reclaim => "reclaim",
            _ => "other",
        };

    internal static string Outcome(RaftCompletionKind completion) =>
        completion switch
        {
            RaftCompletionKind.Applied => "applied",
            RaftCompletionKind.Read => "read",
            RaftCompletionKind.NotLeader => "not_leader",
            RaftCompletionKind.Busy => "busy",
            RaftCompletionKind.OutcomeUnknown => "outcome_unknown",
            RaftCompletionKind.Unavailable => "unavailable",
            RaftCompletionKind.InvalidRequest => "invalid_request",
            RaftCompletionKind.CapacityExceeded => "capacity_exceeded",
            _ => "other",
        };

    internal static void Begin(string operation) =>
        Active.Add(1, new KeyValuePair<string, object?>(OperationTag, operation));

    internal static void End(string operation, string outcome, double seconds)
    {
        KeyValuePair<string, object?> kind = new(OperationTag, operation),
            result = new(OutcomeTag, outcome);
        Active.Add(-1, kind);
        Operations.Add(1, kind, result);
        Duration.Record(seconds, kind, result);
    }

    internal static void Persisted(string outcome, double seconds) =>
        PersistenceDuration.Record(seconds, new KeyValuePair<string, object?>(OutcomeTag, outcome));
}
