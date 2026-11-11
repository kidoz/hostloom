using System.Diagnostics.Metrics;
using HostLoom.DataGrid.Consensus;

namespace HostLoom.DataGrid.Runtime;

/// <summary>Bounded, process-aggregate measurements for the experimental grid actor.</summary>
public static partial class DataGridRuntimeDiagnostics
{
    public const string MeterName = "HostLoom.DataGrid.Runtime";
    public const string LaneTag = "hostloom.datagrid.lane";
    public const string ReasonTag = "hostloom.datagrid.reason";
    public const string OperationTag = "hostloom.datagrid.operation";
    public const string OutcomeTag = "hostloom.datagrid.outcome";

    private static readonly Meter Meter = new(MeterName);
    private static readonly Gauge<long> ObservationTime = Meter.CreateGauge<long>(
        "hostloom.datagrid.runtime.observation.time",
        "ms",
        "UTC Unix milliseconds at the last owner publication; clocks may jump and samples may become stale."
    );
    private static readonly Gauge<long> State = Meter.CreateGauge<long>(
        "hostloom.datagrid.runtime.state",
        "{state}",
        "Published local Boolean hints. No current-quorum or lease authority is asserted."
    );
    private static readonly Gauge<long> Position = Meter.CreateGauge<long>(
        "hostloom.datagrid.runtime.position",
        "{position}",
        "Last published local consensus positions and historical successful read barrier."
    );
    private static readonly Gauge<long> ReplicaObserved = Meter.CreateGauge<long>(
        "hostloom.datagrid.runtime.replica.observed",
        "{observation}",
        "One when this leadership term has a validated acknowledgement; not fresh reachability."
    );
    private static readonly Gauge<long> ReplicaLag = Meter.CreateGauge<long>(
        "hostloom.datagrid.runtime.replica.lag",
        "{entry}",
        "Log-end minus last acknowledged position. Zero is a placeholder when observed is zero."
    );
    private static readonly Gauge<long> MapKeys = Meter.CreateGauge<long>(
        "hostloom.datagrid.runtime.map.keys",
        "{key}",
        "Charged physical entries, including unreclaimed expired entries, and configured limit."
    );
    private static readonly Gauge<long> MapBytes = Meter.CreateGauge<long>(
        "hostloom.datagrid.runtime.map.bytes",
        "By",
        "Charged encoded key/value bytes and configured limit; excludes heap and replay state."
    );
    private static readonly Gauge<long> StorageBytes = Meter.CreateGauge<long>(
        "hostloom.datagrid.runtime.storage.bytes",
        "By",
        "Cached WAL/image bytes from last successful owned publication; not current disk inventory."
    );
    private static readonly Gauge<long> StorageRecords = Meter.CreateGauge<long>(
        "hostloom.datagrid.runtime.storage.records",
        "{record}",
        "Record count in the last successfully published WAL generation."
    );

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
    private static readonly UpDownCounter<long> AdmissionActive = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.runtime.admission.active",
        "{operation}",
        "Client operations reserved before enqueue through actor completion/drain, independent of caller cancellation."
    );
    private static readonly UpDownCounter<long> AdmissionBytes = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.runtime.admission.bytes",
        "By",
        "Retained encoded submission charges across queued/executing/pending work; excludes response/log/replay state and heap overhead."
    );
    private static readonly UpDownCounter<long> ControlActive = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.runtime.control.active",
        "{item}",
        "Control work reserved before enqueue through action completion or queue drain."
    );
    private static readonly UpDownCounter<long> ControlBytes = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.runtime.control.bytes",
        "By",
        "Retained encoded control work across queue and execution; excludes peer sender/transport copies and heap overhead."
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

    internal static void AdmissionChange(bool control, int operations, long bytes)
    {
        (control ? ControlActive : AdmissionActive).Add(operations);
        (control ? ControlBytes : AdmissionBytes).Add(bytes);
    }

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
