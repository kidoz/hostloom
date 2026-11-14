using System.Diagnostics.Metrics;

namespace HostLoom.DataGrid.Client;

/// <summary>Bounded logical-call and attempt measurements for the experimental grid SDK.</summary>
public static class DataGridClientDiagnostics
{
    public const string MeterName = "HostLoom.DataGrid.Client";
    public const string OperationTag = "hostloom.datagrid.operation";
    public const string OutcomeTag = "hostloom.datagrid.outcome";
    public const string AdmissionTag = "hostloom.datagrid.may_have_been_admitted";
    public const string SubmittedTag = "hostloom.datagrid.submitted";

    private static readonly Meter Meter = new(MeterName);
    private static readonly Counter<long> Calls = Meter.CreateCounter<long>(
        "hostloom.datagrid.client.calls",
        "{call}",
        "Logical SDK completions, including pre-admission refusals; not unique committed writes."
    );
    private static readonly Counter<long> Attempts = Meter.CreateCounter<long>(
        "hostloom.datagrid.client.attempts",
        "{attempt}",
        "Entered physical attempts, including attempts canceled before connecting or writing."
    );
    private static readonly Counter<long> Timeouts = Meter.CreateCounter<long>(
        "hostloom.datagrid.client.timeouts",
        "{call}",
        "Calls terminated by the SDK deadline; excludes caller cancellation, disable and credential retirement."
    );

    internal static void TimedOut(string operation, bool mayHaveBeenAdmitted) =>
        Timeouts.Add(1, new(OperationTag, operation), new(AdmissionTag, mayHaveBeenAdmitted));

    private static readonly UpDownCounter<long> Active = Meter.CreateUpDownCounter<long>(
        "hostloom.datagrid.client.calls.active",
        "{call}",
        "SDK invocations awaiting caller completion; includes brief pre-admission decisions."
    );
    private static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(
        "hostloom.datagrid.client.call.duration",
        "s",
        "Logical invocation through caller completion, including reconnect and all attempts.",
        advice: new InstrumentAdvice<double>
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
            ],
        }
    );

    internal static string Operation(DataGridMessageKind kind) =>
        kind switch
        {
            DataGridMessageKind.Read => "read",
            DataGridMessageKind.Mutation => "mutation",
            DataGridMessageKind.OpenSession => "open_session",
            DataGridMessageKind.CloseSession => "close_session",
            _ => "other",
        };

    internal static string Outcome(DataGridClientFailureCode? code) =>
        code switch
        {
            null => "success",
            DataGridClientFailureCode.InvalidRequest => "invalid_request",
            DataGridClientFailureCode.Unauthorized => "unauthorized",
            DataGridClientFailureCode.ProtocolMismatch => "protocol_mismatch",
            DataGridClientFailureCode.CapacityExceeded => "capacity_exceeded",
            DataGridClientFailureCode.QuorumUnavailable => "quorum_unavailable",
            DataGridClientFailureCode.SessionExpired => "session_expired",
            DataGridClientFailureCode.RequestRetired => "request_retired",
            DataGridClientFailureCode.RequestIdentityMismatch => "request_identity_mismatch",
            DataGridClientFailureCode.OutcomeUnknown => "outcome_unknown",
            DataGridClientFailureCode.MissingMap => "missing_map",
            DataGridClientFailureCode.Redirect => "redirect",
            _ => "other",
        };

    internal static void Attempt(string operation, bool submitted) =>
        Attempts.Add(1, new(OperationTag, operation), new(SubmittedTag, submitted));

    internal static void Begin(string operation) =>
        Active.Add(1, new KeyValuePair<string, object?>(OperationTag, operation));

    internal static void End(
        string operation,
        string outcome,
        bool mayHaveBeenAdmitted,
        double seconds
    )
    {
        KeyValuePair<string, object?> kind = new(OperationTag, operation),
            result = new(OutcomeTag, outcome),
            admission = new(AdmissionTag, mayHaveBeenAdmitted);
        Active.Add(-1, kind);
        Calls.Add(1, kind, result, admission);
        Duration.Record(seconds, kind, result, admission);
    }
}
