using System.Diagnostics.Metrics;

namespace HostLoom.DataGrid.Storage;

/// <summary>Recovery-call observations for the experimental serialized grid file store.</summary>
public static class DataGridStorageDiagnostics
{
    public const string MeterName = "HostLoom.DataGrid.Storage";
    public const string OutcomeTag = "hostloom.datagrid.storage.outcome";
    private static readonly Meter Meter = new(MeterName);
    private static readonly Counter<long> Recoveries = Meter.CreateCounter<long>(
        "hostloom.datagrid.storage.recoveries",
        "{recovery}",
        "Explicit Open calls, including refusals before directory ownership; excludes bootstrap."
    );
    private static readonly Histogram<double> Duration = Meter.CreateHistogram<double>(
        "hostloom.datagrid.storage.recovery.duration",
        "s",
        "Synchronous directory ownership, inventory and WAL/snapshot validation; excludes subsequent runtime replay/startup.",
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
                60,
            ],
        }
    );

    internal static void Recovered(bool success, double seconds)
    {
        KeyValuePair<string, object?> outcome = new(OutcomeTag, success ? "recovered" : "failed");
        Recoveries.Add(1, outcome);
        Duration.Record(seconds, outcome);
    }
}

// Cached successful-publication metadata, not physical file lengths after a failed write.
internal sealed record DataGridStorageProgress(
    long PublishedWalBytes,
    long PublishedSnapshotBytes,
    long DurableCommitIndex,
    long GenerationRecordCount,
    bool Faulted,
    bool Closed
);
