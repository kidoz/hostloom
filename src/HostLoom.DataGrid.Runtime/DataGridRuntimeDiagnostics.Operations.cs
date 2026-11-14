using System.ComponentModel;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.DataGrid.Runtime;

public static partial class DataGridRuntimeDiagnostics
{
    public const string ResourceTag = "hostloom.datagrid.resource";

    static DataGridRuntimeDiagnostics()
    {
        _ = Meter.CreateObservableGauge(
            "hostloom.datagrid.process.memory",
            ObserveMemory,
            "By",
            "Process-wide working set and GC.GetTotalMemory(false) estimate at collection; not per-grid attribution or peak usage."
        );
    }

    private static IEnumerable<Measurement<long>> ObserveMemory()
    {
        var measurements = new List<Measurement<long>>(2)
        {
            new(
                GC.GetTotalMemory(false),
                new KeyValuePair<string, object?>(MeasureTag, "managed_estimate")
            ),
        };
        try
        {
            using var process = Process.GetCurrentProcess();
            measurements.Add(
                new(
                    process.WorkingSet64,
                    new KeyValuePair<string, object?>(MeasureTag, "working_set")
                )
            );
        }
        catch (Exception exception) when (exception is Win32Exception or NotSupportedException)
        {
            // An unavailable OS reading is omitted instead of reporting a healthy zero.
        }
        return measurements;
    }

    internal static void TimedOut(string operation) =>
        Timeouts.Add(1, new KeyValuePair<string, object?>(OperationTag, operation));

    internal static void AppliedResult(
        DataGridBootstrapConfiguration configuration,
        int member,
        DataGridCommittedCommand command,
        DataGridApplyResult result
    )
    {
        if (!Results.Enabled && !MutationItems.Enabled)
            return;
        KeyValuePair<string, object?> local = new(MemberOrdinalTag, member);
        Results.Add(
            1,
            local,
            new(OperationTag, Operation(command)),
            new(
                OutcomeTag,
                result.Status switch
                {
                    DataGridApplyStatus.Completed => "completed",
                    DataGridApplyStatus.SessionExpired => "session_expired",
                    DataGridApplyStatus.RequestRetired => "request_retired",
                    DataGridApplyStatus.RequestIdentityMismatch => "request_identity_mismatch",
                    DataGridApplyStatus.CapacityExceeded => "capacity_exceeded",
                    DataGridApplyStatus.MissingMap => "missing_map",
                    _ => "invalid_request",
                }
            )
        );
        if (command.Mutation is not { } mutation || result.Status != DataGridApplyStatus.Completed)
            return;
        for (var map = 0; map < configuration.MapCount; map++)
        {
            if (configuration.GetMap(map).MapId != mutation.MapId)
                continue;
            for (var i = 0; i < result.ItemCount; i++)
                MutationItems.Add(
                    1,
                    local,
                    new(MapOrdinalTag, map),
                    new(
                        OutcomeTag,
                        result.GetItem(i).Status switch
                        {
                            DataGridMutationStatus.Stored => "stored",
                            DataGridMutationStatus.Removed => "removed",
                            DataGridMutationStatus.Missing => "missing",
                            DataGridMutationStatus.VersionMismatch => "version_mismatch",
                            DataGridMutationStatus.AlreadyExists => "already_exists",
                            DataGridMutationStatus.CapacityExceeded => "capacity_exceeded",
                            _ => "invalid_request",
                        }
                    )
                );
            break;
        }
    }
}
