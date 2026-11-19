namespace HostLoom.DataGrid.ProcessProbe;

internal sealed record ProbeMemorySnapshot(
    long ManagedBytes,
    long TotalAllocatedBytes,
    long HeapBytes,
    long CommittedBytes,
    long FragmentedBytes,
    long MemoryLoadBytes,
    long TotalAvailableMemoryBytes,
    int Gen0Collections,
    int Gen1Collections,
    int Gen2Collections,
    double TotalPauseMilliseconds,
    long GcIndex,
    double PauseTimePercentage,
    double LastPause0Milliseconds,
    double LastPause1Milliseconds
)
{
    internal static ProbeMemorySnapshot Capture()
    {
        var info = GC.GetGCMemoryInfo();
        return new(
            GC.GetTotalMemory(false),
            GC.GetTotalAllocatedBytes(false),
            info.HeapSizeBytes,
            info.TotalCommittedBytes,
            info.FragmentedBytes,
            info.MemoryLoadBytes,
            info.TotalAvailableMemoryBytes,
            GC.CollectionCount(0),
            GC.CollectionCount(1),
            GC.CollectionCount(2),
            GC.GetTotalPauseDuration().TotalMilliseconds,
            info.Index,
            info.PauseTimePercentage,
            info.PauseDurations.Length > 0 ? info.PauseDurations[0].TotalMilliseconds : 0,
            info.PauseDurations.Length > 1 ? info.PauseDurations[1].TotalMilliseconds : 0
        );
    }
}

internal sealed record ProbeFailureSnapshot(
    string Type,
    string RootType,
    string? RootStack,
    bool StackTruncated
)
{
    internal static ProbeFailureSnapshot? Capture(Exception? error)
    {
        if (error is null)
            return null;
        var root = error.GetBaseException();
        var stack = root.StackTrace;
        return new(
            error.GetType().FullName!,
            root.GetType().FullName!,
            stack is { Length: > 8192 } ? stack[..8192] : stack,
            stack is { Length: > 8192 }
        );
    }
}

internal sealed record ProbeMemberSnapshot(
    string Role,
    long Term,
    long CommitIndex,
    long AppliedIndex,
    long SnapshotIndex,
    bool Ready,
    bool Stopped,
    int? LeaderId,
    ProbeFailureSnapshot? RuntimeFailure,
    ProbeFailureSnapshot? ServerFailure,
    ProbeMemorySnapshot Memory,
    ProbeSnapshotInstallSnapshot SnapshotInstall
);

internal sealed record ProbeSnapshotInstallSnapshot(
    int Active,
    long Bytes,
    long ByteLimit,
    bool Stopped
);

// Shared only by the child-process harness and its owning tests, not deployment configuration.
internal static class ProbeConfiguration
{
    internal static readonly Guid Cluster = new("10112233-4455-6677-8899-aabbccddeeff");
    internal static readonly Guid Map = new("00112233-4455-6677-8899-aabbccddeeff");
    internal static readonly Guid Principal = new("20112233-4455-6677-8899-aabbccddeeff");

    internal static readonly Guid SetupPrincipal = new("50112233-4455-6677-8899-aabbccddeeff");

    internal static DataGridBootstrapConfiguration Create(
        IReadOnlyList<int> peerPorts,
        bool feasibility = false
    ) =>
        new(
            Cluster,
            Enumerable
                .Range(0, peerPorts.Count)
                .Select(i => new DataGridMemberConfiguration(
                    new Guid(i + 1, 0, 0, new byte[8]),
                    $"node-{(char)('a' + i)}.example",
                    checked((ushort)peerPorts[i])
                ))
                .ToArray(),
            [
                new(
                    Map,
                    "catalog",
                    DataGridMapOwnership.Derived,
                    "catalog-service",
                    "catalog-loader",
                    feasibility ? 100000 : 1000,
                    feasibility ? 128L * 1024 * 1024 : 1048576,
                    new(1, 1, 2, 1, 1)
                ),
            ]
        );
}
