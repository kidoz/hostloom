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
    int Gen2Collections
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
            GC.CollectionCount(2)
        );
    }
}

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
                .Range(0, 3)
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
