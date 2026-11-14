using HostLoom.DataGrid.Storage;

namespace HostLoom.DataGrid.Runtime;

internal sealed record DataGridReplicaProgress(
    int PeerOrdinal,
    bool Acknowledged,
    long? MatchedIndex,
    long? EntryLag
);

internal sealed record DataGridRuntimeCapacity(
    int LogEntries,
    long LogBytes,
    int Sessions,
    long ResultBytes
);

/// <summary>Immutable owner-thread observation. Reading it performs no actor, network or filesystem work.</summary>
internal sealed record DataGridRuntimeSnapshot(
    int MemberOrdinal,
    long CapturedTimestamp,
    long CapturedUnixMilliseconds,
    DataGridPeerRuntimeStatus Local,
    bool Faulted,
    bool Busy,
    bool LocalReadyHint,
    long LogEndIndex,
    long LastSuccessfulReadBarrierIndex,
    long LastSuccessfulReadBarrierTerm,
    IReadOnlyList<DataGridMapUsage> Maps,
    IReadOnlyList<DataGridReplicaProgress> Replicas,
    DataGridStorageProgress Storage,
    DataGridRuntimeCapacity Capacity
);
