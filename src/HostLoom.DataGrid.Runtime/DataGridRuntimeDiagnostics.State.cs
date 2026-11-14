using System.Diagnostics.Metrics;
using HostLoom.DataGrid.Consensus;

namespace HostLoom.DataGrid.Runtime;

public static partial class DataGridRuntimeDiagnostics
{
    public const string MemberOrdinalTag = "hostloom.datagrid.member.ordinal";
    public const string MapOrdinalTag = "hostloom.datagrid.map.ordinal";
    public const string PeerOrdinalTag = "hostloom.datagrid.peer.ordinal";
    public const string StateTag = "hostloom.datagrid.state";
    public const string PositionTag = "hostloom.datagrid.position";
    public const string MeasureTag = "hostloom.datagrid.measure";

    internal static void RecordState(DataGridRuntimeSnapshot snapshot)
    {
        KeyValuePair<string, object?> member = new(MemberOrdinalTag, snapshot.MemberOrdinal);
        ObservationTime.Record(snapshot.CapturedUnixMilliseconds, member);
        var active = !snapshot.Local.Stopped && !snapshot.Faulted;
        Record(State, "follower", active && snapshot.Local.Role == RaftRole.Follower);
        Record(State, "candidate", active && snapshot.Local.Role == RaftRole.Candidate);
        Record(State, "leader", active && snapshot.Local.Role == RaftRole.Leader);
        Record(State, "faulted", snapshot.Faulted);
        Record(State, "stopped", snapshot.Local.Stopped);
        Record(State, "local_ready_hint", snapshot.LocalReadyHint);
        Record(State, "busy", active && snapshot.Busy);
        Record(State, "storage_faulted", snapshot.Storage.Faulted);
        Record(State, "storage_closed", snapshot.Storage.Closed);
        At("term", snapshot.Local.Term);
        At("log_end", snapshot.LogEndIndex);
        At("committed", snapshot.Local.CommitIndex);
        At("applied", snapshot.Local.AppliedIndex);
        At("snapshot", snapshot.Local.SnapshotIndex);
        At("last_read_barrier", snapshot.LastSuccessfulReadBarrierIndex);
        At("last_read_barrier_term", snapshot.LastSuccessfulReadBarrierTerm);
        foreach (var replica in snapshot.Replicas)
        {
            KeyValuePair<string, object?> peer = new(PeerOrdinalTag, replica.PeerOrdinal);
            ReplicaObserved.Record(replica.Acknowledged ? 1 : 0, member, peer);
            ReplicaLag.Record(replica.EntryLag ?? 0, member, peer);
        }
        foreach (var map in snapshot.Maps)
        {
            KeyValuePair<string, object?> ordinal = new(MapOrdinalTag, map.Ordinal);
            MapKeys.Record(map.ChargedKeys, member, ordinal, new(MeasureTag, "charged"));
            MapKeys.Record(map.KeyLimit, member, ordinal, new(MeasureTag, "limit"));
            MapBytes.Record(map.ChargedBytes, member, ordinal, new(MeasureTag, "charged"));
            MapBytes.Record(map.ByteLimit, member, ordinal, new(MeasureTag, "limit"));
            ExpiredReclaimed.Record(map.ExpiredReclaimed, member, ordinal);
            MapState.Record(
                snapshot.LocalReadyHint ? 1 : 0,
                member,
                ordinal,
                new(StateTag, "local_ready_hint")
            );
            MapState.Record(snapshot.Faulted ? 1 : 0, member, ordinal, new(StateTag, "faulted"));
            MapState.Record(
                snapshot.Local.Stopped ? 1 : 0,
                member,
                ordinal,
                new(StateTag, "stopped")
            );
            MapPosition.Record(
                snapshot.Local.CommitIndex,
                member,
                ordinal,
                new(PositionTag, "committed")
            );
            MapPosition.Record(
                snapshot.Local.AppliedIndex,
                member,
                ordinal,
                new(PositionTag, "applied")
            );
            MapPosition.Record(
                snapshot.Local.SnapshotIndex,
                member,
                ordinal,
                new(PositionTag, "snapshot")
            );
        }
        Items("log_entries", snapshot.Capacity.LogEntries, RaftDurableImage.MaxEntries);
        Items(
            "client_log_entries",
            snapshot.Capacity.LogEntries,
            RaftDurableImage.ClientEntryLimit
        );
        Items("sessions", snapshot.Capacity.Sessions, DataGridLocalStateMachine.MaxSessionCount);
        Bytes("log", snapshot.Capacity.LogBytes, RaftDurableImage.MaxLogBytes);
        Bytes("client_log", snapshot.Capacity.LogBytes, RaftDurableImage.ClientLogByteLimit);
        Bytes(
            "retained_results",
            snapshot.Capacity.ResultBytes,
            DataGridLocalStateMachine.MaxResultBytes
        );
        StorageBytes.Record(
            snapshot.Storage.PublishedWalBytes,
            member,
            new(MeasureTag, "wal_published")
        );
        StorageBytes.Record(
            snapshot.Storage.PublishedSnapshotBytes,
            member,
            new(MeasureTag, "snapshot_published")
        );
        StorageRecords.Record(snapshot.Storage.GenerationRecordCount, member);

        void Record(Gauge<long> instrument, string state, bool value) =>
            instrument.Record(value ? 1 : 0, member, new(StateTag, state));
        void At(string position, long value) =>
            Position.Record(value, member, new(PositionTag, position));
        void Items(string resource, long used, long limit)
        {
            CapacityItems.Record(
                used,
                member,
                new(ResourceTag, resource),
                new(MeasureTag, "charged")
            );
            CapacityItems.Record(
                limit,
                member,
                new(ResourceTag, resource),
                new(MeasureTag, "limit")
            );
        }
        void Bytes(string resource, long used, long limit)
        {
            CapacityBytes.Record(
                used,
                member,
                new(ResourceTag, resource),
                new(MeasureTag, "charged")
            );
            CapacityBytes.Record(
                limit,
                member,
                new(ResourceTag, resource),
                new(MeasureTag, "limit")
            );
        }
    }
}
