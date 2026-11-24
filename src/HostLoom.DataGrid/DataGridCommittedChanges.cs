namespace HostLoom.DataGrid;

// Called only by the serialized Raft owner after durable commit, without invoking user code.
internal interface IDataGridCommittedChangeSink
{
    void Record(long index, Guid map, DataGridKey key);
    void Reset();
}
