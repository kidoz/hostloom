namespace HostLoom.DataGrid.Runtime;

internal readonly record struct DataGridChangeCursor(Guid Generation, Guid Map, long Sequence);

internal readonly record struct DataGridCommittedChange(long Index, DataGridKey Key);

internal sealed record DataGridChangePage(
    DataGridChangeCursor Cursor,
    IReadOnlyList<DataGridCommittedChange> Changes,
    DataGridTimestamp AppliedTime,
    bool Gap,
    bool HasMore
);

/// <summary>The serialized owner's bounded history of committed map changes.</summary>
internal sealed class DataGridChangeFeed(TimeProvider time) : IDataGridCommittedChangeSink
{
    internal const int MaxChanges = 4096;
    internal const int MaxBytes = 4 * 1024 * 1024;
    internal const int MaxPageSize = 128;
    internal static readonly TimeSpan MaxAge = TimeSpan.FromSeconds(30);

    private readonly Queue<Retained> _history = new();
    private Guid _generation = Guid.NewGuid();
    private long _tail;
    private int _bytes;

    void IDataGridCommittedChangeSink.Record(long index, Guid map, DataGridKey key)
    {
        if (_tail == long.MaxValue)
            Reset();
        var now = time.GetTimestamp();
        Prune(now);
        var change = new Retained(++_tail, now, map, new(index, key));
        while (_history.Count >= MaxChanges || _bytes + change.Bytes > MaxBytes)
            RemoveHead();
        _history.Enqueue(change);
        _bytes += change.Bytes;
    }

    void IDataGridCommittedChangeSink.Reset() => Reset();

    internal DataGridChangePage Read(
        Guid map,
        DataGridChangeCursor? after,
        DataGridTimestamp appliedTime
    )
    {
        Prune(time.GetTimestamp());
        var tail = new DataGridChangeCursor(_generation, map, _tail);
        if (after is not { } cursor)
            return new(tail, [], appliedTime, Gap: false, HasMore: false);
        var oldestPosition = _history.TryPeek(out var first) ? first.Sequence - 1 : _tail;
        if (
            cursor.Generation != _generation
            || cursor.Map != map
            || cursor.Sequence < oldestPosition
            || cursor.Sequence > _tail
        )
            return new(tail, [], appliedTime, Gap: true, HasMore: false);

        var changes = new List<DataGridCommittedChange>();
        var sequence = cursor.Sequence;
        foreach (var entry in _history)
        {
            if (entry.Sequence <= sequence)
                continue;
            sequence = entry.Sequence;
            if (entry.Map != map)
                continue;
            changes.Add(entry.Change);
            if (changes.Count == MaxPageSize)
                break;
        }
        return new(
            cursor with
            {
                Sequence = sequence,
            },
            changes.AsReadOnly(),
            appliedTime,
            Gap: false,
            HasMore: sequence < _tail
        );
    }

    private void Prune(long now)
    {
        while (
            _history.TryPeek(out var oldest) && time.GetElapsedTime(oldest.Timestamp, now) >= MaxAge
        )
            RemoveHead();
    }

    private void RemoveHead() => _bytes -= _history.Dequeue().Bytes;

    internal void Reset()
    {
        _history.Clear();
        _generation = Guid.NewGuid();
        _tail = 0;
        _bytes = 0;
    }

    private readonly record struct Retained(
        long Sequence,
        long Timestamp,
        Guid Map,
        DataGridCommittedChange Change
    )
    {
        // Charge fixed metadata and the immutable key retained by this record.
        internal int Bytes => 64 + Change.Key.ByteCount;
    }
}
