namespace HostLoom.DataGrid;

/// <summary>A position returned by a change poll, scoped to one map and runtime generation.</summary>
public readonly record struct DataGridChangeCursor(Guid Generation, Guid Map, long Sequence);

/// <summary>A key changed by a committed operation. Multiple changes can share one index.</summary>
public readonly record struct DataGridCommittedChange(long Index, DataGridKey Key);

/// <summary>Committed key changes and the applied position at which the page was captured.</summary>
public sealed class DataGridChangePage
{
    public const int MaxChanges = 128;

    internal DataGridChangePage(
        DataGridChangeCursor cursor,
        DataGridCommittedChange[] changes,
        DataGridTimestamp appliedTime,
        long appliedIndex,
        bool gap,
        bool hasMore
    )
    {
        Cursor = cursor;
        Changes = Array.AsReadOnly(changes);
        AppliedTime = appliedTime;
        AppliedIndex = appliedIndex;
        Gap = gap;
        HasMore = hasMore;
    }

    public DataGridChangeCursor Cursor { get; }
    public IReadOnlyList<DataGridCommittedChange> Changes { get; }
    public DataGridTimestamp AppliedTime { get; }
    public long AppliedIndex { get; }

    /// <summary>History cannot resume from the supplied cursor. Capture a new baseline.</summary>
    public bool Gap { get; }

    /// <summary>More history may be scanned using this page's cursor.</summary>
    public bool HasMore { get; }
}
