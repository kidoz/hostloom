namespace HostLoom.DataGrid;

/// <summary>Ordinal byte ordering for map trees and canonical snapshot/reclamation order.</summary>
public sealed class DataGridKeyComparer : IComparer<DataGridKey>
{
    public static DataGridKeyComparer Instance { get; } = new();

    private DataGridKeyComparer() { }

    public int Compare(DataGridKey? x, DataGridKey? y)
    {
        if (ReferenceEquals(x, y))
            return 0;
        if (x is null)
            return -1;
        return y is null ? 1 : x.CompareBytesTo(y);
    }
}
