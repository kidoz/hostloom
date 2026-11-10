using System.Buffers.Binary;

namespace HostLoom.DataGrid;

/// <summary>A version scoped to a cluster incarnation and one item in a committed command.</summary>
/// <remarks>
/// Only the state machine may assign versions from committed positions. Deleting an entry never
/// resets the log index. Restoring a snapshot preserves positions and the incarnation. This value
/// is not an external-resource fencing token. The default value denotes no entry.
/// </remarks>
public readonly record struct DataGridEntryVersion
{
    public const int EncodedByteCount = 28;

    public DataGridEntryVersion(Guid incarnationId, long logIndex, int itemIndex = 0)
    {
        if (incarnationId == Guid.Empty)
            throw new ArgumentException("An incarnation is required.", nameof(incarnationId));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(logIndex);
        ArgumentOutOfRangeException.ThrowIfNegative(itemIndex);
        IncarnationId = incarnationId;
        LogIndex = logIndex;
        ItemIndex = itemIndex;
    }

    public Guid IncarnationId { get; }
    public long LogIndex { get; }
    public int ItemIndex { get; }
    public bool IsEmpty => IncarnationId == Guid.Empty;

    /// <summary>Writes exactly 28 bytes: network-order UUID, positive int64 index, int32 item index.</summary>
    public void WriteTo(Span<byte> destination)
    {
        if (IsEmpty)
            throw new InvalidOperationException("An absent entry has no encoded version.");
        if (destination.Length != EncodedByteCount)
            throw new ArgumentException(
                "A version requires exactly 28 bytes.",
                nameof(destination)
            );
        IncarnationId.TryWriteBytes(destination, bigEndian: true, out _);
        BinaryPrimitives.WriteInt64BigEndian(destination[16..], LogIndex);
        BinaryPrimitives.WriteInt32BigEndian(destination[24..], ItemIndex);
    }

    /// <summary>Reads the fixed-width encoding, rejecting absent or malformed versions.</summary>
    public static DataGridEntryVersion ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length != EncodedByteCount)
            throw new ArgumentException("A version requires exactly 28 bytes.", nameof(source));
        return new DataGridEntryVersion(
            new Guid(source[..16], bigEndian: true),
            BinaryPrimitives.ReadInt64BigEndian(source[16..]),
            BinaryPrimitives.ReadInt32BigEndian(source[24..])
        );
    }
}
