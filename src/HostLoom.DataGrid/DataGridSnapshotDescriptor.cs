using System.Buffers.Binary;

namespace HostLoom.DataGrid;

/// <summary>A bounded snapshot reference carried inside a checksummed WAL manifest record.</summary>
/// <remarks>
/// The digest covers the entire snapshot file, including its identity/configuration header.
/// This descriptor does not validate that file, publish it, or contain a member's term/vote.
/// </remarks>
public sealed class DataGridSnapshotDescriptor
{
    public const int EncodedByteCount = 72;
    public const long MaxSnapshotByteCount = 512L * 1024 * 1024;
    private readonly byte[] _digest;

    public DataGridSnapshotDescriptor(
        Guid snapshotId,
        long lastIncludedIndex,
        long lastIncludedTerm,
        long byteCount,
        ReadOnlySpan<byte> digest
    )
    {
        if (snapshotId == Guid.Empty)
            throw new ArgumentException("A snapshot identity is required.", nameof(snapshotId));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(lastIncludedIndex);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(lastIncludedTerm);
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(byteCount);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(byteCount, MaxSnapshotByteCount);
        if (digest.Length != DataGridWalRecord.DigestByteCount)
            throw new ArgumentException(
                "A SHA-256 digest requires exactly 32 bytes.",
                nameof(digest)
            );
        SnapshotId = snapshotId;
        LastIncludedIndex = lastIncludedIndex;
        LastIncludedTerm = lastIncludedTerm;
        ByteCount = byteCount;
        _digest = digest.ToArray();
    }

    public Guid SnapshotId { get; }
    public long LastIncludedIndex { get; }
    public long LastIncludedTerm { get; }
    public long ByteCount { get; }

    public byte[] GetDigest() => (byte[])_digest.Clone();

    /// <summary>Encodes a network-order UUID, three big-endian int64 fields, then the file digest.</summary>
    public byte[] Encode()
    {
        var bytes = new byte[EncodedByteCount];
        SnapshotId.TryWriteBytes(bytes, bigEndian: true, out _);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(16), LastIncludedIndex);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(24), LastIncludedTerm);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(32), ByteCount);
        _digest.CopyTo(bytes, 40);
        return bytes;
    }

    /// <summary>Decodes one manifest payload; the enclosing WAL checksum must also be validated.</summary>
    public static DataGridSnapshotDescriptor ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length != EncodedByteCount)
            throw new InvalidDataException("Invalid snapshot descriptor size.");
        try
        {
            return new DataGridSnapshotDescriptor(
                new Guid(source[..16], bigEndian: true),
                BinaryPrimitives.ReadInt64BigEndian(source[16..]),
                BinaryPrimitives.ReadInt64BigEndian(source[24..]),
                BinaryPrimitives.ReadInt64BigEndian(source[32..]),
                source[40..]
            );
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Invalid snapshot descriptor fields.", exception);
        }
    }
}
