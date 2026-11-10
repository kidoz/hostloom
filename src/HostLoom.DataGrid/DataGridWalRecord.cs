using System.Buffers.Binary;
using System.Security.Cryptography;

namespace HostLoom.DataGrid;

/// <summary>The storage-envelope kind. Payload semantics require separate recovery validation.</summary>
public enum DataGridWalRecordKind
{
    None = 0,
    Identity = 1,
    HardState = 2,
    LogAppend = 3,
    LogTruncate = 4,
    CommitCheckpoint = 5,
    SnapshotManifest = 6,
}

/// <summary>An owned v1 WAL frame. This codec performs no file I/O, repair or consensus recovery.</summary>
/// <remarks>
/// SHA-256 detects accidental corruption, not malicious changes by a storage owner. A valid
/// frame does not establish durable persistence, payload validity or permission to apply a log.
/// </remarks>
public sealed class DataGridWalRecord
{
    public const int HeaderByteCount = 20;
    public const int DigestByteCount = 32;
    public const int MaxEncodedByteCount = 2 * 1024 * 1024;
    public const int MaxPayloadByteCount = MaxEncodedByteCount - HeaderByteCount - DigestByteCount;
    public const ushort FormatVersion = 1;
    private const uint Magic = 0x484c4757; // HLGW
    private readonly byte[] _payload;

    public DataGridWalRecord(DataGridWalRecordKind kind, long number, ReadOnlySpan<byte> payload)
    {
        if (!IsValidKind(kind))
            throw new ArgumentOutOfRangeException(nameof(kind));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(number);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(payload.Length, MaxPayloadByteCount);
        Kind = kind;
        Number = number;
        _payload = payload.ToArray();
    }

    public DataGridWalRecordKind Kind { get; }
    public long Number { get; }
    public int PayloadByteCount => _payload.Length;

    public byte[] GetPayload() => (byte[])_payload.Clone();

    /// <summary>Encodes a caller-owned frame with big-endian fields and a SHA-256 trailer.</summary>
    public byte[] Encode()
    {
        var frame = new byte[HeaderByteCount + _payload.Length + DigestByteCount];
        BinaryPrimitives.WriteUInt32BigEndian(frame, Magic);
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(4), FormatVersion);
        BinaryPrimitives.WriteUInt16BigEndian(frame.AsSpan(6), (ushort)Kind);
        BinaryPrimitives.WriteUInt32BigEndian(frame.AsSpan(8), (uint)_payload.Length);
        BinaryPrimitives.WriteInt64BigEndian(frame.AsSpan(12), Number);
        _payload.CopyTo(frame, HeaderByteCount);
        SHA256.HashData(
            frame.AsSpan(0, frame.Length - DigestByteCount),
            frame.AsSpan(frame.Length - DigestByteCount)
        );
        return frame;
    }

    /// <summary>
    /// Reads exactly one complete frame, validating bounds before payload allocation. The caller
    /// supplies the next expected physical record number; duplicates, gaps and reordering fail.
    /// Incomplete or malformed input throws <see cref="InvalidDataException"/> without repair.
    /// </summary>
    public static DataGridWalRecord ReadFrom(ReadOnlySpan<byte> source, long expectedNumber)
    {
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(expectedNumber);
        if (
            source.Length < HeaderByteCount + DigestByteCount
            || source.Length > MaxEncodedByteCount
        )
            throw new InvalidDataException("Invalid WAL frame size.");
        if (BinaryPrimitives.ReadUInt32BigEndian(source) != Magic)
            throw new InvalidDataException("Invalid WAL magic.");
        if (BinaryPrimitives.ReadUInt16BigEndian(source[4..]) != FormatVersion)
            throw new InvalidDataException("Unsupported WAL format.");
        var kind = (DataGridWalRecordKind)BinaryPrimitives.ReadUInt16BigEndian(source[6..]);
        if (!IsValidKind(kind))
            throw new InvalidDataException("Unknown WAL record kind.");
        var length = BinaryPrimitives.ReadUInt32BigEndian(source[8..]);
        if (
            length > MaxPayloadByteCount
            || source.Length != HeaderByteCount + (long)length + DigestByteCount
        )
            throw new InvalidDataException("Invalid WAL payload length.");
        var number = BinaryPrimitives.ReadInt64BigEndian(source[12..]);
        if (number != expectedNumber)
            throw new InvalidDataException("Unexpected WAL record number.");
        Span<byte> digest = stackalloc byte[DigestByteCount];
        SHA256.HashData(source[..^DigestByteCount], digest);
        if (!CryptographicOperations.FixedTimeEquals(digest, source[^DigestByteCount..]))
            throw new InvalidDataException("WAL checksum mismatch.");
        return new DataGridWalRecord(kind, number, source.Slice(HeaderByteCount, (int)length));
    }

    private static bool IsValidKind(DataGridWalRecordKind kind) =>
        kind is >= DataGridWalRecordKind.Identity and <= DataGridWalRecordKind.SnapshotManifest;
}
