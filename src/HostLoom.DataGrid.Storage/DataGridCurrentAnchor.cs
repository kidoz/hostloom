using System.Buffers.Binary;
using System.Security.Cryptography;

namespace HostLoom.DataGrid.Storage;

/// <summary>Generation selection with an optional v2 published WAL frontier. This codec does not publish files.</summary>
public sealed class DataGridCurrentAnchor
{
    /// <summary>Encoded length of the legacy v1 generation-only anchor.</summary>
    public const int EncodedByteCount = 88;
    public const int FrontierEncodedByteCount = 136;
    private readonly byte[]? _walDigest;

    public DataGridCurrentAnchor(Guid incarnationId, Guid memberId, Guid generationId)
    {
        if (incarnationId == Guid.Empty || memberId == Guid.Empty || generationId == Guid.Empty)
            throw new ArgumentException("Nonempty storage identities are required.");
        IncarnationId = incarnationId;
        MemberId = memberId;
        GenerationId = generationId;
    }

    public DataGridCurrentAnchor(
        Guid incarnationId,
        Guid memberId,
        Guid generationId,
        long publishedWalBytes,
        long publishedRecordCount,
        ReadOnlySpan<byte> walDigest
    )
        : this(incarnationId, memberId, generationId)
    {
        if (
            publishedWalBytes is < 1 or > 1024L * 1024 * 1024
            || publishedRecordCount is < 3 or > DataGridWalRecovery.MaxRecordCount
            || walDigest.Length != 32
        )
            throw new ArgumentException("Invalid WAL publication frontier.");
        PublishedWalBytes = publishedWalBytes;
        PublishedRecordCount = publishedRecordCount;
        _walDigest = walDigest.ToArray();
    }

    public Guid IncarnationId { get; }
    public Guid MemberId { get; }
    public Guid GenerationId { get; }
    public ushort FormatVersion => _walDigest is null ? (ushort)1 : (ushort)2;
    public long? PublishedWalBytes { get; }
    public long? PublishedRecordCount { get; }

    /// <summary>Returns a defensive copy of the prefix digest, or empty for v1.</summary>
    public byte[] GetWalDigest() => _walDigest is null ? [] : (byte[])_walDigest.Clone();

    public byte[] Encode()
    {
        var bytes = new byte[_walDigest is null ? EncodedByteCount : FrontierEncodedByteCount];
        "HLGC"u8.CopyTo(bytes);
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(4), FormatVersion);
        IncarnationId.TryWriteBytes(bytes.AsSpan(8), bigEndian: true, out _);
        MemberId.TryWriteBytes(bytes.AsSpan(24), bigEndian: true, out _);
        GenerationId.TryWriteBytes(bytes.AsSpan(40), bigEndian: true, out _);
        if (_walDigest is not null)
        {
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(56), PublishedWalBytes!.Value);
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(64), PublishedRecordCount!.Value);
            _walDigest.CopyTo(bytes, 72);
        }
        SHA256.HashData(bytes.AsSpan(0, bytes.Length - 32), bytes.AsSpan(bytes.Length - 32));
        return bytes;
    }

    public static DataGridCurrentAnchor ReadFrom(ReadOnlySpan<byte> bytes)
    {
        if (
            bytes.Length is not (EncodedByteCount or FrontierEncodedByteCount)
            || !bytes[..4].SequenceEqual("HLGC"u8)
            || BinaryPrimitives.ReadUInt16BigEndian(bytes[6..]) != 0
        )
            throw new InvalidDataException("Invalid CURRENT header.");
        var version = BinaryPrimitives.ReadUInt16BigEndian(bytes[4..]);
        if (
            !(
                version == 1 && bytes.Length == EncodedByteCount
                || version == 2 && bytes.Length == FrontierEncodedByteCount
            )
        )
            throw new InvalidDataException("Invalid CURRENT version/length.");
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(bytes[..^32], digest);
        if (!CryptographicOperations.FixedTimeEquals(digest, bytes[^32..]))
            throw new InvalidDataException("Invalid CURRENT checksum.");
        var incarnation = new Guid(bytes.Slice(8, 16), bigEndian: true);
        var member = new Guid(bytes.Slice(24, 16), bigEndian: true);
        var generation = new Guid(bytes.Slice(40, 16), bigEndian: true);
        if (incarnation == Guid.Empty || member == Guid.Empty || generation == Guid.Empty)
            throw new InvalidDataException("Invalid CURRENT identity.");
        if (version == 1)
            return new(incarnation, member, generation);
        try
        {
            return new(
                incarnation,
                member,
                generation,
                BinaryPrimitives.ReadInt64BigEndian(bytes[56..]),
                BinaryPrimitives.ReadInt64BigEndian(bytes[64..]),
                bytes.Slice(72, 32)
            );
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Invalid CURRENT frontier.", exception);
        }
    }
}
