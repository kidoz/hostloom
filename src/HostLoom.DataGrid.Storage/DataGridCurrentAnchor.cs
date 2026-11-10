using System.Buffers.Binary;
using System.Security.Cryptography;

namespace HostLoom.DataGrid.Storage;

/// <summary>The sole v1 generation selection anchor. This codec does not publish files.</summary>
public sealed class DataGridCurrentAnchor
{
    public const int EncodedByteCount = 88;

    public DataGridCurrentAnchor(Guid incarnationId, Guid memberId, Guid generationId)
    {
        if (incarnationId == Guid.Empty || memberId == Guid.Empty || generationId == Guid.Empty)
            throw new ArgumentException("Nonempty storage identities are required.");
        IncarnationId = incarnationId;
        MemberId = memberId;
        GenerationId = generationId;
    }

    public Guid IncarnationId { get; }
    public Guid MemberId { get; }
    public Guid GenerationId { get; }

    public byte[] Encode()
    {
        var bytes = new byte[EncodedByteCount];
        "HLGC"u8.CopyTo(bytes);
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(4), 1);
        IncarnationId.TryWriteBytes(bytes.AsSpan(8), bigEndian: true, out _);
        MemberId.TryWriteBytes(bytes.AsSpan(24), bigEndian: true, out _);
        GenerationId.TryWriteBytes(bytes.AsSpan(40), bigEndian: true, out _);
        SHA256.HashData(bytes.AsSpan(0, 56), bytes.AsSpan(56));
        return bytes;
    }

    public static DataGridCurrentAnchor ReadFrom(ReadOnlySpan<byte> bytes)
    {
        if (
            bytes.Length != EncodedByteCount
            || !bytes[..4].SequenceEqual("HLGC"u8)
            || BinaryPrimitives.ReadUInt16BigEndian(bytes[4..]) != 1
            || BinaryPrimitives.ReadUInt16BigEndian(bytes[6..]) != 0
        )
            throw new InvalidDataException("Invalid CURRENT header.");
        Span<byte> digest = stackalloc byte[32];
        SHA256.HashData(bytes[..56], digest);
        if (!CryptographicOperations.FixedTimeEquals(digest, bytes[56..]))
            throw new InvalidDataException("Invalid CURRENT checksum.");
        var incarnation = new Guid(bytes.Slice(8, 16), bigEndian: true);
        var member = new Guid(bytes.Slice(24, 16), bigEndian: true);
        var generation = new Guid(bytes.Slice(40, 16), bigEndian: true);
        if (incarnation == Guid.Empty || member == Guid.Empty || generation == Guid.Empty)
            throw new InvalidDataException("Invalid CURRENT identity.");
        return new(incarnation, member, generation);
    }
}
