using System.Buffers.Binary;
using System.Diagnostics.CodeAnalysis;

namespace HostLoom.DataGrid;

[SuppressMessage(
    "Design",
    "CA1027:Mark enums with FlagsAttribute",
    Justification = "Protocol opcodes are mutually exclusive values, not flags."
)]
public enum DataGridMessageKind
{
    None = 0,
    ClientHello = 1,
    MemberHello = 2,
    Mutation = 3,
    MutationResult = 4,
    Read = 5,
    ReadResult = 6,
    OpenSession = 7,
    OpenSessionResult = 8,
    CloseSession = 9,
    CloseSessionResult = 10,
    VoteRequest = 16,
    VoteResponse = 17,
    AppendRequest = 18,
    AppendResponse = 19,
    SnapshotStart = 20,
    SnapshotChunk = 21,
    SnapshotResponse = 22,
    ReadBarrierRequest = 23,
    ReadBarrierResponse = 24,
    Failure = 31,
}

/// <summary>An owned v1 protocol envelope for use after authenticated TLS.</summary>
/// <remarks>Envelope validation is not authentication, authorization, body validation or dispatch.</remarks>
public sealed class DataGridProtocolFrame
{
    public const int HeaderByteCount = 72;
    public const int MaxEncodedByteCount = 1024 * 1024;
    public const int MaxPayloadByteCount = MaxEncodedByteCount - HeaderByteCount;
    private readonly byte[] _payload;

    public DataGridProtocolFrame(
        DataGridMessageKind kind,
        Guid incarnationId,
        Guid senderId,
        long sessionNumber,
        ulong requestNumber,
        Guid attemptId,
        ReadOnlySpan<byte> payload
    )
    {
        if (!IsValidKind(kind))
            throw new ArgumentOutOfRangeException(nameof(kind));
        if (incarnationId == Guid.Empty)
            throw new ArgumentException("An incarnation is required.", nameof(incarnationId));
        if (senderId == Guid.Empty)
            throw new ArgumentException("A sender is required.", nameof(senderId));
        if (attemptId == Guid.Empty)
            throw new ArgumentException("An attempt identity is required.", nameof(attemptId));
        if (!IsValidSession(kind, sessionNumber, requestNumber))
            throw new ArgumentException(
                "Invalid session/request fields for message kind.",
                nameof(sessionNumber)
            );
        ArgumentOutOfRangeException.ThrowIfGreaterThan(payload.Length, MaxPayloadByteCount);
        Kind = kind;
        IncarnationId = incarnationId;
        SenderId = senderId;
        SessionNumber = sessionNumber;
        RequestNumber = requestNumber;
        AttemptId = attemptId;
        _payload = payload.ToArray();
    }

    public DataGridMessageKind Kind { get; }
    public Guid IncarnationId { get; }
    public Guid SenderId { get; }
    public long SessionNumber { get; }
    public ulong RequestNumber { get; }
    public Guid AttemptId { get; }

    public byte[] GetPayload() => (byte[])_payload.Clone();

    public byte[] Encode()
    {
        var bytes = new byte[HeaderByteCount + _payload.Length];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, (uint)(bytes.Length - 4));
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(4), 1);
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(6), (ushort)Kind);
        IncarnationId.TryWriteBytes(bytes.AsSpan(8), bigEndian: true, out _);
        SenderId.TryWriteBytes(bytes.AsSpan(24), bigEndian: true, out _);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(40), SessionNumber);
        BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan(48), RequestNumber);
        AttemptId.TryWriteBytes(bytes.AsSpan(56), bigEndian: true, out _);
        _payload.CopyTo(bytes, HeaderByteCount);
        return bytes;
    }

    /// <summary>Reads one exact frame using identities supplied by the authenticated connection.</summary>
    public static DataGridProtocolFrame ReadFrom(
        ReadOnlySpan<byte> source,
        Guid expectedIncarnationId,
        Guid expectedSenderId
    )
    {
        if (expectedIncarnationId == Guid.Empty || expectedSenderId == Guid.Empty)
            throw new ArgumentException("Expected authenticated identities must be nonempty.");
        if (source.Length < HeaderByteCount || source.Length > MaxEncodedByteCount)
            throw new InvalidDataException("Invalid protocol frame size.");
        if (
            BinaryPrimitives.ReadUInt32BigEndian(source) != source.Length - 4
            || BinaryPrimitives.ReadUInt16BigEndian(source[4..]) != 1
        )
            throw new InvalidDataException("Invalid protocol length or version.");
        var incarnation = new Guid(source.Slice(8, 16), bigEndian: true);
        var sender = new Guid(source.Slice(24, 16), bigEndian: true);
        if (incarnation != expectedIncarnationId || sender != expectedSenderId)
            throw new InvalidDataException("Protocol identity mismatch.");
        try
        {
            return new DataGridProtocolFrame(
                (DataGridMessageKind)BinaryPrimitives.ReadUInt16BigEndian(source[6..]),
                incarnation,
                sender,
                BinaryPrimitives.ReadInt64BigEndian(source[40..]),
                BinaryPrimitives.ReadUInt64BigEndian(source[48..]),
                new Guid(source.Slice(56, 16), bigEndian: true),
                source[HeaderByteCount..]
            );
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Invalid protocol envelope fields.", exception);
        }
    }

    private static bool IsValidSession(DataGridMessageKind kind, long session, ulong request)
    {
        if (
            kind
            is DataGridMessageKind.Mutation
                or DataGridMessageKind.MutationResult
                or DataGridMessageKind.CloseSession
                or DataGridMessageKind.CloseSessionResult
        )
            return session > 0 && request > 0;
        if (kind == DataGridMessageKind.Failure)
            return (session == 0 && request == 0) || (session > 0 && request > 0);
        return session == 0 && request == 0;
    }

    private static bool IsValidKind(DataGridMessageKind kind) =>
        kind
            is >= DataGridMessageKind.ClientHello
                and <= DataGridMessageKind.CloseSessionResult
                or >= DataGridMessageKind.VoteRequest
                and <= DataGridMessageKind.ReadBarrierResponse
                or DataGridMessageKind.Failure;
}
