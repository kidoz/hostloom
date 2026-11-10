using System.Buffers.Binary;
using System.Security.Cryptography;

namespace HostLoom.DataGrid;

public enum DataGridMutationKind
{
    None = 0,
    Put = 1,
    PutIfAbsent = 2,
    CompareExchange = 3,
    Remove = 4,
    CompareRemove = 5,
}

public enum DataGridExpiryKind
{
    Never = 0,
    Relative = 1,
    Absolute = 2,
}

/// <summary>One immutable mutation item, with owned value bytes and an optional CAS version.</summary>
public sealed class DataGridMutationItem
{
    public const int MaxValueByteCount = 64 * 1024;
    private readonly byte[] _value;

    public DataGridMutationItem(
        DataGridKey key,
        ReadOnlySpan<byte> value,
        DataGridEntryVersion expectedVersion = default
    )
    {
        ArgumentNullException.ThrowIfNull(key);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(value.Length, MaxValueByteCount);
        Key = key;
        ExpectedVersion = expectedVersion;
        _value = value.ToArray();
    }

    public DataGridKey Key { get; }
    public DataGridEntryVersion ExpectedVersion { get; }
    public int ValueByteCount => _value.Length;

    public byte[] GetValue() => (byte[])_value.Clone();

    internal void CopyValueTo(Span<byte> destination) => _value.AsSpan().CopyTo(destination);
}

/// <summary>A canonical v1 client mutation. Identical retries have identical bytes and digest.</summary>
/// <remarks>Leader time and physical attempt identity are deliberately outside this command.</remarks>
public sealed class DataGridMutationCommand
{
    public const int HeaderByteCount = 52;
    public const int MaxItemCount = 128;

    // Reserve the AppendRequest header (34), entry header (20), and replicated wrapper (36).
    public const int MaxEncodedByteCount = DataGridProtocolFrame.MaxPayloadByteCount - 90;
    private readonly DataGridMutationItem[] _items;
    private readonly int _encodedByteCount;

    public DataGridMutationCommand(
        DataGridMutationKind kind,
        Guid mapId,
        long sessionNumber,
        ulong requestNumber,
        DataGridExpiryKind expiryKind,
        long expiryTicks,
        IReadOnlyList<DataGridMutationItem> items
    )
    {
        if (kind is < DataGridMutationKind.Put or > DataGridMutationKind.CompareRemove)
            throw new ArgumentOutOfRangeException(nameof(kind));
        if (mapId == Guid.Empty)
            throw new ArgumentException("A map identity is required.", nameof(mapId));
        ArgumentOutOfRangeException.ThrowIfNegativeOrZero(sessionNumber);
        ArgumentOutOfRangeException.ThrowIfZero(requestNumber);
        if (
            expiryKind is < DataGridExpiryKind.Never or > DataGridExpiryKind.Absolute
            || (expiryKind == DataGridExpiryKind.Never && expiryTicks != 0)
            || (
                expiryKind == DataGridExpiryKind.Relative
                && (expiryTicks <= 0 || expiryTicks > DateTimeOffset.MaxValue.Ticks)
            )
            || (
                expiryKind == DataGridExpiryKind.Absolute
                && (expiryTicks < 0 || expiryTicks > DateTimeOffset.MaxValue.Ticks)
            )
            || (
                (kind is DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove)
                && expiryKind != DataGridExpiryKind.Never
            )
        )
            throw new ArgumentException("Invalid expiration policy.", nameof(expiryKind));
        ArgumentNullException.ThrowIfNull(items);
        if (items.Count is < 1 or > MaxItemCount)
            throw new ArgumentOutOfRangeException(nameof(items));
        Kind = kind;
        MapId = mapId;
        SessionNumber = sessionNumber;
        RequestNumber = requestNumber;
        ExpiryKind = expiryKind;
        ExpiryTicks = expiryTicks;
        _items = new DataGridMutationItem[items.Count];
        var size = HeaderByteCount;
        for (var i = 0; i < _items.Length; i++)
        {
            var item =
                items[i] ?? throw new ArgumentException("Null mutation item.", nameof(items));
            if (
                (
                    (
                        kind
                        is DataGridMutationKind.CompareExchange
                            or DataGridMutationKind.CompareRemove
                    )
                    && (
                        item.ExpectedVersion.IsEmpty
                        || item.ExpectedVersion.ItemIndex >= MaxItemCount
                    )
                )
                || (
                    (
                        kind
                        is not (
                            DataGridMutationKind.CompareExchange
                            or DataGridMutationKind.CompareRemove
                        )
                    ) && !item.ExpectedVersion.IsEmpty
                )
                || (
                    (kind is DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove)
                    && item.ValueByteCount != 0
                )
            )
                throw new ArgumentException("Invalid mutation item fields.", nameof(items));
            for (var j = 0; j < i; j++)
                if (_items[j].Key.Equals(item.Key))
                    throw new ArgumentException("Duplicate mutation key.", nameof(items));
            size += 2 + item.Key.ByteCount;
            if (kind is not (DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove))
                size += 4 + item.ValueByteCount;
            if (
                (kind is DataGridMutationKind.CompareExchange or DataGridMutationKind.CompareRemove)
            )
                size += DataGridEntryVersion.EncodedByteCount;
            if (size > MaxEncodedByteCount)
                throw new ArgumentOutOfRangeException(nameof(items));
            _items[i] = item;
        }
        _encodedByteCount = size;
    }

    public DataGridMutationKind Kind { get; }
    public Guid MapId { get; }
    public long SessionNumber { get; }
    public ulong RequestNumber { get; }
    public DataGridExpiryKind ExpiryKind { get; }
    public long ExpiryTicks { get; }
    public int ItemCount => _items.Length;

    public DataGridMutationItem GetItem(int index) => _items[index];

    public byte[] Encode()
    {
        var bytes = new byte[_encodedByteCount];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, 0x484c474f); // HLGO
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(4), 1);
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(6), (ushort)Kind);
        MapId.TryWriteBytes(bytes.AsSpan(8), bigEndian: true, out _);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(24), SessionNumber);
        BinaryPrimitives.WriteUInt64BigEndian(bytes.AsSpan(32), RequestNumber);
        bytes[40] = (byte)ExpiryKind;
        BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(42), (ushort)_items.Length);
        BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(44), ExpiryTicks);
        var offset = HeaderByteCount;
        foreach (var item in _items)
        {
            BinaryPrimitives.WriteUInt16BigEndian(bytes.AsSpan(offset), (ushort)item.Key.ByteCount);
            offset += 2;
            item.Key.CopyTo(bytes.AsSpan(offset, item.Key.ByteCount));
            offset += item.Key.ByteCount;
            if (Kind is DataGridMutationKind.CompareExchange or DataGridMutationKind.CompareRemove)
            {
                item.ExpectedVersion.WriteTo(
                    bytes.AsSpan(offset, DataGridEntryVersion.EncodedByteCount)
                );
                offset += DataGridEntryVersion.EncodedByteCount;
            }
            if (Kind is not (DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove))
            {
                BinaryPrimitives.WriteUInt32BigEndian(
                    bytes.AsSpan(offset),
                    (uint)item.ValueByteCount
                );
                offset += 4;
                item.CopyValueTo(bytes.AsSpan(offset, item.ValueByteCount));
                offset += item.ValueByteCount;
            }
        }
        return bytes;
    }

    public byte[] ComputeDigest() => SHA256.HashData(Encode());

    /// <summary>Validates a client mutation body against its already validated envelope.</summary>
    public static DataGridMutationCommand ReadFrom(DataGridProtocolFrame frame)
    {
        ArgumentNullException.ThrowIfNull(frame);
        if (frame.Kind != DataGridMessageKind.Mutation)
            throw new InvalidDataException("Expected a mutation frame.");
        var command = ReadFrom(frame.GetPayload());
        if (
            command.SessionNumber != frame.SessionNumber
            || command.RequestNumber != frame.RequestNumber
        )
            throw new InvalidDataException("Mutation identity differs from envelope.");
        foreach (var item in command._items)
            if (
                !item.ExpectedVersion.IsEmpty
                && item.ExpectedVersion.IncarnationId != frame.IncarnationId
            )
                throw new InvalidDataException("Foreign expected version.");
        return command;
    }

    /// <summary>Rejects malformed lengths/counts before allocating their corresponding payloads.</summary>
    public static DataGridMutationCommand ReadFrom(ReadOnlySpan<byte> source)
    {
        if (source.Length < HeaderByteCount || source.Length > MaxEncodedByteCount)
            throw new InvalidDataException("Invalid mutation size.");
        if (
            BinaryPrimitives.ReadUInt32BigEndian(source) != 0x484c474f
            || BinaryPrimitives.ReadUInt16BigEndian(source[4..]) != 1
            || source[41] != 0
        )
            throw new InvalidDataException("Invalid mutation header.");
        var kind = (DataGridMutationKind)BinaryPrimitives.ReadUInt16BigEndian(source[6..]);
        if (kind is < DataGridMutationKind.Put or > DataGridMutationKind.CompareRemove)
            throw new InvalidDataException("Unknown mutation kind.");
        var count = BinaryPrimitives.ReadUInt16BigEndian(source[42..]);
        if (count is < 1 or > MaxItemCount)
            throw new InvalidDataException("Invalid mutation item count.");
        try
        {
            var items = new DataGridMutationItem[count];
            var offset = HeaderByteCount;
            for (var i = 0; i < count; i++)
            {
                Require(source, offset, 2);
                var keyLength = BinaryPrimitives.ReadUInt16BigEndian(source[offset..]);
                offset += 2;
                if (keyLength is < 1 or > DataGridKey.MaxByteCount)
                    throw new InvalidDataException("Invalid mutation key size.");
                Require(source, offset, keyLength);
                var key = new DataGridKey(source.Slice(offset, keyLength));
                offset += keyLength;
                var version = default(DataGridEntryVersion);
                if (
                    (
                        kind
                        is DataGridMutationKind.CompareExchange
                            or DataGridMutationKind.CompareRemove
                    )
                )
                {
                    Require(source, offset, DataGridEntryVersion.EncodedByteCount);
                    version = DataGridEntryVersion.ReadFrom(
                        source.Slice(offset, DataGridEntryVersion.EncodedByteCount)
                    );
                    offset += DataGridEntryVersion.EncodedByteCount;
                }
                ReadOnlySpan<byte> value = [];
                if (kind is not (DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove))
                {
                    Require(source, offset, 4);
                    var valueLength = BinaryPrimitives.ReadUInt32BigEndian(source[offset..]);
                    offset += 4;
                    if (valueLength > DataGridMutationItem.MaxValueByteCount)
                        throw new InvalidDataException("Invalid mutation value size.");
                    Require(source, offset, (int)valueLength);
                    value = source.Slice(offset, (int)valueLength);
                    offset += (int)valueLength;
                }
                items[i] = new DataGridMutationItem(key, value, version);
            }
            if (offset != source.Length)
                throw new InvalidDataException("Trailing mutation bytes.");
            return new DataGridMutationCommand(
                kind,
                new Guid(source.Slice(8, 16), bigEndian: true),
                BinaryPrimitives.ReadInt64BigEndian(source[24..]),
                BinaryPrimitives.ReadUInt64BigEndian(source[32..]),
                (DataGridExpiryKind)source[40],
                BinaryPrimitives.ReadInt64BigEndian(source[44..]),
                items
            );
        }
        catch (ArgumentException exception)
        {
            throw new InvalidDataException("Invalid mutation fields.", exception);
        }
    }

    private static void Require(ReadOnlySpan<byte> source, int offset, int length)
    {
        if (length > source.Length - offset)
            throw new InvalidDataException("Truncated mutation item.");
    }
}
