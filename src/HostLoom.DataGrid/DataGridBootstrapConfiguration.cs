using System.Buffers.Binary;
using System.Net;
using System.Security.Cryptography;
using System.Text;

namespace HostLoom.DataGrid;

public enum DataGridMapOwnership
{
    None = 0,
    Authoritative = 1,
    Derived = 2,
}

/// <summary>Explicit immutable map policy. Unsupported v1 selections are rejected.</summary>
/// <remarks>Codes are consistency=1 quorum, durability=1 majority-persistent, expiration=1 never-only
/// or 2 explicit deadlines, eviction=1 reject, schema=1 opaque bytes.</remarks>
public readonly record struct DataGridMapPolicy(
    byte Consistency,
    byte Durability,
    byte Expiration,
    byte Eviction,
    byte Schema
);

public sealed class DataGridMemberConfiguration
{
    public DataGridMemberConfiguration(Guid memberId, string dnsName, ushort port)
    {
        if (memberId == Guid.Empty)
            throw new ArgumentException("A member identity is required.", nameof(memberId));
        ArgumentNullException.ThrowIfNull(dnsName);
        if (dnsName.Length is < 1 or > 253 || IPAddress.TryParse(dnsName, out _))
            throw new ArgumentException("A canonical DNS endpoint is required.", nameof(dnsName));
        foreach (var label in dnsName.Split('.'))
        {
            if (label.Length is < 1 or > 63 || label[0] == '-' || label[^1] == '-')
                throw new ArgumentException("Invalid DNS label.", nameof(dnsName));
            foreach (var character in label)
                if (character is not (>= 'a' and <= 'z') and not (>= '0' and <= '9') and not '-')
                    throw new ArgumentException(
                        "DNS names must use lowercase ASCII labels.",
                        nameof(dnsName)
                    );
        }
        ArgumentOutOfRangeException.ThrowIfZero(port);
        MemberId = memberId;
        DnsName = dnsName;
        Port = port;
    }

    public Guid MemberId { get; }
    public string DnsName { get; }
    public ushort Port { get; }
}

public sealed class DataGridMapConfiguration
{
    public const int GroupMaxKeyCount = 100000;
    public const long GroupMaxDataByteCount = 256L * 1024 * 1024;

    public DataGridMapConfiguration(
        Guid mapId,
        string name,
        DataGridMapOwnership ownership,
        string durableStateOwner,
        string refillOwner,
        int maxKeyCount,
        long maxDataByteCount,
        DataGridMapPolicy policy
    )
    {
        if (mapId == Guid.Empty)
            throw new ArgumentException("A map identity is required.", nameof(mapId));
        ValidateText(name, 128, allowEmpty: false);
        ValidateText(durableStateOwner, 256, allowEmpty: false);
        ValidateText(refillOwner, 256, allowEmpty: true);
        if (ownership is not (DataGridMapOwnership.Authoritative or DataGridMapOwnership.Derived))
            throw new ArgumentOutOfRangeException(nameof(ownership));
        if ((ownership == DataGridMapOwnership.Derived) == (refillOwner.Length == 0))
            throw new ArgumentException(
                "Refill responsibility is required only for a derived map.",
                nameof(refillOwner)
            );
        if (maxKeyCount is < 1 or > GroupMaxKeyCount)
            throw new ArgumentOutOfRangeException(nameof(maxKeyCount));
        if (maxDataByteCount is < 1 or > GroupMaxDataByteCount)
            throw new ArgumentOutOfRangeException(nameof(maxDataByteCount));
        if (
            policy.Consistency != 1
            || policy.Durability != 1
            || policy.Expiration is not (1 or 2)
            || policy.Eviction != 1
            || policy.Schema != 1
        )
            throw new ArgumentException("Unsupported v1 map policy.", nameof(policy));
        MapId = mapId;
        Name = name;
        Ownership = ownership;
        DurableStateOwner = durableStateOwner;
        RefillOwner = refillOwner;
        MaxKeyCount = maxKeyCount;
        MaxDataByteCount = maxDataByteCount;
        Policy = policy;
    }

    public Guid MapId { get; }
    public string Name { get; }
    public DataGridMapOwnership Ownership { get; }
    public string DurableStateOwner { get; }
    public string RefillOwner { get; }
    public int MaxKeyCount { get; }
    public long MaxDataByteCount { get; }
    public DataGridMapPolicy Policy { get; }

    internal static void ValidateText(string value, int maxBytes, bool allowEmpty)
    {
        ArgumentNullException.ThrowIfNull(value);
        var length = new UTF8Encoding(false, true).GetByteCount(value);
        if (
            (!allowEmpty && length == 0)
            || length > maxBytes
            || value.Contains('\0', StringComparison.Ordinal)
        )
            throw new ArgumentException("Invalid configuration text.", nameof(value));
    }
}

/// <summary>Canonical immutable v1 bootstrap configuration and SHA-256 digest.</summary>
/// <remarks>This encoder validates declarations, not endpoint reachability, ACLs or storage recovery.</remarks>
public sealed class DataGridBootstrapConfiguration
{
    public const int MaxEncodedByteCount = 16384;
    private readonly DataGridMemberConfiguration[] _members;
    private readonly DataGridMapConfiguration[] _maps;
    private readonly byte[] _canonical;

    public DataGridBootstrapConfiguration(
        Guid incarnationId,
        IReadOnlyList<DataGridMemberConfiguration> members,
        IReadOnlyList<DataGridMapConfiguration> maps
    )
    {
        if (incarnationId == Guid.Empty)
            throw new ArgumentException("An incarnation is required.", nameof(incarnationId));
        ArgumentNullException.ThrowIfNull(members);
        ArgumentNullException.ThrowIfNull(maps);
        if (members.Count != 3 || maps.Count is < 1 or > 16)
            throw new ArgumentException("V1 requires three members and one to sixteen maps.");
        _members = new DataGridMemberConfiguration[3];
        _maps = new DataGridMapConfiguration[maps.Count];
        for (var i = 0; i < _members.Length; i++)
            _members[i] =
                members[i] ?? throw new ArgumentException("Null member.", nameof(members));
        for (var i = 0; i < _maps.Length; i++)
            _maps[i] = maps[i] ?? throw new ArgumentException("Null map.", nameof(maps));
        Array.Sort(_members, static (x, y) => CompareIds(x.MemberId, y.MemberId));
        Array.Sort(_maps, static (x, y) => CompareIds(x.MapId, y.MapId));
        for (var i = 0; i < _members.Length; i++)
        for (var j = 0; j < i; j++)
            if (
                _members[i].MemberId == _members[j].MemberId
                || (
                    _members[i].DnsName == _members[j].DnsName
                    && _members[i].Port == _members[j].Port
                )
            )
                throw new ArgumentException(
                    "Duplicate member identity or endpoint.",
                    nameof(members)
                );
        long keys = 0,
            bytes = 0;
        for (var i = 0; i < _maps.Length; i++)
        {
            for (var j = 0; j < i; j++)
                if (_maps[i].MapId == _maps[j].MapId || _maps[i].Name == _maps[j].Name)
                    throw new ArgumentException("Duplicate map identity or name.", nameof(maps));
            keys += _maps[i].MaxKeyCount;
            bytes += _maps[i].MaxDataByteCount;
        }
        if (
            keys > DataGridMapConfiguration.GroupMaxKeyCount
            || bytes > DataGridMapConfiguration.GroupMaxDataByteCount
        )
            throw new ArgumentException("Declared map quotas exceed group capacity.", nameof(maps));
        IncarnationId = incarnationId;
        using var stream = new MemoryStream();
        WriteInteger(stream, 0x484c4746, 4); // HLGF
        WriteInteger(stream, 1, 2);
        WriteInteger(stream, 0, 2);
        WriteId(stream, incarnationId);
        WriteInteger(stream, 3, 2);
        WriteInteger(stream, (ulong)_maps.Length, 2);
        foreach (var member in _members)
        {
            WriteId(stream, member.MemberId);
            WriteText(stream, member.DnsName);
            WriteInteger(stream, member.Port, 2);
        }
        foreach (var map in _maps)
        {
            WriteId(stream, map.MapId);
            WriteText(stream, map.Name);
            stream.WriteByte((byte)map.Ownership);
            stream.WriteByte(map.Policy.Consistency);
            stream.WriteByte(map.Policy.Durability);
            stream.WriteByte(map.Policy.Expiration);
            stream.WriteByte(map.Policy.Eviction);
            stream.WriteByte(map.Policy.Schema);
            WriteInteger(stream, (ulong)map.MaxKeyCount, 4);
            WriteInteger(stream, (ulong)map.MaxDataByteCount, 8);
            WriteText(stream, map.DurableStateOwner);
            WriteText(stream, map.RefillOwner);
        }
        _canonical = stream.ToArray();
        if (_canonical.Length > MaxEncodedByteCount)
            throw new ArgumentException("Configuration exceeds its encoded limit.");
    }

    public Guid IncarnationId { get; }
    public int MapCount => _maps.Length;

    public DataGridMemberConfiguration GetMember(int index) => _members[index];

    public DataGridMapConfiguration GetMap(int index) => _maps[index];

    public byte[] Encode() => (byte[])_canonical.Clone();

    public byte[] ComputeDigest() => SHA256.HashData(_canonical);

    private static int CompareIds(Guid x, Guid y)
    {
        Span<byte> left = stackalloc byte[16];
        Span<byte> right = stackalloc byte[16];
        x.TryWriteBytes(left, bigEndian: true, out _);
        y.TryWriteBytes(right, bigEndian: true, out _);
        return left.SequenceCompareTo(right);
    }

    private static void WriteId(Stream stream, Guid id)
    {
        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes, bigEndian: true, out _);
        stream.Write(bytes);
    }

    private static void WriteInteger(Stream stream, ulong value, int width)
    {
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, value);
        stream.Write(bytes[(8 - width)..]);
    }

    private static void WriteText(Stream stream, string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        WriteInteger(stream, (ulong)bytes.Length, 2);
        stream.Write(bytes);
    }
}
