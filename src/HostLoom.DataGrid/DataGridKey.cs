using System.Text;

namespace HostLoom.DataGrid;

/// <summary>An owned, nonempty byte key with ordinal byte equality.</summary>
/// <remarks>Text keys use strict UTF-8 without normalization. Hash codes are not routing identifiers.</remarks>
public sealed class DataGridKey : IEquatable<DataGridKey>
{
    /// <summary>The initial protocol's maximum encoded key size.</summary>
    public const int MaxByteCount = 1024;

    private static readonly UTF8Encoding TextEncoding = new(false, true);
    private readonly byte[] _bytes;

    /// <summary>Copies the supplied bytes. Subsequent caller mutations cannot change this key.</summary>
    public DataGridKey(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty || bytes.Length > MaxByteCount)
            throw new ArgumentOutOfRangeException(nameof(bytes));
        _bytes = bytes.ToArray();
    }

    /// <summary>The encoded byte count.</summary>
    public int ByteCount => _bytes.Length;

    /// <summary>Encodes text with strict UTF-8; invalid UTF-16 is rejected rather than replaced.</summary>
    public static DataGridKey FromUtf8(string value)
    {
        ArgumentNullException.ThrowIfNull(value);
        var count = TextEncoding.GetByteCount(value);
        if (count is 0 or > MaxByteCount)
            throw new ArgumentOutOfRangeException(nameof(value));
        return new DataGridKey(TextEncoding.GetBytes(value));
    }

    /// <summary>Returns a copy that the caller may mutate.</summary>
    public byte[] ToArray() => (byte[])_bytes.Clone();

    /// <summary>Copies this key to caller-owned storage.</summary>
    public void CopyTo(Span<byte> destination) => _bytes.AsSpan().CopyTo(destination);

    public bool Equals(DataGridKey? other) =>
        other is not null && _bytes.AsSpan().SequenceEqual(other._bytes);

    internal int CompareBytesTo(DataGridKey other) =>
        _bytes.AsSpan().SequenceCompareTo(other._bytes);

    public override bool Equals(object? obj) => obj is DataGridKey other && Equals(other);

    public override int GetHashCode()
    {
        // Stable FNV-1a for local dictionaries only; no partition assignment is defined.
        var hash = 2166136261u;
        foreach (var value in _bytes)
            hash = unchecked((hash ^ value) * 16777619u);
        return unchecked((int)hash);
    }
}
