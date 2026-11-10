using System.Buffers.Binary;
using System.Text;

namespace HostLoom.DataGrid;

internal sealed class GridWriter(int limit) : IDisposable
{
    private readonly MemoryStream _stream = new();
    internal int Length => checked((int)_stream.Length);

    internal void Raw(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length > limit - _stream.Position)
            throw new InvalidOperationException("Encoded capacity exceeded.");
        _stream.Write(bytes);
    }

    internal void Number(ulong value, int width)
    {
        Span<byte> bytes = stackalloc byte[8];
        BinaryPrimitives.WriteUInt64BigEndian(bytes, value);
        Raw(bytes[(8 - width)..]);
    }

    internal void Id(Guid id)
    {
        Span<byte> bytes = stackalloc byte[16];
        id.TryWriteBytes(bytes, bigEndian: true, out _);
        Raw(bytes);
    }

    internal void Text(string value)
    {
        var bytes = Encoding.UTF8.GetBytes(value);
        Number((ulong)bytes.Length, 2);
        Raw(bytes);
    }

    internal void Key(DataGridKey? key)
    {
        var bytes = key?.ToArray() ?? [];
        Number((ulong)bytes.Length, 2);
        Raw(bytes);
    }

    internal int BeginSection(int id, int count)
    {
        Number((ulong)id, 2);
        Number(0, 2);
        Number((ulong)count, 4);
        var position = Length;
        Number(0, 4);
        return position;
    }

    internal void EndSection(int position)
    {
        var end = _stream.Position;
        _stream.Position = position;
        Number((ulong)(end - position - 4), 4);
        _stream.Position = end;
    }

    internal byte[] ToArray() => _stream.ToArray();

    public void Dispose() => _stream.Dispose();
}

internal ref struct GridReader(ReadOnlySpan<byte> bytes)
{
    private ReadOnlySpan<byte> _remaining = bytes;

    internal ReadOnlySpan<byte> Take(int length)
    {
        if (length < 0 || length > _remaining.Length)
            throw new InvalidDataException("Truncated binary data.");
        var result = _remaining[..length];
        _remaining = _remaining[length..];
        return result;
    }

    internal ulong Number(int width)
    {
        var data = Take(width);
        ulong value = 0;
        foreach (var part in data)
            value = (value << 8) | part;
        return value;
    }

    internal long Signed() => unchecked((long)Number(8));

    internal int Count(int width, int maximum)
    {
        var value = Number(width);
        if (value > (ulong)maximum)
            throw new InvalidDataException("Invalid binary count/length.");
        return (int)value;
    }

    internal bool Flag()
    {
        var value = Number(1);
        if (value > 1)
            throw new InvalidDataException("Invalid boolean.");
        return value == 1;
    }

    internal Guid Id(bool allowEmpty = false)
    {
        var id = new Guid(Take(16), bigEndian: true);
        if (!allowEmpty && id == Guid.Empty)
            throw new InvalidDataException("Empty binary identity.");
        return id;
    }

    internal DataGridTimestamp Time() => new(Signed());

    internal string Text(int maximum)
    {
        var length = Count(2, maximum);
        if (length == 0)
            throw new InvalidDataException("Empty text.");
        var text = new UTF8Encoding(false, true).GetString(Take(length));
        if (text.Contains('\0', StringComparison.Ordinal))
            throw new InvalidDataException("Invalid text.");
        return text;
    }

    internal DataGridKey? Key(bool allowEmpty)
    {
        var length = Count(2, DataGridKey.MaxByteCount);
        if (length == 0)
            return allowEmpty ? null : throw new InvalidDataException("Empty key.");
        return new(Take(length));
    }

    internal GridReader Section(int expectedId, int maximumCount, out int count)
    {
        if (Number(2) != (ulong)expectedId || Number(2) != 0)
            throw new InvalidDataException("Invalid snapshot section.");
        count = Count(4, maximumCount);
        var length = Count(4, (int)DataGridSnapshotDescriptor.MaxSnapshotByteCount);
        return new(Take(length));
    }

    internal void End()
    {
        if (!_remaining.IsEmpty)
            throw new InvalidDataException("Trailing binary bytes.");
    }
}
