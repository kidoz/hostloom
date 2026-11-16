using System.Buffers.Binary;
using System.Text;

namespace HostLoom.DataGrid;

internal sealed class GridWriter : IDisposable
{
    private readonly int _limit;
    private readonly Stream _stream;

    internal GridWriter(int limit, int initialCapacity = 0)
    {
        if (initialCapacity < 0 || initialCapacity > limit)
            throw new ArgumentOutOfRangeException(nameof(initialCapacity));
        _limit = limit;
        _stream = new MemoryStream(initialCapacity);
    }

    private GridWriter(ReadOnlyMemory<byte> expected)
    {
        _limit = expected.Length;
        _stream = new ComparisonStream(expected);
    }

    private GridWriter(DataGridSnapshotContent content)
    {
        _limit = content.Length;
        _stream = new ComparisonStream(content);
    }

    private GridWriter(DataGridSnapshotContent.Builder builder)
    {
        _limit = builder.ExpectedLength;
        _stream = builder;
    }

    internal static GridWriter Segmented(int length) =>
        new(new DataGridSnapshotContent.Builder(length));

    internal static GridWriter Comparing(DataGridSnapshotContent expected) => new(expected);

    internal static GridWriter Comparing(ReadOnlyMemory<byte> expected) => new(expected);

    internal DataGridSnapshotContent DetachSnapshotContent() =>
        _stream is DataGridSnapshotContent.Builder builder
            ? builder.Complete()
            : throw new InvalidOperationException("This writer does not own snapshot blocks.");

    internal void CompleteComparison()
    {
        if (_stream is not ComparisonStream comparison)
            throw new InvalidOperationException("This writer does not compare content.");
        comparison.Complete();
    }

    internal int Length => checked((int)_stream.Length);

    internal void Raw(ReadOnlySpan<byte> bytes)
    {
        if (bytes.Length > _limit - _stream.Position)
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

    internal byte[] ToArray() =>
        _stream is MemoryStream buffer
            ? buffer.ToArray()
            : throw new InvalidOperationException("A comparison writer has no output buffer.");

    internal byte[] DetachExactContent()
    {
        if (_stream is not MemoryStream buffer)
            throw new InvalidOperationException("A comparison writer has no output buffer.");
        if (buffer.Length != buffer.Capacity)
            throw new InvalidOperationException("Exact snapshot size disagrees with its encoding.");
        var bytes = buffer.GetBuffer();
        // The closed writer cannot modify the transferred array; other codecs keep ToArray.
        _stream.Dispose();
        return bytes;
    }

    internal void NumberAt(int position, ulong value, int width)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(width, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(width, 8);
        if (position < 0 || position > Length - width)
            throw new ArgumentOutOfRangeException(nameof(position));
        var end = _stream.Position;
        _stream.Position = position;
        Number(value, width);
        _stream.Position = end;
    }

    public void Dispose() => _stream.Dispose();

    // Borrows canonical bytes for forward comparison only. Every write is checked;
    // completion also rejects an unconsumed suffix. No image-sized output is retained.
    private sealed class ComparisonStream : Stream
    {
        private readonly ReadOnlyMemory<byte> _expected;
        private readonly DataGridSnapshotContent? _content;
        private readonly int _length;
        private int _position;
        private bool _disposed;

        internal ComparisonStream(ReadOnlyMemory<byte> expected)
        {
            _expected = expected;
            _length = expected.Length;
        }

        internal ComparisonStream(DataGridSnapshotContent expected)
        {
            _content = expected;
            _length = expected.Length;
        }

        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => !_disposed;
        public override long Length => _position;
        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        internal void Complete()
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_position != _length)
                throw new InvalidDataException("Canonical encoding has a different length.");
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (
                buffer.Length > _length - _position
                || !(
                    _content is null
                        ? buffer.SequenceEqual(_expected.Span.Slice(_position, buffer.Length))
                        : _content.Matches(_position, buffer)
                )
            )
                throw new InvalidDataException("Snapshot disagrees with the canonical encoding.");
            _position += buffer.Length;
        }

        public override void Write(byte[] buffer, int offset, int count) =>
            Write(buffer.AsSpan(offset, count));

        public override void Flush() => ObjectDisposedException.ThrowIf(_disposed, this);

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            _disposed = true;
            base.Dispose(disposing);
        }
    }
}

internal ref struct GridReader
{
    private ReadOnlySpan<byte> _remaining;
    private readonly DataGridSnapshotContent? _content;
    private int _position;
    private readonly int _end;

    internal GridReader(ReadOnlySpan<byte> bytes)
    {
        _remaining = bytes;
        _content = null;
        _position = 0;
        _end = 0;
    }

    internal GridReader(DataGridSnapshotContent content)
        : this(content, 0, content.Length) { }

    private GridReader(DataGridSnapshotContent content, int offset, int length)
    {
        _remaining = default;
        _content = content;
        _position = offset;
        _end = checked(offset + length);
    }

    private readonly int Remaining => _content is null ? _remaining.Length : _end - _position;

    internal GridReader Subreader(int length)
    {
        CheckLength(length);
        if (_content is null)
            return new(Take(length));
        var result = new GridReader(_content, _position, length);
        _position += length;
        return result;
    }

    internal ReadOnlyMemory<byte> Value(int length, bool borrow)
    {
        CheckLength(length);
        if (_content is null)
            return Take(length).ToArray();
        var result = _content.ReadValue(_position, length, borrow);
        _position += length;
        return result;
    }

    private readonly void CheckLength(int length)
    {
        if (length < 0 || length > Remaining)
            throw new InvalidDataException("Truncated binary data.");
    }

    internal ReadOnlySpan<byte> Take(int length)
    {
        CheckLength(length);
        if (_content is not null)
        {
            var data = _content.ReadField(_position, length);
            _position += length;
            return data;
        }
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
        return Subreader(length);
    }

    internal void End()
    {
        if (Remaining != 0)
            throw new InvalidDataException("Trailing binary bytes.");
    }
}
