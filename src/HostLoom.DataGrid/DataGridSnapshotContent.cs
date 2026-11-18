using System.Security.Cryptography;

namespace HostLoom.DataGrid;

// Immutable owned canonical bytes. Runtime consumers never flatten the complete image.
internal sealed class DataGridSnapshotContent
{
    internal const int BlockBytes = 65536;
    private readonly byte[][] _blocks;

    private DataGridSnapshotContent(byte[][] blocks, int length)
    {
        _blocks = blocks;
        Length = length;
    }

    internal int Length { get; }
    internal int BlockCount => _blocks.Length;

    internal static DataGridSnapshotContent CopyFrom(ReadOnlySpan<byte> bytes)
    {
        if (bytes.IsEmpty)
            return new([], 0);
        using var builder = new Builder(bytes.Length);
        builder.Write(bytes);
        return builder.Complete();
    }

    // One borrowed contiguous region, bounded by a block even at an arbitrary offset.
    internal ReadOnlyMemory<byte> BlockAt(int offset)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(offset);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(offset, Length);
        return offset == Length
            ? ReadOnlyMemory<byte>.Empty
            : _blocks[offset / BlockBytes].AsMemory(offset % BlockBytes);
    }

    internal void CopyTo(int offset, Span<byte> destination)
    {
        CheckRange(offset, destination.Length, Length);
        while (!destination.IsEmpty)
        {
            var block = BlockAt(offset).Span;
            var count = Math.Min(block.Length, destination.Length);
            block[..count].CopyTo(destination);
            destination = destination[count..];
            offset += count;
        }
    }

    internal ReadOnlyMemory<byte> ReadValue(int offset, int length, bool borrow)
    {
        CheckRange(offset, length, Length);
        if (length > BlockBytes)
            throw new InvalidDataException("Snapshot field exceeds the bounded read size.");
        if (length == 0)
            return ReadOnlyMemory<byte>.Empty;
        var first = BlockAt(offset);
        if (borrow && length <= first.Length)
            return first[..length];
        var copy = new byte[length];
        CopyTo(offset, copy);
        return copy;
    }

    internal ReadOnlySpan<byte> ReadField(int offset, int length)
    {
        CheckRange(offset, length, Length);
        if (length > BlockBytes)
            throw new InvalidDataException("Snapshot field exceeds the bounded read size.");
        var first = BlockAt(offset);
        return length <= first.Length
            ? first.Span[..length]
            : ReadValue(offset, length, false).Span;
    }

    internal bool Matches(int offset, ReadOnlySpan<byte> bytes)
    {
        CheckRange(offset, bytes.Length, Length);
        return Matches(_blocks, offset, bytes);
    }

    internal byte[] ComputeHash()
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        foreach (var block in _blocks)
            hash.AppendData(block);
        return hash.GetHashAndReset();
    }

    // Only the explicit public-copy path uses an image-sized allocation.
    internal byte[] ToArray()
    {
        var bytes = new byte[Length];
        CopyTo(0, bytes);
        return bytes;
    }

    private static bool Matches(byte[][] blocks, int offset, ReadOnlySpan<byte> bytes)
    {
        while (!bytes.IsEmpty)
        {
            var block = blocks[offset / BlockBytes].AsSpan(offset % BlockBytes);
            var count = Math.Min(block.Length, bytes.Length);
            if (!bytes[..count].SequenceEqual(block[..count]))
                return false;
            bytes = bytes[count..];
            offset += count;
        }
        return true;
    }

    private static void CheckRange(int offset, int length, int total)
    {
        if (offset < 0 || length < 0 || offset > total || length > total - offset)
            throw new InvalidDataException("Snapshot range is outside its content.");
    }

    internal sealed class Builder : Stream
    {
        private byte[][]? _blocks;
        private readonly int _expectedLength;
        private int _position;
        private bool _completed;

        internal Builder(int length)
        {
            if (length <= 0 || length > DataGridSnapshotDescriptor.MaxSnapshotByteCount)
                throw new InvalidDataException("Invalid snapshot content size.");
            _expectedLength = length;
            _blocks = new byte[(length - 1) / BlockBytes + 1][];
        }

        internal int ExpectedLength => _expectedLength;
        public override bool CanRead => false;
        public override bool CanSeek => false;
        public override bool CanWrite => _blocks is not null && !_completed;
        public override long Length => _position;
        public override long Position
        {
            get => _position;
            set => throw new NotSupportedException();
        }

        public override void Write(ReadOnlySpan<byte> buffer)
        {
            EnsureWritable();
            CheckRange(_position, buffer.Length, _expectedLength);
            while (!buffer.IsEmpty)
            {
                var index = _position / BlockBytes;
                var block = _blocks![index] ??= new byte[
                    Math.Min(BlockBytes, _expectedLength - index * BlockBytes)
                ];
                var destination = block.AsSpan(_position % BlockBytes);
                var count = Math.Min(destination.Length, buffer.Length);
                buffer[..count].CopyTo(destination);
                buffer = buffer[count..];
                _position += count;
            }
        }

        internal bool MatchesWritten(int offset, ReadOnlySpan<byte> bytes)
        {
            EnsureWritable();
            CheckRange(offset, bytes.Length, _position);
            return Matches(_blocks!, offset, bytes);
        }

        internal DataGridSnapshotContent Complete()
        {
            EnsureWritable();
            if (_position != _expectedLength)
                throw new InvalidDataException("Incomplete snapshot content.");
            var blocks = _blocks!;
            _blocks = null;
            _completed = true;
            return new(blocks, _expectedLength);
        }

        private void EnsureWritable()
        {
            if (_completed)
                throw new InvalidOperationException(
                    "Snapshot content ownership already transferred."
                );
            ObjectDisposedException.ThrowIf(_blocks is null, this);
        }

        public override void Write(byte[] buffer, int offset, int count) =>
            Write(buffer.AsSpan(offset, count));

        public override void Flush() => EnsureWritable();

        public override int Read(byte[] buffer, int offset, int count) =>
            throw new NotSupportedException();

        public override long Seek(long offset, SeekOrigin origin) =>
            throw new NotSupportedException();

        public override void SetLength(long value) => throw new NotSupportedException();

        protected override void Dispose(bool disposing)
        {
            _blocks = null;
            base.Dispose(disposing);
        }
    }
}
