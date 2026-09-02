using System.Buffers;

namespace HostLoom.Caching.Internal;

/// <summary>
/// An <see cref="IBufferWriter{T}"/> over <see cref="ArrayPool{T}"/> memory, returned to the pool
/// on dispose. The written bytes are borrowed by whoever reads <see cref="WrittenMemory"/> until
/// the writer is disposed, which is the ownership rule the store contract states.
/// </summary>
internal sealed class PooledBufferWriter : IBufferWriter<byte>, IDisposable
{
    private byte[] _buffer;
    private int _written;

    public PooledBufferWriter(int initialCapacity = 256) =>
        _buffer = ArrayPool<byte>.Shared.Rent(initialCapacity);

    public ReadOnlyMemory<byte> WrittenMemory => _buffer.AsMemory(0, _written);

    public ReadOnlySpan<byte> WrittenSpan => _buffer.AsSpan(0, _written);

    public int WrittenCount => _written;

    public void Advance(int count)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(count);
        if (_written + count > _buffer.Length)
        {
            throw new InvalidOperationException("Cannot advance past the end of the buffer.");
        }

        _written += count;
    }

    public Memory<byte> GetMemory(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsMemory(_written);
    }

    public Span<byte> GetSpan(int sizeHint = 0)
    {
        EnsureCapacity(sizeHint);
        return _buffer.AsSpan(_written);
    }

    public void Reset() => _written = 0;

    public void Dispose()
    {
        var buffer = _buffer;
        _buffer = [];
        _written = 0;
        if (buffer.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(buffer);
        }
    }

    private void EnsureCapacity(int sizeHint)
    {
        if (sizeHint <= 0)
        {
            sizeHint = 1;
        }

        var available = _buffer.Length - _written;
        if (available >= sizeHint)
        {
            return;
        }

        var required = checked(_written + sizeHint);
        var size = Math.Max(required, _buffer.Length * 2);
        var replacement = ArrayPool<byte>.Shared.Rent(size);
        _buffer.AsSpan(0, _written).CopyTo(replacement);
        var old = _buffer;
        _buffer = replacement;
        if (old.Length > 0)
        {
            ArrayPool<byte>.Shared.Return(old);
        }
    }
}
