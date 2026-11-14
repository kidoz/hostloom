using System.Security.Cryptography;

namespace HostLoom.DataGrid.Storage;

/// <summary>Reads and hashes exactly the selected prefix, leaving the underlying owner open.</summary>
internal sealed class DataGridWalPrefixStream(Stream source, long length, IncrementalHash digest)
    : Stream
{
    private long _position;
    public override bool CanRead => true;
    public override bool CanSeek => false;
    public override bool CanWrite => false;
    public override long Length => length;
    public override long Position
    {
        get => _position;
        set => throw new NotSupportedException();
    }

    public override int Read(byte[] buffer, int offset, int count) =>
        Read(buffer.AsSpan(offset, count));

    public override int Read(Span<byte> buffer)
    {
        var count = (int)Math.Min(buffer.Length, length - _position);
        if (count == 0)
            return 0;
        var read = source.Read(buffer[..count]);
        digest.AppendData(buffer[..read]);
        _position += read;
        return read;
    }

    public override int ReadByte()
    {
        Span<byte> value = stackalloc byte[1];
        return Read(value) == 0 ? -1 : value[0];
    }

    public override void Flush() => throw new NotSupportedException();

    public override long Seek(long offset, SeekOrigin origin) => throw new NotSupportedException();

    public override void SetLength(long value) => throw new NotSupportedException();

    public override void Write(byte[] buffer, int offset, int count) =>
        throw new NotSupportedException();
}
