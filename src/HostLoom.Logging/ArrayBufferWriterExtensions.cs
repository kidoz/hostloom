using System.Buffers;

namespace HostLoom.Logging;

internal static class ArrayBufferWriterExtensions
{
    /// <summary>
    /// Discards everything written after the first <paramref name="length"/> bytes, so a
    /// half-written record or element can be cut from a shared buffer. Resetting the written
    /// count leaves the buffer's contents in place, so advancing again re-commits the bytes
    /// before the cut unchanged and without a copy.
    /// </summary>
    public static void Truncate(this ArrayBufferWriter<byte> buffer, int length)
    {
        buffer.ResetWrittenCount();
        buffer.Advance(length);
    }
}
