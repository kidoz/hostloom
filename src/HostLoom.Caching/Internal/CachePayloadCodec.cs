using System.Buffers;
using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;

namespace HostLoom.Caching.Internal;

/// <summary>Outcome of decoding a distributed-tier payload.</summary>
internal enum PayloadDecodeStatus
{
    Ok,

    /// <summary>Written by another payload format version; a silent miss during a rolling deploy.</summary>
    VersionMismatch,

    /// <summary>Malformed, or the serializer rejected it; a miss logged at error level.</summary>
    Corrupt,
}

/// <summary>
/// The distributed-tier envelope: one header byte (format version in the upper nibble, flags in
/// the lower), then, when the <c>tagged</c> flag is set, the tag names (a 16-bit count and
/// length-prefixed UTF-8 strings) so another instance can index the entry in its in-process tier,
/// then, when the <c>compressed</c> flag is set, the uncompressed body length as four
/// little-endian bytes, then the body.
/// </summary>
internal static class CachePayloadCodec
{
    public const byte FormatVersion = 1;
    private const byte CompressedFlag = 0x1;
    private const byte TaggedFlag = 0x2;
    private const int HeaderLength = 1;
    private const int LengthPrefix = sizeof(uint);
    private const int MaxTags = ushort.MaxValue;

    /// <summary>Serializes and wraps <paramref name="value"/>; the caller disposes the writer.</summary>
    /// <param name="bodyLength">
    /// The serialized body before compression. The caller bounds it, because a decoder trusts the
    /// declared uncompressed length only up to the same bound.
    /// </param>
    /// <returns>Whether the body was compressed.</returns>
    public static bool Encode<T>(
        ICacheValueSerializer serializer,
        T value,
        IReadOnlyCollection<string>? tags,
        int compressionThreshold,
        PooledBufferWriter destination,
        out int bodyLength
    )
    {
        using var raw = new PooledBufferWriter();
        serializer.Serialize(raw, value);
        var body = raw.WrittenSpan;
        bodyLength = body.Length;
        var tagged = tags is { Count: > 0 };

        // Compress first, into a scratch buffer, so the header is written once with the right
        // flag; an incompressible body is stored plain rather than larger than it started.
        using var scratch = new PooledBufferWriter();
        var compressed = 0;
        if (body.Length >= compressionThreshold)
        {
            var target = scratch.GetSpan(BrotliEncoder.GetMaxCompressedLength(body.Length));
            if (
                BrotliEncoder.TryCompress(body, target, out compressed, quality: 4, window: 22)
                && compressed < body.Length
            )
            {
                scratch.Advance(compressed);
            }
            else
            {
                compressed = 0;
            }
        }

        var compress = compressed > 0;
        var flags = (byte)((tagged ? TaggedFlag : 0) | (compress ? CompressedFlag : 0));
        var header = destination.GetSpan(HeaderLength);
        header[0] = (byte)((FormatVersion << 4) | flags);
        destination.Advance(HeaderLength);
        if (tagged)
        {
            WriteTags(tags!, destination);
        }

        if (compress)
        {
            var target = destination.GetSpan(LengthPrefix + compressed);
            BinaryPrimitives.WriteUInt32LittleEndian(target, (uint)body.Length);
            scratch.WrittenSpan.CopyTo(target[LengthPrefix..]);
            destination.Advance(LengthPrefix + compressed);
            return true;
        }

        var plain = destination.GetSpan(body.Length);
        body.CopyTo(plain);
        destination.Advance(body.Length);
        return false;
    }

    /// <summary>Unwraps and deserializes <paramref name="payload"/>.</summary>
    /// <param name="maxBodyBytes">
    /// Largest uncompressed body accepted. The length prefix comes from the store and a poisoned or
    /// truncated value can declare any size, so it buys a buffer only up to the bound the writer
    /// enforces.
    /// </param>
    public static PayloadDecodeStatus TryDecode<T>(
        ICacheValueSerializer serializer,
        ReadOnlySpan<byte> payload,
        long maxBodyBytes,
        out T? value,
        out string[]? tags,
        out Exception? failure
    )
    {
        value = default;
        tags = null;
        failure = null;
        if (payload.Length < HeaderLength)
        {
            return PayloadDecodeStatus.Corrupt;
        }

        var header = payload[0];
        if (header >> 4 != FormatVersion)
        {
            return PayloadDecodeStatus.VersionMismatch;
        }

        var body = payload[HeaderLength..];
        byte[]? rented = null;
        try
        {
            if ((header & TaggedFlag) != 0 && !TryReadTags(ref body, out tags))
            {
                return PayloadDecodeStatus.Corrupt;
            }

            if ((header & CompressedFlag) != 0)
            {
                if (body.Length < LengthPrefix)
                {
                    return PayloadDecodeStatus.Corrupt;
                }

                var declared = BinaryPrimitives.ReadUInt32LittleEndian(body);
                var cap = Math.Min(maxBodyBytes, Array.MaxLength);
                if (declared > cap)
                {
                    failure = new InvalidDataException(
                        $"The payload declares an uncompressed length of {declared} bytes, above the {cap} bytes Caching:MaxPayloadBytes allows."
                    );
                    return PayloadDecodeStatus.Corrupt;
                }

                var length = (int)declared;
                rented = ArrayPool<byte>.Shared.Rent(length);
                if (
                    !BrotliDecoder.TryDecompress(
                        body[LengthPrefix..],
                        rented.AsSpan(0, length),
                        out var written
                    )
                    || written != length
                )
                {
                    return PayloadDecodeStatus.Corrupt;
                }

                body = rented.AsSpan(0, length);
            }

            value = serializer.Deserialize<T>(body);
            return PayloadDecodeStatus.Ok;
        }
        catch (Exception exception) when (exception is not OperationCanceledException)
        {
            failure = exception;
            value = default;
            return PayloadDecodeStatus.Corrupt;
        }
        finally
        {
            if (rented is not null)
            {
                ArrayPool<byte>.Shared.Return(rented);
            }
        }
    }

    private static void WriteTags(IReadOnlyCollection<string> tags, PooledBufferWriter destination)
    {
        if (tags.Count > MaxTags)
        {
            throw new ArgumentException(
                $"An entry may carry at most {MaxTags} tags.",
                nameof(tags)
            );
        }

        var count = destination.GetSpan(sizeof(ushort));
        BinaryPrimitives.WriteUInt16LittleEndian(count, (ushort)tags.Count);
        destination.Advance(sizeof(ushort));
        foreach (var tag in tags)
        {
            var byteCount = Encoding.UTF8.GetByteCount(tag);
            if (byteCount > ushort.MaxValue)
            {
                throw new ArgumentException(
                    "A tag must encode to at most 65535 bytes.",
                    nameof(tags)
                );
            }

            var span = destination.GetSpan(sizeof(ushort) + byteCount);
            BinaryPrimitives.WriteUInt16LittleEndian(span, (ushort)byteCount);
            Encoding.UTF8.GetBytes(tag, span[sizeof(ushort)..]);
            destination.Advance(sizeof(ushort) + byteCount);
        }
    }

    private static bool TryReadTags(ref ReadOnlySpan<byte> body, out string[]? tags)
    {
        tags = null;
        if (body.Length < sizeof(ushort))
        {
            return false;
        }

        var count = BinaryPrimitives.ReadUInt16LittleEndian(body);
        body = body[sizeof(ushort)..];
        var read = new string[count];
        for (var i = 0; i < count; i++)
        {
            if (body.Length < sizeof(ushort))
            {
                return false;
            }

            var length = BinaryPrimitives.ReadUInt16LittleEndian(body);
            body = body[sizeof(ushort)..];
            if (body.Length < length)
            {
                return false;
            }

            read[i] = Encoding.UTF8.GetString(body[..length]);
            body = body[length..];
        }

        tags = read;
        return true;
    }
}
