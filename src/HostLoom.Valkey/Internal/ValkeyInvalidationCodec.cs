using System.Buffers;
using System.Text.Json;
using HostLoom.Caching;

namespace HostLoom.Valkey.Internal;

internal static class ValkeyInvalidationCodec
{
    internal const int MaxPayloadBytes = 1_048_576;
    internal const int MaxItems = 10_000;

    /// <summary>The kernel's default <c>Caching:MaxKeyLength</c>, used when no options are at hand.</summary>
    internal const int DefaultMaxKeyLength = 512;

    internal static byte[] Encode(CacheInvalidation invalidation)
    {
        ArgumentNullException.ThrowIfNull(invalidation);
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer);
        writer.WriteStartArray();
        writer.WriteNumberValue(1);
        WriteItems(writer, invalidation.Keys);
        WriteItems(writer, invalidation.Tags);
        if (invalidation.FlushAll)
        {
            // A fourth element. An instance on an earlier package version reads the longer array
            // as malformed and counts it, which is the documented behaviour for a flush it cannot apply.
            writer.WriteBooleanValue(true);
        }

        writer.WriteEndArray();
        writer.Flush();
        if (buffer.WrittenCount > MaxPayloadBytes)
            throw new ArgumentException("Invalidation payload is too large.", nameof(invalidation));
        return buffer.WrittenSpan.ToArray();
    }

    internal static CacheInvalidation? Decode(ReadOnlyMemory<byte> payload) =>
        Decode(payload, DefaultMaxKeyLength);

    /// <summary>
    /// Decodes a message received from the channel, or returns null when it is oversized, has
    /// too many items, or names a key or tag the kernel itself would reject (empty, longer than
    /// <paramref name="maxKeyLength"/>, or containing whitespace or control characters). The
    /// whole message is dropped on any violation so a publisher cannot smuggle one bad item in
    /// among good ones.
    /// </summary>
    internal static CacheInvalidation? Decode(ReadOnlyMemory<byte> payload, int maxKeyLength)
    {
        if (payload.Length > MaxPayloadBytes)
            return null;
        try
        {
            using var document = JsonDocument.Parse(
                payload,
                new JsonDocumentOptions { MaxDepth = 4 }
            );
            var root = document.RootElement;
            if (
                root.ValueKind != JsonValueKind.Array
                || root.GetArrayLength() is not (3 or 4)
                || root[0].ValueKind != JsonValueKind.Number
                || !root[0].TryGetInt32(out var version)
                || version != 1
            )
                return null;
            var flush = false;
            if (root.GetArrayLength() == 4)
            {
                if (root[3].ValueKind != JsonValueKind.True)
                    return null;
                flush = true;
            }
            var keys = ReadItems(root[1], maxKeyLength);
            var tags = ReadItems(root[2], maxKeyLength);
            return keys is null || tags is null
                ? null
                : new CacheInvalidation(keys, tags) { FlushAll = flush };
        }
        catch (JsonException)
        {
            return null;
        }
    }

    private static void WriteItems(Utf8JsonWriter writer, IReadOnlyCollection<string> items)
    {
        ArgumentNullException.ThrowIfNull(items);
        if (items.Count > MaxItems)
            throw new ArgumentException("Too many invalidation items.", nameof(items));
        writer.WriteStartArray();
        foreach (var item in items)
        {
            ArgumentNullException.ThrowIfNull(item);
            writer.WriteStringValue(item);
            if (writer.BytesPending + writer.BytesCommitted > MaxPayloadBytes)
                throw new ArgumentException("Invalidation payload is too large.", nameof(items));
        }
        writer.WriteEndArray();
    }

    private static string[]? ReadItems(JsonElement array, int maxKeyLength)
    {
        if (array.ValueKind != JsonValueKind.Array || array.GetArrayLength() > MaxItems)
            return null;
        var items = new string[array.GetArrayLength()];
        var index = 0;
        foreach (var item in array.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
                return null;
            var value = item.GetString();
            if (!CacheKey.IsValid(value, maxKeyLength))
                return null;
            items[index++] = value!;
        }
        return items;
    }
}
