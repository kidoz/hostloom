using System.Buffers;
using System.Text.Json;
using HostLoom.Caching;

namespace HostLoom.Valkey.Internal;

internal static class ValkeyInvalidationCodec
{
    internal const int MaxPayloadBytes = 1_048_576;
    private const int MaxItems = 10_000;

    internal static byte[] Encode(CacheInvalidation invalidation)
    {
        ArgumentNullException.ThrowIfNull(invalidation);
        var buffer = new ArrayBufferWriter<byte>();
        using var writer = new Utf8JsonWriter(buffer);
        writer.WriteStartArray();
        writer.WriteNumberValue(1);
        WriteItems(writer, invalidation.Keys);
        WriteItems(writer, invalidation.Tags);
        writer.WriteEndArray();
        writer.Flush();
        if (buffer.WrittenCount > MaxPayloadBytes)
            throw new ArgumentException("Invalidation payload is too large.", nameof(invalidation));
        return buffer.WrittenSpan.ToArray();
    }

    internal static CacheInvalidation? Decode(ReadOnlyMemory<byte> payload)
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
                || root.GetArrayLength() != 3
                || root[0].ValueKind != JsonValueKind.Number
                || !root[0].TryGetInt32(out var version)
                || version != 1
            )
                return null;
            var keys = ReadItems(root[1]);
            var tags = ReadItems(root[2]);
            return keys is null || tags is null ? null : new CacheInvalidation(keys, tags);
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

    private static string[]? ReadItems(JsonElement array)
    {
        if (array.ValueKind != JsonValueKind.Array || array.GetArrayLength() > MaxItems)
            return null;
        var items = new string[array.GetArrayLength()];
        var index = 0;
        foreach (var item in array.EnumerateArray())
        {
            if (item.ValueKind != JsonValueKind.String)
                return null;
            items[index++] = item.GetString()!;
        }
        return items;
    }
}
