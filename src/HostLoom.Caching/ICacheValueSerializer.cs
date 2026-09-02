using System.Buffers;

namespace HostLoom.Caching;

/// <summary>
/// Turns values into distributed-tier payloads and back. Generic over <c>T</c> so a
/// source-generated serializer context works without reflection; the CLR type always comes from
/// the generic argument and never from payload content.
/// </summary>
public interface ICacheValueSerializer
{
    /// <summary>Writes <paramref name="value"/> to <paramref name="destination"/>.</summary>
    void Serialize<T>(IBufferWriter<byte> destination, T value);

    /// <summary>Reads a <typeparamref name="T"/> from <paramref name="payload"/>.</summary>
    T? Deserialize<T>(ReadOnlySpan<byte> payload);
}
