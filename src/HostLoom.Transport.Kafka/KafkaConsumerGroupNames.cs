using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace HostLoom.Transport.Kafka;

/// <summary>Consumer group naming. Changing the scheme changes the committed-offset identity.</summary>
public enum KafkaConsumerGroupNaming
{
    /// <summary>Role-qualified SHA-256 identities over length-prefixed UTF-8 components.</summary>
    Version2,

    /// <summary>Original dotted names, for migration with existing committed offsets.</summary>
    Legacy,
}

/// <summary>Deterministic consumer group names for transport and administration tools.</summary>
public static class KafkaConsumerGroupNames
{
    private static readonly Encoding Utf8 = new UTF8Encoding(false, true);

    /// <summary>Names a competing request-consumer group.</summary>
    public static string Request(
        string prefix,
        string address,
        KafkaConsumerGroupNaming naming = KafkaConsumerGroupNaming.Version2
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        var normalized = new RequestAddress(address).Value;
        return naming switch
        {
            KafkaConsumerGroupNaming.Version2 => Encode("request", prefix, normalized),
            KafkaConsumerGroupNaming.Legacy => $"{prefix}.{normalized}",
            _ => throw new ArgumentOutOfRangeException(nameof(naming)),
        };
    }

    /// <summary>Names one event subscription group; subscription names are ordinal and not trimmed.</summary>
    public static string Subscription(
        string prefix,
        string topic,
        string subscription,
        KafkaConsumerGroupNaming naming = KafkaConsumerGroupNaming.Version2
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        var normalized = new RequestAddress(topic).Value;
        return naming switch
        {
            KafkaConsumerGroupNaming.Version2 => Encode("event", prefix, normalized, subscription),
            KafkaConsumerGroupNaming.Legacy => $"{prefix}.{normalized}.{subscription}",
            _ => throw new ArgumentOutOfRangeException(nameof(naming)),
        };
    }

    internal static string Reply(string prefix, string instance, KafkaConsumerGroupNaming naming) =>
        naming == KafkaConsumerGroupNaming.Legacy
            ? $"{prefix}.replies.{instance}"
            : Encode("reply", prefix, instance);

    private static string Encode(string role, params string[] components)
    {
        using var hash = IncrementalHash.CreateHash(HashAlgorithmName.SHA256);
        Span<byte> length = stackalloc byte[4];
        foreach (var component in components)
        {
            var bytes = Utf8.GetBytes(component);
            BinaryPrimitives.WriteInt32BigEndian(length, bytes.Length);
            hash.AppendData(length);
            hash.AppendData(bytes);
        }
        return $"hostloom.v2.{role}.{Convert.ToHexStringLower(hash.GetHashAndReset())}";
    }
}
