using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace HostLoom.Transport.Kafka;

/// <summary>Deterministic consumer group names for transport and administration tools.</summary>
public static class KafkaConsumerGroupNames
{
    private const string NamingVersion = "v1";
    private static readonly Encoding Utf8 = new UTF8Encoding(false, true);

    /// <summary>Names a competing request-consumer group.</summary>
    public static string Request(string prefix, string address)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        var normalized = new RequestAddress(address).Value;
        return Encode("request", prefix, normalized);
    }

    /// <summary>Names one event subscription group; subscription names are ordinal and not trimmed.</summary>
    public static string Subscription(string prefix, string topic, string subscription)
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(prefix);
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        var normalized = new RequestAddress(topic).Value;
        return Encode("event", prefix, normalized, subscription);
    }

    internal static string Reply(string prefix, string instance) =>
        Encode("reply", prefix, instance);

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
        return $"hostloom.{NamingVersion}.{role}.{Convert.ToHexStringLower(hash.GetHashAndReset())}";
    }
}
