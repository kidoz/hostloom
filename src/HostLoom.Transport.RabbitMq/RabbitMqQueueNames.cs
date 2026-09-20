using System.Buffers.Binary;
using System.Security.Cryptography;
using System.Text;

namespace HostLoom.Transport.RabbitMq;

/// <summary>The physical queue naming scheme; request producers and listeners must use the same mode.</summary>
public enum RabbitMqQueueNaming
{
    /// <summary>Role-qualified SHA-256 identities over length-prefixed UTF-8 components.</summary>
    Version2,

    /// <summary>Original raw request names and dotted subscription names, for migration only.</summary>
    Legacy,
}

/// <summary>Deterministic physical queue names, also usable by migration and administration tools.</summary>
public static class RabbitMqQueueNames
{
    private static readonly Encoding Utf8 = new UTF8Encoding(false, true);

    /// <summary>Names a request queue using the same address normalization as the broker.</summary>
    public static string Request(
        string address,
        RabbitMqQueueNaming naming = RabbitMqQueueNaming.Version2
    )
    {
        var normalized = new RequestAddress(address).Value;
        return naming switch
        {
            RabbitMqQueueNaming.Version2 => Encode("request", normalized),
            RabbitMqQueueNaming.Legacy => normalized,
            _ => throw new ArgumentOutOfRangeException(nameof(naming)),
        };
    }

    /// <summary>Names an event subscription queue. Subscription names are ordinal and are not trimmed.</summary>
    public static string Subscription(
        string topic,
        string subscription,
        RabbitMqQueueNaming naming = RabbitMqQueueNaming.Version2
    )
    {
        var normalized = new RequestAddress(topic).Value;
        ArgumentException.ThrowIfNullOrWhiteSpace(subscription);
        return naming switch
        {
            RabbitMqQueueNaming.Version2 => Encode("event", normalized, subscription),
            RabbitMqQueueNaming.Legacy => $"{normalized}.{subscription}",
            _ => throw new ArgumentOutOfRangeException(nameof(naming)),
        };
    }

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
