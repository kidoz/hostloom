using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.Redis;

/// <summary>Meter and activity source for <c>HostLoom.Redis</c>.</summary>
public static class RedisDiagnostics
{
    /// <summary>Activity source name to enable when configuring OpenTelemetry tracing.</summary>
    public const string ActivitySourceName = "HostLoom.Redis";

    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.Redis";

    internal const string ClientTag = "hostloom.redis.client";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    private static readonly ConcurrentDictionary<RedisConnection, byte> LiveConnections = new(
        ReferenceEqualityComparer.Instance
    );

    internal static readonly Counter<long> Reconnects = Meter.CreateCounter<long>(
        "hostloom.redis.reconnects",
        "{reconnect}",
        "Times the Redis connection was restored after a failure."
    );

#pragma warning disable CA1823 // observable instruments are kept alive by the meter, not read.
    private static readonly ObservableGauge<long> ConnectionState = Meter.CreateObservableGauge(
        "hostloom.redis.connection.state",
        static () => Observe(),
        "{state}",
        "1 while the Redis connection is established, 0 while it is down or reconnecting."
    );
#pragma warning restore CA1823

    internal static void Register(RedisConnection connection) => LiveConnections[connection] = 0;

    internal static void Unregister(RedisConnection connection) =>
        LiveConnections.TryRemove(connection, out _);

    private static IEnumerable<Measurement<long>> Observe()
    {
        foreach (var connection in LiveConnections.Keys)
        {
            yield return new Measurement<long>(
                connection.IsConnected ? 1 : 0,
                new KeyValuePair<string, object?>(ClientTag, connection.Options.ClientName)
            );
        }
    }
}
