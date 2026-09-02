using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.Locking;

/// <summary>
/// Meter and activity source for <c>HostLoom.Locking</c>. Service identity stays in OpenTelemetry
/// resource attributes; the namespace travels as a tag, never in an instrument name.
/// </summary>
public static class LockingDiagnostics
{
    /// <summary>Activity source name to enable when configuring OpenTelemetry tracing.</summary>
    public const string ActivitySourceName = "HostLoom.Locking";

    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.Locking";

    /// <summary>Tag carrying <see cref="LockingOptions.Namespace"/> on every instrument.</summary>
    public const string NamespaceTag = "hostloom.lock.namespace";

    /// <summary>Tag on <c>hostloom.lock.acquire.duration</c>: <c>acquired</c>, <c>not_acquired</c>, or <c>unavailable</c>.</summary>
    public const string OutcomeTag = "hostloom.lock.outcome";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    private static readonly ConcurrentDictionary<DistributedLock, byte> Instances = new();

    internal static readonly Histogram<double> AcquireDuration = Meter.CreateHistogram<double>(
        "hostloom.lock.acquire.duration",
        "s",
        "Time spent acquiring a lease, including retries, by outcome."
    );

    internal static readonly Histogram<double> HoldDuration = Meter.CreateHistogram<double>(
        "hostloom.lock.hold.duration",
        "s",
        "Time between acquiring a lease and releasing it."
    );

    internal static readonly UpDownCounter<long> Active = Meter.CreateUpDownCounter<long>(
        "hostloom.lock.active",
        "{lock}",
        "Leases currently held by this process."
    );

    internal static readonly Counter<long> Lost = Meter.CreateCounter<long>(
        "hostloom.lock.lost",
        "{lease}",
        "Leases that expired or were refused by the provider before release."
    );

    // Declared after Meter and Instances on purpose: static initialisers run in textual order.
    private static readonly ObservableGauge<int> Enabled = Meter.CreateObservableGauge(
        "hostloom.lock.enabled",
        ObserveEnabled,
        "{state}",
        "1 when the lock coordinates across instances, 0 in single-instance mode."
    );

    internal static void Register(DistributedLock instance) => Instances.TryAdd(instance, 0);

    internal static void Unregister(DistributedLock instance) =>
        Instances.TryRemove(instance, out _);

    private static IEnumerable<Measurement<int>> ObserveEnabled()
    {
        _ = Enabled;
        foreach (var instance in Instances.Keys)
        {
            yield return new Measurement<int>(
                instance.Enabled ? 1 : 0,
                new KeyValuePair<string, object?>(NamespaceTag, instance.Namespace)
            );
        }
    }
}
