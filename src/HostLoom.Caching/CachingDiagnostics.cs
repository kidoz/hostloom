using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.Caching;

/// <summary>
/// Meter and activity source for <c>HostLoom.Caching</c>. Instruments are named
/// <c>hostloom.cache.*</c>; identity lives in the <c>hostloom.cache.namespace</c> tag and never in
/// an instrument name, so one dashboard serves every cache.
/// </summary>
public static class CachingDiagnostics
{
    /// <summary>Activity source name to enable when configuring OpenTelemetry tracing.</summary>
    public const string ActivitySourceName = "HostLoom.Caching";

    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.Caching";

    internal const string NamespaceTag = "hostloom.cache.namespace";
    internal const string OperationTag = "hostloom.cache.operation";
    internal const string OutcomeTag = "hostloom.cache.outcome";
    internal const string DirectionTag = "hostloom.cache.direction";
    internal const string KindTag = "hostloom.cache.kind";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    private static readonly ConcurrentDictionary<TieredCache, byte> LiveCaches = new(
        ReferenceEqualityComparer.Instance
    );

    internal static readonly Histogram<double> OperationDuration = Meter.CreateHistogram<double>(
        "hostloom.cache.operation.duration",
        "s",
        "Time one cache operation took, tagged by operation and outcome."
    );

    internal static readonly Histogram<double> FactoryDuration = Meter.CreateHistogram<double>(
        "hostloom.cache.factory.duration",
        "s",
        "Time a get-or-create factory took."
    );

    internal static readonly Counter<long> StampedeLeaseMissed = Meter.CreateCounter<long>(
        "hostloom.cache.stampede.lease_missed",
        "{miss}",
        "Get-or-create calls that ran the factory without holding the cluster-wide lease."
    );

    internal static readonly Counter<long> Invalidations = Meter.CreateCounter<long>(
        "hostloom.cache.invalidations",
        "{message}",
        "Invalidation messages sent and received."
    );

    internal static readonly Counter<long> InvalidationResubscribedCounter =
        Meter.CreateCounter<long>(
            "hostloom.cache.invalidation.resubscribed",
            "{resubscribe}",
            "Times the invalidation subscription was re-established after a reconnect."
        );

    internal static readonly Counter<long> Errors = Meter.CreateCounter<long>(
        "hostloom.cache.errors",
        "{error}",
        "Distributed-store and serialization failures, tagged by kind."
    );

    internal static readonly Counter<long> Compressions = Meter.CreateCounter<long>(
        "hostloom.cache.compressions",
        "{payload}",
        "Payloads compressed before being written to the distributed tier."
    );

#pragma warning disable CA1823 // observable instruments are kept alive by the meter, not read.
    private static readonly ObservableGauge<long> Entries = Meter.CreateObservableGauge(
        "hostloom.cache.entries",
        static () => Observe(static cache => cache.LocalEntryCount),
        "{entry}",
        "Entries held in the in-process tier."
    );

    private static readonly ObservableGauge<long> GuardsActive = Meter.CreateObservableGauge(
        "hostloom.cache.guards.active",
        static () => Observe(static cache => cache.ActiveGuardCount),
        "{guard}",
        "Single-flight guards currently held or awaited."
    );
#pragma warning restore CA1823

    /// <summary>
    /// Records that an invalidation subscription was re-established after a reconnect. Called by
    /// backend packages, which own the connection and see the event.
    /// </summary>
    public static void InvalidationResubscribed(string @namespace) =>
        InvalidationResubscribedCounter.Add(
            1,
            new KeyValuePair<string, object?>(NamespaceTag, @namespace)
        );

    /// <summary>Counts one distributed-store failure for a composition that keeps no tag of its own.</summary>
    internal static void RecordStoreFailure(string @namespace, CacheFailureKind kind) =>
        Errors.Add(
            1,
            new KeyValuePair<string, object?>(NamespaceTag, @namespace),
            new KeyValuePair<string, object?>(KindTag, KindName(kind))
        );

    internal static string KindName(CacheFailureKind kind) =>
        kind switch
        {
            CacheFailureKind.Unavailable => "unavailable",
            CacheFailureKind.Timeout => "timeout",
            _ => "other",
        };

    internal static void Register(TieredCache cache) => LiveCaches[cache] = 0;

    internal static void Unregister(TieredCache cache) => LiveCaches.TryRemove(cache, out _);

    private static IEnumerable<Measurement<long>> Observe(Func<TieredCache, long> read)
    {
        foreach (var cache in LiveCaches.Keys)
        {
            yield return new Measurement<long>(
                read(cache),
                new KeyValuePair<string, object?>(NamespaceTag, cache.Namespace)
            );
        }
    }
}
