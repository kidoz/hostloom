using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.RateLimiting;

/// <summary>Meter for backend-neutral rate-limit acquisition outcomes.</summary>
public static class RateLimitingDiagnostics
{
    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.RateLimiting";

    /// <summary>Tag carrying <see cref="RateLimitingOptions.Namespace"/>.</summary>
    public const string NamespaceTag = "hostloom.rate.namespace";

    /// <summary>Bounded acquisition outcome tag.</summary>
    public const string OutcomeTag = "hostloom.rate.outcome";

    /// <summary>Bounded store failure-kind tag.</summary>
    public const string FailureKindTag = "hostloom.rate.failure.kind";

    private static readonly Meter Meter = new(MeterName);

    private static readonly Counter<long> Acquisitions = Meter.CreateCounter<long>(
        "hostloom.ratelimit.acquisitions",
        "{acquisition}",
        "Asynchronous rate-limit decisions, including explicit degraded admissions."
    );

    private static readonly Counter<long> StoreErrors = Meter.CreateCounter<long>(
        "hostloom.ratelimit.store.errors",
        "{error}",
        "Rate-limit store failures by backend-neutral kind."
    );

    internal static readonly Histogram<double> AcquireDuration = Meter.CreateHistogram<double>(
        "hostloom.ratelimit.acquire.duration",
        "s",
        "Time spent obtaining one asynchronous rate-limit decision.",
        advice: new InstrumentAdvice<double>
        {
            HistogramBucketBoundaries =
            [
                0.0001,
                0.00025,
                0.0005,
                0.001,
                0.0025,
                0.005,
                0.01,
                0.025,
                0.05,
                0.1,
                0.25,
                0.5,
                1,
                2.5,
                5,
                10,
                30,
            ],
        }
    );

    internal static void Record(string ns, string outcome, TimeSpan duration, string? failureKind)
    {
        KeyValuePair<string, object?> namespaceTag = new(NamespaceTag, ns);
        KeyValuePair<string, object?> outcomeTag = new(OutcomeTag, outcome);
        Acquisitions.Add(1, namespaceTag, outcomeTag);
        AcquireDuration.Record(duration.TotalSeconds, namespaceTag, outcomeTag);
        if (failureKind is not null)
        {
            StoreErrors.Add(
                1,
                namespaceTag,
                new KeyValuePair<string, object?>(FailureKindTag, failureKind)
            );
        }
    }

    internal static long Timestamp() => Stopwatch.GetTimestamp();

    internal static TimeSpan Elapsed(long timestamp) => Stopwatch.GetElapsedTime(timestamp);
}
