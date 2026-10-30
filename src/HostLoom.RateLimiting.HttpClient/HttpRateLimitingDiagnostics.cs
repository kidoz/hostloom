using System.Diagnostics.Metrics;

namespace HostLoom.RateLimiting.HttpClient;

/// <summary>Meter for outgoing HTTP rate-limit outcomes.</summary>
public static class HttpRateLimitingDiagnostics
{
    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.RateLimiting.HttpClient";

    /// <summary>Bounded outcome tag: sent, degraded, rejected, queue_full, or unavailable.</summary>
    public const string OutcomeTag = "hostloom.rate.http.outcome";

    private static readonly Meter Meter = new(MeterName);
    private static readonly Counter<long> Requests = Meter.CreateCounter<long>(
        "hostloom.ratelimit.http.requests",
        "{request}",
        "Outgoing HTTP requests by local rate-limit outcome."
    );

    private static readonly Histogram<double> WaitDuration = Meter.CreateHistogram<double>(
        "hostloom.ratelimit.http.wait.duration",
        "s",
        "Time spent acquiring an outgoing HTTP permit, including bounded local waits.",
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

    internal static void Record(string outcome, TimeSpan duration)
    {
        KeyValuePair<string, object?> tag = new(OutcomeTag, outcome);
        Requests.Add(1, tag);
        WaitDuration.Record(duration.TotalSeconds, tag);
    }
}
