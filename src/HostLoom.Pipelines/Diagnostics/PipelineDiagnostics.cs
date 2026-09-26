using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.Pipelines;

public static class PipelineDiagnostics
{
    public const string ActivitySourceName = "HostLoom.Pipelines";

    /// <summary>Meter name to enable when configuring OpenTelemetry.</summary>
    public const string MeterName = "HostLoom.Pipelines";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    internal static readonly Histogram<double> FilterDuration = Meter.CreateHistogram<double>(
        "hostloom.pipeline.filter.duration",
        "s",
        "Time one filter spent on its own work during one send, excluding the filters downstream of it.",
        tags: null,
        advice: new InstrumentAdvice<double>
        {
            // Operations: sub-millisecond hits up to slow calls of tens of seconds.
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

    internal static readonly Counter<long> FilterFailures = Meter.CreateCounter<long>(
        "hostloom.pipeline.filter.failures",
        "{failure}",
        "Filter invocations that faulted; cancellation is not counted as a failure."
    );
}
