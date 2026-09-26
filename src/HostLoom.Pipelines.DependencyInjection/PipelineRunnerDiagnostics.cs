using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.Pipelines.DependencyInjection;

// Shares the meter and activity-source names with the core package, so enabling
// "HostLoom.Pipelines" in OpenTelemetry captures filter and run signals together.
internal static class PipelineRunnerDiagnostics
{
    internal static readonly ActivitySource ActivitySource = new(
        PipelineDiagnostics.ActivitySourceName
    );

    private static readonly Meter Meter = new(PipelineDiagnostics.MeterName);

    internal static readonly Histogram<double> RunDuration = Meter.CreateHistogram<double>(
        "hostloom.pipeline.run.duration",
        "s",
        "Time one pipeline run spent from start to completion, across every retry attempt.",
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

    internal static readonly UpDownCounter<long> ActiveRuns = Meter.CreateUpDownCounter<long>(
        "hostloom.pipeline.run.active",
        "{run}",
        "Pipeline runs currently executing."
    );
}
