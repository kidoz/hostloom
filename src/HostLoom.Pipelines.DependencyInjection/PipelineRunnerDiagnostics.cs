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
        "Time one pipeline run spent from scope creation to completion."
    );

    internal static readonly UpDownCounter<long> ActiveRuns = Meter.CreateUpDownCounter<long>(
        "hostloom.pipeline.run.active",
        "{run}",
        "Pipeline runs currently executing."
    );
}
