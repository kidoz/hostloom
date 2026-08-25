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
        "Time one filter spent on its own work during one send, excluding the filters downstream of it."
    );

    internal static readonly Counter<long> FilterFailures = Meter.CreateCounter<long>(
        "hostloom.pipeline.filter.failures",
        "{failure}",
        "Filter invocations that faulted; cancellation is not counted as a failure."
    );
}
