using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom;

public static class HostLoomDiagnostics
{
    public const string ActivitySourceName = "HostLoom";

    /// <summary>Meter name to enable when configuring OpenTelemetry.</summary>
    public const string MeterName = "HostLoom";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    internal static readonly Histogram<double> RequestDuration = Meter.CreateHistogram<double>(
        "hostloom.request.duration",
        "s",
        "Time spent handling one inbound request, including any receive-pipeline retries."
    );

    internal static readonly UpDownCounter<long> ActiveRequests = Meter.CreateUpDownCounter<long>(
        "hostloom.request.active",
        "{request}",
        "Inbound requests currently being handled."
    );

    internal static readonly Counter<long> Faults = Meter.CreateCounter<long>(
        "hostloom.request.faults",
        "{fault}",
        "Inbound requests answered with a fault envelope instead of a response."
    );

    internal static readonly Counter<long> Retries = Meter.CreateCounter<long>(
        "hostloom.request.retries",
        "{retry}",
        "Handler invocations beyond the first, contributed by the receive pipeline."
    );

    internal static readonly Counter<long> OutboxPublished = Meter.CreateCounter<long>(
        "hostloom.outbox.published",
        "{message}",
        "Outbox messages the relay published and marked."
    );

    internal static readonly Counter<long> OutboxFailed = Meter.CreateCounter<long>(
        "hostloom.outbox.failed",
        "{attempt}",
        "Outbox publish attempts that failed and left the message pending."
    );

    internal static readonly Histogram<double> OutboxLag = Meter.CreateHistogram<double>(
        "hostloom.outbox.lag",
        "s",
        "Time between appending an outbox message and publishing it."
    );
}
