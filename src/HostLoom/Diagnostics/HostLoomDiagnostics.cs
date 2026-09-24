using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom;

public static class HostLoomDiagnostics
{
    public const string ActivitySourceName = "HostLoom";

    /// <summary>Meter name to enable when configuring OpenTelemetry.</summary>
    public const string MeterName = "HostLoom";

    /// <summary>
    /// Tag on the <c>hostloom.request.*</c> instruments naming what was received, because
    /// requests and event deliveries share them: <see cref="RequestKind"/> or <see cref="EventKind"/>.
    /// </summary>
    internal const string MessageKindTag = "hostloom.message.kind";

    internal const string RequestKind = "request";

    internal const string EventKind = "event";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    internal static readonly Histogram<double> RequestDuration = Meter.CreateHistogram<double>(
        "hostloom.request.duration",
        "s",
        "Time spent handling one inbound request or event delivery, including any receive-pipeline retries."
    );

    internal static readonly UpDownCounter<long> ActiveRequests = Meter.CreateUpDownCounter<long>(
        "hostloom.request.active",
        "{request}",
        "Inbound requests and event deliveries currently being handled."
    );

    internal static readonly Counter<long> Faults = Meter.CreateCounter<long>(
        "hostloom.request.faults",
        "{fault}",
        "Inbound requests answered with a fault envelope instead of a response, and event deliveries whose handlers failed."
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

    internal static readonly Counter<long> OutboxDeadLettered = Meter.CreateCounter<long>(
        "hostloom.outbox.dead_lettered",
        "{message}",
        "Outbox messages that exhausted their attempts and are no longer claimed."
    );

    internal static readonly Histogram<double> OutboxLag = Meter.CreateHistogram<double>(
        "hostloom.outbox.lag",
        "s",
        "Time between appending an outbox message and publishing it."
    );

    internal static readonly Counter<long> InboxDuplicates = Meter.CreateCounter<long>(
        "hostloom.inbox.duplicates",
        "{delivery}",
        "Redelivered events the inbox recognised and did not run handlers for."
    );
}
