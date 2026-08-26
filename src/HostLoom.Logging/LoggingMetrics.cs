using System.Diagnostics.Metrics;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// The pipeline's health surface, published through a <see cref="Meter"/> so whatever
/// OpenTelemetry or Prometheus exporter the application already runs can scrape it. Loss is never
/// silent: every dropped record, blocked call, and component failure lands in an instrument.
/// Nothing here logs back through the pipeline it measures.
/// </summary>
internal sealed class LoggingMetrics : IDisposable
{
    public const string MeterName = "HostLoom.Logging";

    public const string ReasonQueueFull = "queue_full";
    public const string ReasonEnqueueTimeout = "enqueue_timeout";
    public const string ReasonWriterFault = "writer_fault";
    public const string ReasonProviderDisposed = "provider_disposed";
    public const string ReasonShutdownTimeout = "shutdown_timeout";

    public const string ComponentFormatter = "formatter";
    public const string ComponentSink = "sink";

    private readonly Meter _meter;
    private readonly Counter<long> _dropped;
    private readonly Counter<long> _blocked;
    private readonly Histogram<double> _blockedDuration;
    private readonly Counter<long> _failures;

    public LoggingMetrics(Func<int> queueDepth, Func<bool> writerHealthy, Func<string> writerState)
    {
        _meter = new Meter(MeterName);
        _dropped = _meter.CreateCounter<long>(
            "hostloom.logging.records.dropped",
            unit: "{record}",
            description: "Log records dropped instead of written."
        );
        _blocked = _meter.CreateCounter<long>(
            "hostloom.logging.enqueue.blocked",
            unit: "{call}",
            description: "Log calls that blocked because the queue was full."
        );
        _blockedDuration = _meter.CreateHistogram<double>(
            "hostloom.logging.enqueue.blocked.duration",
            unit: "s",
            description: "Time log calls spent blocked on a full queue."
        );
        _failures = _meter.CreateCounter<long>(
            "hostloom.logging.failures",
            unit: "{failure}",
            description: "Unexpected component failures inside the logging pipeline."
        );
        _meter.CreateObservableGauge(
            "hostloom.logging.queue.depth",
            () => (long)queueDepth(),
            unit: "{record}",
            description: "Records waiting in the bounded queue."
        );
        _meter.CreateObservableGauge(
            "hostloom.logging.writer.state",
            () =>
                new Measurement<long>(
                    writerHealthy() ? 1L : 0L,
                    new KeyValuePair<string, object?>("state", writerState())
                ),
            unit: null,
            description: "1 while the background writer is healthy, 0 once faulted or disposed."
        );
    }

    public void RecordDropped(string reason, LogLevel level, long count = 1) =>
        _dropped.Add(
            count,
            new KeyValuePair<string, object?>("reason", reason),
            new KeyValuePair<string, object?>("level", LevelName(level))
        );

    public void RecordBlocked(LogLevel level) =>
        _blocked.Add(1, new KeyValuePair<string, object?>("level", LevelName(level)));

    public void RecordBlockedFor(double seconds, LogLevel level) =>
        _blockedDuration.Record(
            seconds,
            new KeyValuePair<string, object?>("level", LevelName(level))
        );

    public void RecordFailure(string component) =>
        _failures.Add(1, new KeyValuePair<string, object?>("component", component));

    public void Dispose() => _meter.Dispose();

    private static string LevelName(LogLevel level) =>
        level switch
        {
            LogLevel.Trace => "Trace",
            LogLevel.Debug => "Debug",
            LogLevel.Information => "Information",
            LogLevel.Warning => "Warning",
            LogLevel.Error => "Error",
            LogLevel.Critical => "Critical",
            _ => "None",
        };
}
