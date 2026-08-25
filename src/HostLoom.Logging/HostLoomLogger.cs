using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

internal sealed class HostLoomLogger(
    string category,
    LogPipeline pipeline,
    HostLoomLoggerOptions options
) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    /// <summary>
    /// The Microsoft.Extensions.Logging entry point, used by every library that is not calling the
    /// interpolated fast path. It boxes <paramref name="state"/> and builds a string, because the
    /// interface signature requires both — a cost the fast path exists to avoid.
    /// </summary>
    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        ArgumentNullException.ThrowIfNull(formatter);
        var entry = LogEntryPool.Rent();
        entry.Level = logLevel;
        entry.AppendLiteral(formatter(state, exception));
        Emit(entry, eventId, exception);
    }

    /// <summary>Fast path: the entry is already rendered, so only the ambient metadata is added.</summary>
    internal void Emit(LogEntry entry, EventId eventId, Exception? exception)
    {
        entry.Category = category;
        entry.EventId = eventId;
        entry.Exception = exception;
        entry.Timestamp = Stopwatch.GetTimestamp();
        entry.ThreadId = Environment.CurrentManagedThreadId;

        // Captured here, never on the writer thread: Activity.Current is ambient to this thread, so
        // reading it after the hand-off would attach the wrong trace, or none at all.
        if (options.CaptureActivity && Activity.Current is { } activity)
        {
            entry.TraceId = activity.TraceId;
            entry.SpanId = activity.SpanId;
            entry.HasActivity = true;
        }

        pipeline.Enqueue(entry);
    }
}
