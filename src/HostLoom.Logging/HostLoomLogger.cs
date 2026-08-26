using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

internal sealed class HostLoomLogger(
    string category,
    LogPipeline pipeline,
    HostLoomLoggerOptions options,
    HostLoomLoggerProvider provider
) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => provider.ScopeProvider.Push(state);

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    /// <summary>
    /// The Microsoft.Extensions.Logging entry point, used by every library that is not calling the
    /// interpolated fast path. It boxes <paramref name="state"/> and builds a string, because the
    /// interface signature requires both — a cost the fast path exists to avoid. Structured state
    /// (what <c>FormattedLogValues</c>, <c>LoggerMessage.Define</c>, and source-generated logger
    /// methods pass) is captured pair by pair as typed fields, so template holes stay queryable.
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
        var destructured = pipeline.Capture.CaptureState(entry, state);
        if (destructured && entry.Template is { } template)
        {
            // Safe rendering for '@' events: the MEL formatter would stringify the hole through
            // the value's ToString(), and a record type's generated ToString prints every member
            // — including what [NotLogged] and [LogMasked] just excluded. Render the message
            // from the captured, protected representations instead, the way Serilog does.
            EventCapture.RenderTemplate(entry, template);
        }
        else
        {
            // Rendered exactly once, through the caller's own formatter.
            entry.AppendLiteral(formatter(state, exception));
        }

        Emit(entry, eventId, exception);
    }

    /// <summary>Fast path: the entry is already rendered, so only the ambient metadata is added.</summary>
    internal void Emit(LogEntry entry, EventId eventId, Exception? exception)
    {
        entry.Category = category;
        entry.EventId = eventId;
        entry.Exception = exception;
        // The wall clock, not a Stopwatch anchor: an anchor set at startup never sees an NTP
        // correction, and over weeks of uptime the drift breaks cross-service correlation.
        entry.Timestamp = options.TimeProvider.GetUtcNow();
        entry.ThreadId = Environment.CurrentManagedThreadId;

        // Captured here, never on the writer thread: Activity.Current is ambient to this thread, so
        // reading it after the hand-off would attach the wrong trace, or none at all.
        if (options.CaptureActivity && Activity.Current is { } activity)
        {
            entry.TraceId = activity.TraceId;
            entry.SpanId = activity.SpanId;
            entry.HasActivity = true;
        }

        // Scopes are snapshotted here, on the producer thread, for the same reason as
        // Activity.Current: the ambient scope chain is gone once the entry crosses the queue.
        try
        {
            provider.ScopeProvider.ForEachScope(
                static (scope, state) => state.Capture.CaptureScope(scope, state.Entry),
                (Capture: pipeline.Capture, Entry: entry)
            );
        }
        catch (Exception)
        {
            // A scope provider that throws mid-walk costs the remaining scopes, never the event.
            pipeline.Metrics.RecordFailure(LoggingMetrics.ComponentScope);
        }

        if (entry.ScopeTexts is { Count: > 0 } scopeTexts)
        {
            EventCapture.AddScopeArray(entry, scopeTexts);
        }

        // Enrichers run here for the same reason: AsyncLocal context is gone after the hand-off.
        var enrichers = options.Enrichers;
        for (var i = 0; i < enrichers.Count; i++)
        {
            var writer = new LogEntryWriter(entry);
            try
            {
                enrichers[i].Enrich(ref writer);
            }
            catch (Exception)
            {
                // One broken enricher costs neither the event, the remaining enrichers, nor the
                // caller; the failure is counted instead of thrown.
                pipeline.Metrics.RecordFailure(LoggingMetrics.ComponentEnricher);
            }
        }

        var statics = pipeline.StaticFields;
        for (var i = 0; i < statics.Length; i++)
        {
            entry.AddFieldUtf8Text(statics[i].Name, statics[i].Value, LogFieldSource.Static);
        }

        pipeline.Enqueue(entry);
    }
}
