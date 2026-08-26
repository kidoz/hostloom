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
        // Rendered exactly once, through the caller's own formatter.
        entry.AppendLiteral(formatter(state, exception));
        CaptureState(entry, state);
        Emit(entry, eventId, exception);
    }

    private static void CaptureState<TState>(LogEntry entry, TState state)
    {
        if (state is IReadOnlyList<KeyValuePair<string, object?>> list)
        {
            for (var i = 0; i < list.Count; i++)
            {
                CapturePair(entry, list[i]);
            }

            return;
        }

        if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
        {
            foreach (var pair in pairs)
            {
                CapturePair(entry, pair);
            }
        }
    }

    private static void CapturePair(LogEntry entry, KeyValuePair<string, object?> pair)
    {
        var name = pair.Key;
        if (name == "{OriginalFormat}")
        {
            // The template is metadata, not a field: stored for template-aware formatters.
            entry.Template = pair.Value as string;
            return;
        }

        if (name.Length > 0 && (name[0] == '@' || name[0] == '$'))
        {
            // Serilog-compatible template operators: both strip their prefix from the emitted
            // name. '$' forces the invariant string; '@' asks for destructuring — until the
            // destructurer lands, a non-scalar '@' value degrades to the same invariant string.
            var stripped = name[1..];
            if (name[0] == '$')
            {
                CaptureStringified(entry, stripped, pair.Value);
                return;
            }

            CaptureValue(entry, stripped, pair.Value);
            return;
        }

        CaptureValue(entry, name, pair.Value);
    }

    private static void CaptureValue(LogEntry entry, string name, object? value)
    {
        switch (value)
        {
            case null:
                entry.AddFieldNull(name);
                break;
            case string text:
                entry.AddFieldText(name, text);
                break;
            case bool flag:
                entry.AddFieldBoolean(name, flag);
                break;
            case int number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number);
                break;
            case long number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number);
                break;
            case double number:
                entry.AddFieldFormattable(
                    name,
                    number,
                    double.IsFinite(number) ? LogFieldKind.Number : LogFieldKind.Text
                );
                break;
            case float number:
                entry.AddFieldFormattable(
                    name,
                    number,
                    float.IsFinite(number) ? LogFieldKind.Number : LogFieldKind.Text
                );
                break;
            case decimal number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number);
                break;
            case short number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number);
                break;
            case ushort number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number);
                break;
            case byte number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number);
                break;
            case sbyte number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number);
                break;
            case uint number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number);
                break;
            case ulong number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number);
                break;
            case Guid id:
                entry.AddFieldFormattable(name, id, LogFieldKind.Text);
                break;
            case DateTimeOffset when1:
                entry.AddFieldFormattable(name, when1, LogFieldKind.Text, "O");
                break;
            case DateTime when1:
                entry.AddFieldFormattable(name, when1, LogFieldKind.Text, "O");
                break;
            case TimeSpan duration:
                entry.AddFieldFormattable(name, duration, LogFieldKind.Text);
                break;
            case DateOnly day:
                entry.AddFieldFormattable(name, day, LogFieldKind.Text, "O");
                break;
            case TimeOnly time:
                entry.AddFieldFormattable(name, time, LogFieldKind.Text, "O");
                break;
            case char letter:
                entry.AddFieldText(name, new ReadOnlySpan<char>(in letter));
                break;
            case Enum:
                // The name, matching Serilog's scalar enum rendering, not the numeric value.
                entry.AddFieldText(name, value.ToString() ?? string.Empty);
                break;
            default:
                CaptureStringified(entry, name, value);
                break;
        }
    }

    /// <summary>A non-scalar (or '$'-forced) value: the invariant string representation. Full
    /// object destructuring for '@' holes is the destructurer's job, not this path's.</summary>
    private static void CaptureStringified(LogEntry entry, string name, object? value)
    {
        var text = value switch
        {
            null => string.Empty,
            IFormattable formattable => formattable.ToString(
                null,
                System.Globalization.CultureInfo.InvariantCulture
            ),
            _ => value.ToString() ?? string.Empty,
        };
        entry.AddFieldText(name, text);
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

        pipeline.Enqueue(entry);
    }
}
