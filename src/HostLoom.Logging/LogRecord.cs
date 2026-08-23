using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// A read-only view over one pooled entry, handed to formatters. A ref struct so it cannot outlive
/// the entry it borrows, and so nothing about a log line reaches the heap on the way to a sink.
/// </summary>
public readonly ref struct LogRecord
{
    private readonly LogEntry _entry;

    internal LogRecord(LogEntry entry, DateTimeOffset timestamp)
    {
        _entry = entry;
        Timestamp = timestamp;
    }

    public DateTimeOffset Timestamp { get; }

    public LogLevel Level => _entry.Level;

    public string Category => _entry.Category;

    public EventId EventId => _entry.EventId;

    public Exception? Exception => _entry.Exception;

    public int ThreadId => _entry.ThreadId;

    public bool HasActivity => _entry.HasActivity;

    public ActivityTraceId TraceId => _entry.TraceId;

    public ActivitySpanId SpanId => _entry.SpanId;

    /// <summary>The rendered message, already UTF-8. Never transcoded from a string.</summary>
    public ReadOnlySpan<byte> Message => _entry.Message;

    public int FieldCount => _entry.FieldCount;

    /// <summary>Field values are slices of <see cref="Message"/>, so reading them copies nothing.</summary>
    public void GetField(int index, out ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value) =>
        _entry.GetField(index, out name, out value);
}
