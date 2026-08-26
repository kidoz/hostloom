using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// The JSON shape of one structured field value, so a formatter can emit typed JSON instead of
/// quoting everything. Numbers, booleans, and fragments are complete tokens emitted raw; text must
/// be JSON-escaped by the formatter.
/// </summary>
// CA1028: the kind is stored in every pooled LogField; a byte keeps that struct compact.
#pragma warning disable CA1028
public enum LogFieldKind : byte
#pragma warning restore CA1028
{
    /// <summary>UTF-8 text. The formatter must JSON-escape it on the way out.</summary>
    Text,

    /// <summary>A complete JSON number token, safe to emit raw.</summary>
    Number,

    /// <summary><c>true</c> or <c>false</c>, safe to emit raw.</summary>
    Boolean,

    /// <summary>An explicit null. The value payload is empty.</summary>
    Null,

    /// <summary>
    /// A pre-validated JSON fragment (an object or an array), safe to emit raw. Only the library's
    /// own serialization writes this kind; nothing caller-supplied is passed through unvalidated.
    /// </summary>
    Json,
}

/// <summary>
/// A read-only view over one pooled entry, handed to formatters. A ref struct so it cannot outlive
/// the entry it borrows, and so nothing about a log line reaches the heap on the way to a sink.
/// </summary>
public readonly ref struct LogRecord
{
    private readonly LogEntry _entry;

    internal LogRecord(LogEntry entry)
    {
        _entry = entry;
    }

    /// <summary>Wall-clock time read on the calling thread when the event was captured.</summary>
    public DateTimeOffset Timestamp => _entry.Timestamp;

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

    /// <summary>
    /// Field values are slices of the entry's retained buffers, so reading them copies nothing.
    /// The kind tells the formatter whether the bytes are text to escape or a raw JSON token.
    /// </summary>
    public void GetField(
        int index,
        out ReadOnlySpan<byte> name,
        out ReadOnlySpan<byte> value,
        out LogFieldKind kind
    ) => _entry.GetField(index, out name, out value, out kind);

    /// <summary>
    /// The formatted text a caller-supplied format produced in the message ("025" for
    /// <c>{count:000}</c>), present only when it differs from the field value. CLEF-style
    /// formatters emit these as renderings; JSON formatters are free to ignore them.
    /// </summary>
    public bool TryGetRendering(int index, out ReadOnlySpan<byte> rendering) =>
        _entry.TryGetRendering(index, out rendering);
}
