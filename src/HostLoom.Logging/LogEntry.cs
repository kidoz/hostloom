using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// Where a field came from. Lower values win name collisions: an event hole beats a scope value,
/// which beats an enricher, which beats a static field. Within one source the last occurrence
/// wins. Only holes exist today; scopes, enrichers, and statics plug into the same ranking.
/// </summary>
internal enum LogFieldSource : byte
{
    Hole = 0,
    Scope = 1,
    Enricher = 2,
    Static = 3,
}

/// <summary>
/// Offsets of one structured field. The value slices the message buffer when the rendered text is
/// already a canonical token, and the value buffer when an explicit format made the two diverge.
/// A rendering length of -1 means the message text and the value are the same bytes. A name
/// length of -1 marks a field suppressed during normalization.
/// </summary>
internal readonly record struct LogField(
    int NameStart,
    int NameLength,
    int ValueStart,
    int ValueLength,
    LogFieldKind Kind,
    LogFieldSource Source,
    bool ValueInMessage,
    int RenderingStart,
    int RenderingLength
);

/// <summary>
/// One log record, rendered straight to UTF-8. Pooled and reused, so a steady-state write allocates
/// nothing: the message, the field names, and the field table all live in buffers the entry retains.
/// </summary>
internal sealed class LogEntry
{
    private const int MaxRetainedBuffer = 64 * 1024;

    private byte[] _message = new byte[512];
    private byte[] _names = new byte[256];
    private byte[] _values = new byte[256];
    private LogField[] _fields = new LogField[8];
    private int _messageLength;
    private int _namesLength;
    private int _valuesLength;
    private int _fieldCount;

    public LogLevel Level { get; set; }

    public string Category { get; set; } = string.Empty;

    /// <summary>The <c>{OriginalFormat}</c> message template when the standard path supplied one.
    /// Stored for template-aware formatters (CLEF <c>@mt</c>); never emitted as a field.</summary>
    public string? Template { get; set; }

    public EventId EventId { get; set; }

    public Exception? Exception { get; set; }

    /// <summary>Wall-clock time read at capture on the calling thread, never reconstructed later.</summary>
    public DateTimeOffset Timestamp { get; set; }

    public int ThreadId { get; set; }

    public ActivityTraceId TraceId { get; set; }

    public ActivitySpanId SpanId { get; set; }

    public bool HasActivity { get; set; }

    public ReadOnlySpan<byte> Message => _message.AsSpan(0, _messageLength);

    public int FieldCount => _fieldCount;

    public void GetField(
        int index,
        out ReadOnlySpan<byte> name,
        out ReadOnlySpan<byte> value,
        out LogFieldKind kind
    )
    {
        var field = _fields[index];
        name = _names.AsSpan(field.NameStart, field.NameLength);
        var source = field.ValueInMessage ? _message : _values;
        value = source.AsSpan(field.ValueStart, field.ValueLength);
        kind = field.Kind;
    }

    public bool TryGetRendering(int index, out ReadOnlySpan<byte> rendering)
    {
        var field = _fields[index];
        if (field.RenderingLength < 0)
        {
            rendering = default;
            return false;
        }

        rendering = _message.AsSpan(field.RenderingStart, field.RenderingLength);
        return true;
    }

    public void AppendLiteral(string value)
    {
        if (value.Length == 0)
        {
            return;
        }

        EnsureMessage(Encoding.UTF8.GetMaxByteCount(value.Length));
        _messageLength += Encoding.UTF8.GetBytes(value, _message.AsSpan(_messageLength));
    }

    /// <summary>
    /// Formats directly into the message buffer. The constraint keeps the call devirtualized, so a
    /// value type never boxes on the way in — the reason the concrete overloads exist on the handler.
    /// An explicit caller format may render a token JSON cannot hold ("00042", "FF", "1,234"), so
    /// the field value is then re-formatted canonically into the value buffer while the message
    /// keeps the rendering. <paramref name="canonicalFormat"/> is the format that produces the
    /// canonical token when the type's default format does not (ISO-8601 for date/time).
    /// </summary>
    public void AppendFormattable<T>(
        T value,
        string? format,
        string? name,
        LogFieldKind kind,
        string? canonicalFormat = null
    )
        where T : IUtf8SpanFormattable
    {
        var renderingStart = _messageLength;
        var messageFormat = format ?? canonicalFormat;
        int written;
        while (
            !value.TryFormat(
                _message.AsSpan(_messageLength),
                out written,
                messageFormat,
                CultureInfo.InvariantCulture
            )
        )
        {
            EnsureMessage(Math.Max(64, _message.Length));
        }

        _messageLength += written;
        if (name is null)
        {
            return;
        }

        if (format is null)
        {
            RecordField(name, kind, valueInMessage: true, renderingStart, _messageLength - renderingStart, 0, -1);
            return;
        }

        var valueStart = _valuesLength;
        while (
            !value.TryFormat(
                _values.AsSpan(_valuesLength),
                out written,
                canonicalFormat,
                CultureInfo.InvariantCulture
            )
        )
        {
            EnsureValues(Math.Max(64, _values.Length));
        }

        _valuesLength += written;
        RecordField(
            name,
            kind,
            valueInMessage: false,
            valueStart,
            written,
            renderingStart,
            _messageLength - renderingStart
        );
    }

    /// <summary>Booleans get their own path: <see cref="bool"/> has no UTF-8 formatter to constrain to.</summary>
    public void AppendBoolean(bool value, string? name)
    {
        var start = _messageLength;
        var text = value ? "true"u8 : "false"u8;
        EnsureMessage(text.Length);
        text.CopyTo(_message.AsSpan(_messageLength));
        _messageLength += text.Length;
        RecordField(name, LogFieldKind.Boolean, valueInMessage: true, start, _messageLength - start, 0, -1);
    }

    public void AppendText(ReadOnlySpan<char> value, string? name)
    {
        var start = _messageLength;
        EnsureMessage(Encoding.UTF8.GetMaxByteCount(value.Length));
        _messageLength += Encoding.UTF8.GetBytes(value, _message.AsSpan(_messageLength));
        RecordField(name, LogFieldKind.Text, valueInMessage: true, start, _messageLength - start, 0, -1);
    }

    public void Reset()
    {
        _messageLength = 0;
        _namesLength = 0;
        _valuesLength = 0;
        _fieldCount = 0;
        Exception = null;
        Category = string.Empty;
        Template = null;
        EventId = default;
        HasActivity = false;
    }

    /// <summary>
    /// Field writers for values that are not part of the rendered message — the standard
    /// <c>ILogger</c> path captures its state pairs through these. Values land in the value
    /// buffer; the message stays exactly what the caller's formatter rendered.
    /// </summary>
    public void AddFieldText(string name, ReadOnlySpan<char> value)
    {
        var start = _valuesLength;
        EnsureValues(Encoding.UTF8.GetMaxByteCount(value.Length));
        _valuesLength += Encoding.UTF8.GetBytes(value, _values.AsSpan(_valuesLength));
        RecordField(
            name,
            LogFieldKind.Text,
            valueInMessage: false,
            start,
            _valuesLength - start,
            0,
            -1
        );
    }

    public void AddFieldBoolean(string name, bool value)
    {
        var start = _valuesLength;
        var text = value ? "true"u8 : "false"u8;
        EnsureValues(text.Length);
        text.CopyTo(_values.AsSpan(_valuesLength));
        _valuesLength += text.Length;
        RecordField(name, LogFieldKind.Boolean, valueInMessage: false, start, text.Length, 0, -1);
    }

    public void AddFieldNull(string name) =>
        RecordField(name, LogFieldKind.Null, valueInMessage: false, _valuesLength, 0, 0, -1);

    public void AddFieldFormattable<T>(
        string name,
        T value,
        LogFieldKind kind,
        string? format = null
    )
        where T : IUtf8SpanFormattable
    {
        var start = _valuesLength;
        int written;
        while (
            !value.TryFormat(
                _values.AsSpan(_valuesLength),
                out written,
                format,
                CultureInfo.InvariantCulture
            )
        )
        {
            EnsureValues(Math.Max(64, _values.Length));
        }

        _valuesLength += written;
        RecordField(name, kind, valueInMessage: false, start, written, 0, -1);
    }

    private void RecordField(
        string? name,
        LogFieldKind kind,
        bool valueInMessage,
        int valueStart,
        int valueLength,
        int renderingStart,
        int renderingLength
    )
    {
        if (name is null)
        {
            return;
        }

        if (_fieldCount == _fields.Length)
        {
            Array.Resize(ref _fields, _fields.Length * 2);
        }

        var nameStart = _namesLength;
        var required = Encoding.UTF8.GetMaxByteCount(name.Length);
        if (_namesLength + required > _names.Length)
        {
            Array.Resize(ref _names, Math.Max(_names.Length * 2, _namesLength + required));
        }

        _namesLength += Encoding.UTF8.GetBytes(name, _names.AsSpan(_namesLength));
        _fields[_fieldCount++] = new LogField(
            nameStart,
            _namesLength - nameStart,
            valueStart,
            valueLength,
            kind,
            LogFieldSource.Hole,
            valueInMessage,
            renderingStart,
            renderingLength
        );
    }

    /// <summary>
    /// Applies the collision policy on the writer thread, before formatting: validates names,
    /// escapes a leading <c>@</c> to <c>@@</c>, resolves duplicates by source rank (last
    /// occurrence wins within a source), drops names the formatter reserves for itself, and
    /// enforces the caps. After this runs, the fields a formatter sees contain no duplicate and
    /// no reserved names, so even a <c>SkipValidation</c> writer cannot emit duplicate keys.
    /// Precedence losers are replaced silently — that is documented semantics; only invalid,
    /// reserved, and over-cap fields are counted as dropped.
    /// </summary>
    public void NormalizeFields(
        int maxNameLength,
        int maxFields,
        ILogFormatter formatter,
        LoggingMetrics? metrics
    )
    {
        if (_fieldCount == 0)
        {
            return;
        }

        for (var i = 0; i < _fieldCount; i++)
        {
            var field = _fields[i];
            if (field.NameLength == 0)
            {
                DropField(i, metrics, LoggingMetrics.FieldReasonEmptyName);
                continue;
            }

            if (field.NameLength > maxNameLength)
            {
                DropField(i, metrics, LoggingMetrics.FieldReasonNameTooLong);
                continue;
            }

            if (_names[field.NameStart] == (byte)'@')
            {
                EscapeName(i);
            }
        }

        for (var i = 0; i < _fieldCount; i++)
        {
            var field = _fields[i];
            if (field.NameLength < 0)
            {
                continue;
            }

            for (var j = 0; j < _fieldCount; j++)
            {
                if (j == i)
                {
                    continue;
                }

                var other = _fields[j];
                if (other.NameLength < 0 || !SameName(field, other))
                {
                    continue;
                }

                var beaten =
                    other.Source < field.Source || (other.Source == field.Source && j > i);
                if (beaten)
                {
                    _fields[i] = field with { NameLength = -1 };
                    break;
                }
            }
        }

        for (var i = 0; i < _fieldCount; i++)
        {
            var field = _fields[i];
            if (field.NameLength < 0)
            {
                continue;
            }

            if (formatter.OwnsFieldName(_names.AsSpan(field.NameStart, field.NameLength)))
            {
                DropField(i, metrics, LoggingMetrics.FieldReasonReserved);
            }
        }

        var write = 0;
        for (var i = 0; i < _fieldCount; i++)
        {
            var field = _fields[i];
            if (field.NameLength < 0)
            {
                continue;
            }

            if (write == maxFields)
            {
                metrics?.RecordFieldDropped(
                    LoggingMetrics.FieldReasonRecordCap,
                    SourceName(field.Source)
                );
                continue;
            }

            _fields[write++] = field;
        }

        _fieldCount = write;
    }

    private void DropField(int index, LoggingMetrics? metrics, string reason)
    {
        var field = _fields[index];
        metrics?.RecordFieldDropped(reason, SourceName(field.Source));
        _fields[index] = field with { NameLength = -1 };
    }

    /// <summary>CLEF-style escape: a user name beginning with <c>@</c> doubles the first
    /// <c>@</c>, so it can never impersonate a formatter-reified property.</summary>
    private void EscapeName(int index)
    {
        var field = _fields[index];
        var length = field.NameLength + 1;
        if (_namesLength + length > _names.Length)
        {
            Array.Resize(ref _names, Math.Max(_names.Length * 2, _namesLength + length));
        }

        var start = _namesLength;
        _names[start] = (byte)'@';
        _names.AsSpan(field.NameStart, field.NameLength).CopyTo(_names.AsSpan(start + 1));
        _namesLength += length;
        _fields[index] = field with { NameStart = start, NameLength = length };
    }

    private bool SameName(in LogField a, in LogField b) =>
        _names
            .AsSpan(a.NameStart, a.NameLength)
            .SequenceEqual(_names.AsSpan(b.NameStart, b.NameLength));

    private static string SourceName(LogFieldSource source) =>
        source switch
        {
            LogFieldSource.Scope => "scope",
            LogFieldSource.Enricher => "enricher",
            LogFieldSource.Static => "static",
            _ => "hole",
        };

    private void EnsureMessage(int additional)
    {
        if (_messageLength + additional <= _message.Length)
        {
            return;
        }

        Array.Resize(ref _message, Math.Max(_message.Length * 2, _messageLength + additional));
    }

    private void EnsureValues(int additional)
    {
        if (_valuesLength + additional <= _values.Length)
        {
            return;
        }

        Array.Resize(ref _values, Math.Max(_values.Length * 2, _valuesLength + additional));
    }

    /// <summary>Drops buffers a burst grew beyond the retention cap, so one huge line is not held forever.</summary>
    public void TrimIfOversized()
    {
        if (_message.Length > MaxRetainedBuffer)
        {
            _message = new byte[512];
        }

        if (_names.Length > MaxRetainedBuffer)
        {
            _names = new byte[256];
        }

        if (_values.Length > MaxRetainedBuffer)
        {
            _values = new byte[256];
        }
    }
}

/// <summary>
/// Shared free list. Entries cross from the calling thread to the writer thread, so a thread-local
/// pool cannot return them; Log4j2 hits the same constraint and solves it the same way.
/// </summary>
internal static class LogEntryPool
{
    private const int MaxRetained = 1024;

    private static readonly ConcurrentQueue<LogEntry> Free = new();
    private static int _retained;

    public static LogEntry Rent()
    {
        if (!Free.TryDequeue(out var entry))
        {
            return new LogEntry();
        }

        Interlocked.Decrement(ref _retained);
        entry.Reset();
        return entry;
    }

    public static void Return(LogEntry entry)
    {
        if (Interlocked.Increment(ref _retained) > MaxRetained)
        {
            Interlocked.Decrement(ref _retained);
            return;
        }

        entry.TrimIfOversized();
        Free.Enqueue(entry);
    }
}
