using System.Buffers;
using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Numerics;
using System.Text;
using System.Text.Unicode;
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

/// <summary>A borrowed caller name; stripping a template operator never copies the string.</summary>
internal readonly record struct LogFieldName(string? Text, int Offset = 0)
{
    public ReadOnlySpan<char> Span => Text.AsSpan(Offset);
    public int Length => Span.Length;

    public override string ToString() => Offset == 0 ? Text ?? string.Empty : Span.ToString();

    public static implicit operator LogFieldName(string? text) => new(text);
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
    int RenderingLength,
    LogFieldName CaptureName,
    bool NameRejected
);

/// <summary>
/// One log record, rendered straight to UTF-8. Pooled and reused, so a steady-state write allocates
/// nothing: the message, the field names, and the field table all live in buffers the entry retains.
/// </summary>
internal sealed class LogEntry
{
    private const int MaxRetainedBuffer = 64 * 1024;

    // Enough for complete canonical primitive tokens even when text caps are very small.
    private const int PrimitiveFormatBudget = 128;

    private byte[] _message = new byte[512];
    private byte[] _names = new byte[256];
    private byte[] _values = new byte[256];
    private LogField[] _fields = new LogField[8];
    private int _messageLength;
    private int _namesLength;
    private int _valuesLength;
    private int _fieldCount;
    private bool _deferNameValidation;
    private int _maxFieldNameLength = HostLoomLoggerOptions.DefaultMaxFieldNameLength;
    private int _maxMessageLength = HostLoomLoggerOptions.DefaultMaxMessageLength;
    private int _maxTextFieldLength = HostLoomLoggerOptions.DefaultMaxTextFieldLength;
    private int _maxCapturedFields = CaptureCeiling(
        HostLoomLoggerOptions.DefaultMaxFieldsPerRecord
    );
    private bool _messageTruncated;

    /// <summary>Fields refused at capture, by source, reported when the writer normalizes.</summary>
    private readonly int[] _overflow = new int[4];
    private readonly int[] _nameTooLong = new int[4];

    /// <summary>Open-addressing table of field indexes (plus one) for deduplicating large
    /// records; reused, and trimmed with the entry.</summary>
    private int[] _slots = [];

    public LogLevel Level { get; set; }

    /// <summary>Fast-path formatting failures, reported when the event reaches its provider.</summary>
    public int CaptureFailures { get; set; }

    /// <summary>Whether this entry participates in its pipeline's outstanding-record count.</summary>
    public bool AccountingPending { get; set; }

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

    /// <summary>Whether the message reached its cap and was closed with the "…" sentinel. Hole
    /// values captured afterwards still ship as fields; only the text stops growing.</summary>
    public bool MessageTruncated => _messageTruncated;

    /// <summary>
    /// Encoded destructured bytes this record may still spend, shared by its event holes and its
    /// scope values so one record's destructured output stays bounded as a whole. Negative until
    /// the first destructuring consumer initializes it from the options; spending clamps at zero.
    /// </summary>
    public int DestructuringBudget { get; set; } = -1;

    public int FieldCount => _fieldCount;

    /// <summary>Length of the retained field table, for the pool's trimming tests.</summary>
    internal int FieldCapacity => _fields.Length;

    /// <summary>Applies the record-size caps of the provider this entry is captured for. An entry
    /// rented for a foreign logger keeps the defaults.</summary>
    public void ApplyCaps(HostLoomLoggerOptions options)
    {
        _maxFieldNameLength = options.MaxFieldNameLength;
        _maxMessageLength = options.MaxMessageLength;
        _maxTextFieldLength = options.MaxTextFieldLength;
        _maxCapturedFields = CaptureCeiling(options.MaxFieldsPerRecord);
    }

    /// <summary>
    /// How many fields capture records before refusing more: four times the per-record cap. The
    /// exact cap applies later, after duplicates and reserved names are resolved on the writer
    /// thread; this ceiling only bounds what a pathological state (thousands of pairs) can make
    /// the caller encode and the writer examine. Fields past it are counted as over the cap.
    /// </summary>
    private static int CaptureCeiling(int maxFieldsPerRecord) =>
        (int)Math.Min(4L * maxFieldsPerRecord, int.MaxValue);

    /// <summary>Whether capture has reached its ceiling; a refused field is counted.</summary>
    private bool CaptureFull(LogFieldSource source)
    {
        if (_fieldCount < _maxCapturedFields)
        {
            return false;
        }

        _overflow[(int)source]++;
        return true;
    }

    // A foreign logger can dispatch to providers with different limits. Borrow names until
    // handoff; each receiving provider validates and encodes them under its own options.
    public void DeferNameValidation() => _deferNameValidation = true;

    public int OverflowHoleFields => _overflow[(int)LogFieldSource.Hole];

    public void ImportOverflowHoleFields(int count) => _overflow[(int)LogFieldSource.Hole] += count;

    public void GetHandoffField(
        int index,
        out string name,
        out ReadOnlySpan<byte> value,
        out LogFieldKind kind
    )
    {
        name = _fields[index].CaptureName.ToString();
        GetField(index, out _, out value, out kind);
    }

    /// <summary>Rendering is done. Drop rejected fields and release every caller-name reference
    /// before an entry crosses into the queue (or a synchronous formatter).</summary>
    public void FinalizeCapture()
    {
        var write = 0;
        for (var i = 0; i < _fieldCount; i++)
        {
            var field = _fields[i];
            if (!field.NameRejected)
                _fields[write++] = field with { CaptureName = default };
        }
        Array.Clear(_fields, write, _fieldCount - write);
        _fieldCount = write;
    }

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

    public void AppendLiteral(string value) => AppendText(value, null);

    /// <summary>Drop oversized template metadata only after safe rendering has used it. CLEF
    /// then emits the capped rendered message without retaining or emitting the large template.</summary>
    public void FinalizeTemplate()
    {
        if (
            Template is { } template
            && (
                template.Length > _maxMessageLength
                || Encoding.UTF8.GetByteCount(template) > _maxMessageLength
            )
        )
        {
            Template = null;
        }
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
        if (_messageTruncated)
        {
            // The message is closed; the value still ships as a field in its canonical form.
            if (name is not null)
            {
                AddFieldFormattable(name, value, kind, canonicalFormat);
            }

            return;
        }

        var renderingStart = _messageLength;
        // A small fixed allowance preserves complete primitive tokens even with tiny message
        // caps. Text formatters never get an input-sized buffer or an unlimited retry loop.
        var budget = Math.Max(PrimitiveFormatBudget, _maxMessageLength - _messageLength);
        if (format is null)
        {
            budget = Math.Max(budget, _maxTextFieldLength);
        }

        if (
            !TryFormatBounded(
                ref _message,
                _messageLength,
                budget,
                value,
                format ?? canonicalFormat,
                out var written,
                out var failed
            )
        )
        {
            if (failed && format is null)
            {
                AppendCaptureFailure(name);
            }
            else
            {
                // A display format can fail while the canonical value remains valid. Keep
                // its type and capture it independently of the failed message rendering.
                if (failed)
                    CaptureFailures++;
                AppendLiteral(failed ? "[DestructuringFailed]" : "…");
                if (name is not null)
                {
                    AddFieldFormattable(name, value, kind, canonicalFormat);
                }
            }
            return;
        }

        _messageLength += written;
        if (name is null)
        {
            CapMessage();
            return;
        }

        if (format is null && (kind != LogFieldKind.Text || written <= _maxTextFieldLength))
        {
            RecordField(name, kind, true, renderingStart, written, 0, -1);
            CapMessage();
            return;
        }

        var valueStart = _valuesLength;
        if (format is null)
        {
            // The rendering fitted the message budget but not the text-field budget.
            written = CopyText(
                ref _values,
                valueStart,
                _message.AsSpan(renderingStart, written),
                _maxTextFieldLength,
                out _
            );
        }
        else if (
            !TryFormatBounded(
                ref _values,
                valueStart,
                kind == LogFieldKind.Number
                    ? PrimitiveFormatBudget
                    : Math.Max(PrimitiveFormatBudget, _maxTextFieldLength),
                value,
                canonicalFormat,
                out written,
                out failed
            )
        )
        {
            _messageLength = renderingStart;
            if (failed)
            {
                AppendCaptureFailure(name);
            }
            else
            {
                AppendText("…", name);
            }
            return;
        }
        else if (kind == LogFieldKind.Text && written > _maxTextFieldLength)
        {
            written = CopyText(
                ref _values,
                valueStart,
                _values.AsSpan(valueStart, written),
                _maxTextFieldLength,
                out _
            );
        }

        _valuesLength += written;
        RecordField(
            name,
            kind,
            false,
            valueStart,
            written,
            format is null ? 0 : renderingStart,
            format is null ? -1 : _messageLength - renderingStart
        );
        CapMessage();
    }

    public void AppendCaptureFailure(string? name)
    {
        CaptureFailures++;
        AppendText("[DestructuringFailed]", name);
    }

    /// <summary>Caller formatting may fail or refuse every buffer. Neither can grow storage
    /// beyond the budget; partially written bytes are never committed.</summary>
    private static bool TryFormatBounded<T>(
        ref byte[] buffer,
        int start,
        int budget,
        T value,
        string? format,
        out int written,
        out bool failed
    )
        where T : IUtf8SpanFormattable
    {
        written = 0;
        failed = false;
        try
        {
            while (true)
            {
                var available = Math.Min(buffer.Length - start, budget);
                if (
                    value.TryFormat(
                        buffer.AsSpan(start, available),
                        out written,
                        format,
                        CultureInfo.InvariantCulture
                    )
                )
                {
                    if ((uint)written > (uint)available)
                    {
                        failed = true;
                        return false;
                    }
                    return true;
                }
                if (available == budget)
                {
                    return false;
                }
                var next = (int)Math.Min(budget, Math.Max(64L, (long)available * 2));
                EnsureTextBuffer(ref buffer, checked(start + next));
            }
        }
        catch (Exception)
        {
            failed = true;
            return false;
        }
    }

    /// <summary>Booleans get their own path: <see cref="bool"/> has no UTF-8 formatter to constrain to.</summary>
    public void AppendBoolean(bool value, string? name)
    {
        if (_messageTruncated)
        {
            if (name is not null)
            {
                AddFieldBoolean(name, value);
            }

            return;
        }

        var start = _messageLength;
        var text = value ? "true"u8 : "false"u8;
        EnsureMessage(text.Length);
        text.CopyTo(_message.AsSpan(_messageLength));
        _messageLength += text.Length;
        RecordField(
            name,
            LogFieldKind.Boolean,
            valueInMessage: true,
            start,
            _messageLength - start,
            0,
            -1
        );
        CapMessage();
    }

    public void AppendText(ReadOnlySpan<char> value, string? name)
    {
        if (name is not null && !CaptureFull(LogFieldSource.Hole))
        {
            // Keep the bounded field independent of the message budget, including when the
            // message is already closed or only part of this hole fits in it.
            var start = _valuesLength;
            AddFieldText(name, value);
            AppendUtf8ToMessage(_values.AsSpan(start, _valuesLength - start));
            return;
        }

        if (_messageTruncated || value.IsEmpty)
        {
            return;
        }

        _messageLength += EncodeText(
            ref _message,
            _messageLength,
            value,
            _maxMessageLength - _messageLength,
            out _messageTruncated
        );
    }

    private void AppendUtf8ToMessage(ReadOnlySpan<byte> value)
    {
        if (_messageTruncated)
        {
            return;
        }

        _messageLength += CopyText(
            ref _message,
            _messageLength,
            value,
            _maxMessageLength - _messageLength,
            out _messageTruncated
        );
    }

    public void Reset()
    {
        _messageLength = 0;
        CaptureFailures = 0;
        AccountingPending = false;
        _namesLength = 0;
        _valuesLength = 0;
        Array.Clear(_fields, 0, _fieldCount);
        _fieldCount = 0;
        _deferNameValidation = false;
        _maxFieldNameLength = HostLoomLoggerOptions.DefaultMaxFieldNameLength;
        _messageTruncated = false;
        _maxMessageLength = HostLoomLoggerOptions.DefaultMaxMessageLength;
        _maxTextFieldLength = HostLoomLoggerOptions.DefaultMaxTextFieldLength;
        _maxCapturedFields = CaptureCeiling(HostLoomLoggerOptions.DefaultMaxFieldsPerRecord);
        Array.Clear(_overflow);
        Array.Clear(_nameTooLong);
        DestructuringBudget = -1;
        Exception = null;
        Category = string.Empty;
        Template = null;
        EventId = default;
        HasActivity = false;
        ScopeTexts?.Clear();
        TemplateRenderings?.Clear();
    }

    /// <summary>
    /// Field writers for values that are not part of the rendered message — the standard
    /// <c>ILogger</c> path captures its state pairs through these. Values land in the value
    /// buffer; the message stays exactly what the caller's formatter rendered.
    /// </summary>
    public void AddFieldText(
        LogFieldName name,
        ReadOnlySpan<char> value,
        LogFieldSource source = LogFieldSource.Hole
    )
    {
        if (CaptureFull(source))
        {
            return;
        }

        var start = _valuesLength;
        var written = EncodeText(ref _values, start, value, _maxTextFieldLength, out _);
        _valuesLength = start + written;
        RecordField(name, LogFieldKind.Text, valueInMessage: false, start, written, 0, -1, source);
    }

    /// <summary>Copies an already-encoded UTF-8 value — the static-field path, where the value
    /// bytes were encoded once at provider start.</summary>
    public void AddFieldUtf8Text(
        LogFieldName name,
        ReadOnlySpan<byte> utf8Value,
        LogFieldSource source
    )
    {
        if (CaptureFull(source))
        {
            return;
        }

        var start = _valuesLength;
        var written = CopyText(ref _values, start, utf8Value, _maxTextFieldLength, out _);
        _valuesLength = start + written;
        RecordField(name, LogFieldKind.Text, valueInMessage: false, start, written, 0, -1, source);
    }

    public void AddFieldBoolean(
        LogFieldName name,
        bool value,
        LogFieldSource source = LogFieldSource.Hole
    )
    {
        if (CaptureFull(source))
        {
            return;
        }

        var start = _valuesLength;
        var text = value ? "true"u8 : "false"u8;
        EnsureValues(text.Length);
        text.CopyTo(_values.AsSpan(_valuesLength));
        _valuesLength += text.Length;
        RecordField(
            name,
            LogFieldKind.Boolean,
            valueInMessage: false,
            start,
            text.Length,
            0,
            -1,
            source
        );
    }

    public void AddFieldNull(LogFieldName name, LogFieldSource source = LogFieldSource.Hole) =>
        RecordField(
            name,
            LogFieldKind.Null,
            valueInMessage: false,
            _valuesLength,
            0,
            0,
            -1,
            source
        );

    /// <summary>
    /// Appends a captured field's value bytes to the message — the safe-render path for events
    /// with '@' holes, whose message must come from the masked representations rather than from
    /// a ToString() that may print excluded members. First matching name wins.
    /// </summary>
    public bool AppendFieldValueToMessage(ReadOnlySpan<char> name)
    {
        for (var i = 0; i < _fieldCount; i++)
        {
            var field = _fields[i];
            if (!field.CaptureName.Span.SequenceEqual(name))
            {
                continue;
            }

            if (_messageTruncated)
            {
                return true;
            }

            if (field.Kind == LogFieldKind.Null)
            {
                AppendUtf8ToMessage("null"u8);
                return true;
            }

            var source = field.ValueInMessage ? _message : _values;
            AppendUtf8ToMessage(source.AsSpan(field.ValueStart, field.ValueLength));
            return true;
        }

        return false;
    }

    /// <summary>
    /// Appends the value of the first field named <paramref name="name"/> at or after
    /// <paramref name="firstIndex"/> to <paramref name="target"/> — the scope-text renderer,
    /// which must resolve a hole against the fields captured for that one scope rather than
    /// against an event hole or an outer scope carrying the same name.
    /// </summary>
    public bool TryAppendFieldValue(ReadOnlySpan<char> name, int firstIndex, StringBuilder target)
    {
        for (var i = firstIndex; i < _fieldCount; i++)
        {
            var field = _fields[i];
            if (!field.CaptureName.Span.SequenceEqual(name))
            {
                continue;
            }

            if (field.Kind == LogFieldKind.Null)
            {
                target.Append("null");
                return true;
            }

            var source = field.ValueInMessage ? _message : _values;
            target.Append(Encoding.UTF8.GetString(source, field.ValueStart, field.ValueLength));
            return true;
        }

        return false;
    }

    /// <summary>A complete, pre-validated JSON fragment produced by the library itself.</summary>
    public void AddFieldJson(
        LogFieldName name,
        ReadOnlySpan<byte> json,
        LogFieldSource source = LogFieldSource.Hole
    )
    {
        if (CaptureFull(source))
        {
            return;
        }

        var start = _valuesLength;
        EnsureValues(json.Length);
        json.CopyTo(_values.AsSpan(_valuesLength));
        _valuesLength += json.Length;
        RecordField(
            name,
            LogFieldKind.Json,
            valueInMessage: false,
            start,
            json.Length,
            0,
            -1,
            source
        );
    }

    /// <summary>Rendered texts of active scopes, outer-to-inner, gathered during the producer-side
    /// scope walk and emitted as the <c>Scope</c> array. Retained with the pooled entry.</summary>
    public List<string>? ScopeTexts { get; private set; }

    public List<string> EnsureScopeTexts() => ScopeTexts ??= [];

    /// <summary>Renderings of the template's formatted tokens, in template order, captured on the
    /// standard path for CLEF <c>@r</c>. Retained with the pooled entry.</summary>
    public List<string>? TemplateRenderings { get; private set; }

    public List<string> EnsureTemplateRenderings() => TemplateRenderings ??= [];

    public void AddFieldFormattable<T>(
        LogFieldName name,
        T value,
        LogFieldKind kind,
        string? format = null,
        LogFieldSource source = LogFieldSource.Hole
    )
        where T : IUtf8SpanFormattable
    {
        if (CaptureFull(source))
        {
            return;
        }

        var start = _valuesLength;
        if (
            !TryFormatBounded(
                ref _values,
                start,
                kind == LogFieldKind.Number
                    ? PrimitiveFormatBudget
                    : Math.Max(PrimitiveFormatBudget, _maxTextFieldLength),
                value,
                format,
                out var written,
                out var failed
            )
        )
        {
            if (failed)
            {
                CaptureFailures++;
            }
            AddFieldText(name, failed ? "[DestructuringFailed]" : "…", source);
            return;
        }

        if (kind == LogFieldKind.Text && written > _maxTextFieldLength)
        {
            written = CopyText(
                ref _values,
                start,
                _values.AsSpan(start, written),
                _maxTextFieldLength,
                out _
            );
        }
        _valuesLength += written;
        RecordField(name, kind, valueInMessage: false, start, written, 0, -1, source);
    }

    private void RecordField(
        LogFieldName name,
        LogFieldKind kind,
        bool valueInMessage,
        int valueStart,
        int valueLength,
        int renderingStart,
        int renderingLength,
        LogFieldSource source = LogFieldSource.Hole
    )
    {
        // The AddField* writers check before encoding a value; the message-slicing fast path
        // reaches this check with its hole text already in the message, where it stays.
        if (name.Text is null || CaptureFull(source))
        {
            return;
        }

        // Keep the already captured value available to safe rendering, but never encode an
        // invalid name. Both names and values remain subject to the field capture ceiling.
        var rejected =
            !_deferNameValidation
            && (
                name.Length > _maxFieldNameLength
                || Encoding.UTF8.GetByteCount(name.Span) > _maxFieldNameLength
            );
        if (rejected)
            _nameTooLong[(int)source]++;

        if (_fieldCount == _fields.Length)
            Array.Resize(ref _fields, _fields.Length * 2);

        var nameStart = _namesLength;
        if (!rejected && !_deferNameValidation)
        {
            var required = Encoding.UTF8.GetMaxByteCount(name.Length);
            if (_namesLength + required > _names.Length)
                Array.Resize(ref _names, Math.Max(_names.Length * 2, _namesLength + required));
            _namesLength += Encoding.UTF8.GetBytes(name.Span, _names.AsSpan(_namesLength));
        }
        _fields[_fieldCount++] = new LogField(
            nameStart,
            _namesLength - nameStart,
            valueStart,
            valueLength,
            kind,
            source,
            valueInMessage,
            renderingStart,
            renderingLength,
            name,
            rejected
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
        FinalizeCapture();
        for (var source = 0; source < _overflow.Length; source++)
        {
            if (_nameTooLong[source] > 0)
            {
                metrics?.RecordFieldDropped(
                    LoggingMetrics.FieldReasonNameTooLong,
                    SourceName((LogFieldSource)source),
                    _nameTooLong[source]
                );
            }
            if (_overflow[source] > 0)
            {
                metrics?.RecordFieldDropped(
                    LoggingMetrics.FieldReasonRecordCap,
                    SourceName((LogFieldSource)source),
                    _overflow[source]
                );
            }
        }

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

        if (_fieldCount <= SmallRecordFields)
        {
            ResolveCollisionsPairwise();
        }
        else
        {
            ResolveCollisionsHashed();
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

    /// <summary>Records up to this many fields compare names pairwise, which beats hashing at
    /// the sizes nearly every record has.</summary>
    private const int SmallRecordFields = 16;

    /// <summary>Per name, the field of the lowest source rank wins, the last one within it.</summary>
    private void ResolveCollisionsPairwise()
    {
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

                var beaten = other.Source < field.Source || (other.Source == field.Source && j > i);
                if (beaten)
                {
                    _fields[i] = field with { NameLength = -1 };
                    break;
                }
            }
        }
    }

    /// <summary>
    /// The same rule in one pass for large records: a table keyed by name keeps the current
    /// winner, which a later field replaces when its source ranks the same or higher — the same
    /// winner the pairwise comparison picks, at linear instead of quadratic cost.
    /// </summary>
    private void ResolveCollisionsHashed()
    {
        var size = (int)BitOperations.RoundUpToPowerOf2((uint)_fieldCount * 2);
        if (_slots.Length < size)
        {
            _slots = new int[size];
        }
        else
        {
            Array.Clear(_slots, 0, size);
        }

        var mask = size - 1;
        for (var i = 0; i < _fieldCount; i++)
        {
            var field = _fields[i];
            if (field.NameLength < 0)
            {
                continue;
            }

            var hash = new HashCode();
            hash.AddBytes(_names.AsSpan(field.NameStart, field.NameLength));
            var slot = hash.ToHashCode() & mask;
            while (true)
            {
                var occupant = _slots[slot] - 1;
                if (occupant < 0)
                {
                    _slots[slot] = i + 1;
                    break;
                }

                var winner = _fields[occupant];
                if (!SameName(field, winner))
                {
                    slot = (slot + 1) & mask;
                    continue;
                }

                if (field.Source <= winner.Source)
                {
                    _fields[occupant] = winner with { NameLength = -1 };
                    _slots[slot] = i + 1;
                }
                else
                {
                    _fields[i] = field with { NameLength = -1 };
                }

                break;
            }
        }
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

    /// <summary>
    /// Closes the message at <see cref="HostLoomLoggerOptions.MaxMessageLength"/>: the cut lands
    /// on a UTF-8 character boundary and is marked with "…". A hole whose value lived in the cut
    /// region is moved to the value buffer first, so the field survives the text cap intact.
    /// </summary>
    private void CapMessage()
    {
        if (_messageLength <= _maxMessageLength)
        {
            return;
        }

        var cut = BoundaryBefore(_message, _maxMessageLength);
        for (var i = 0; i < _fieldCount; i++)
        {
            var field = _fields[i];
            if (field.ValueInMessage && field.ValueStart + field.ValueLength > cut)
            {
                var start = _valuesLength;
                EnsureValues(field.ValueLength);
                _message.AsSpan(field.ValueStart, field.ValueLength).CopyTo(_values.AsSpan(start));
                _valuesLength += field.ValueLength;
                _fields[i] = field with
                {
                    ValueInMessage = false,
                    ValueStart = start,
                    RenderingStart = 0,
                    RenderingLength = -1,
                };
            }
            else if (
                field.RenderingLength >= 0
                && field.RenderingStart + field.RenderingLength > cut
            )
            {
                _fields[i] = field with { RenderingStart = 0, RenderingLength = -1 };
            }
        }

        _messageLength = cut;
        EnsureMessage(Ellipsis.Length);
        Ellipsis.CopyTo(_message.AsSpan(_messageLength));
        _messageLength += Ellipsis.Length;
        _messageTruncated = true;
    }

    private static ReadOnlySpan<byte> Ellipsis => "…"u8;

    // Allocate against the remaining budget, never against an unbounded caller input.
    private static int EncodeText(
        ref byte[] buffer,
        int start,
        ReadOnlySpan<char> text,
        int max,
        out bool truncated
    )
    {
        var capacity = (int)Math.Min((long)text.Length * 3, max);
        EnsureTextBuffer(ref buffer, start + capacity + Ellipsis.Length);
        var status = Utf8.FromUtf16(
            text,
            buffer.AsSpan(start, capacity),
            out _,
            out var written,
            replaceInvalidSequences: true,
            isFinalBlock: true
        );
        truncated = status != OperationStatus.Done;
        if (truncated)
        {
            Ellipsis.CopyTo(buffer.AsSpan(start + written));
            written += Ellipsis.Length;
        }

        return written;
    }

    private static int CopyText(
        ref byte[] buffer,
        int start,
        ReadOnlySpan<byte> text,
        int max,
        out bool truncated
    )
    {
        truncated = text.Length > max;
        var length = truncated ? BoundaryBefore(text, max) : text.Length;
        EnsureTextBuffer(ref buffer, start + length + (truncated ? Ellipsis.Length : 0));
        text[..length].CopyTo(buffer.AsSpan(start));
        if (truncated)
        {
            Ellipsis.CopyTo(buffer.AsSpan(start + length));
            length += Ellipsis.Length;
        }

        return length;
    }

    private static void EnsureTextBuffer(ref byte[] buffer, int required)
    {
        if (required > buffer.Length)
        {
            Array.Resize(ref buffer, Math.Max(buffer.Length * 2, required));
        }
    }

    /// <summary>The largest offset at most <paramref name="max"/> that does not split a UTF-8
    /// sequence. The byte at <paramref name="max"/> must exist in <paramref name="text"/>.</summary>
    private static int BoundaryBefore(ReadOnlySpan<byte> text, int max)
    {
        var cut = max;
        while (cut > 0 && (text[cut] & 0xC0) == 0x80)
        {
            cut--;
        }

        return cut;
    }

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

        if (ScopeTexts is { Capacity: > 64 })
        {
            ScopeTexts = null;
        }

        if (TemplateRenderings is { Capacity: > 64 })
        {
            TemplateRenderings = null;
        }

        // A record with thousands of fields must not leave every later use of this pooled entry
        // holding a table that size.
        if (_fields.Length > MaxRetainedFields)
        {
            _fields = new LogField[8];
        }

        if (_slots.Length > MaxRetainedFields * 2)
        {
            _slots = [];
        }
    }

    private const int MaxRetainedFields = 64;
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
        return entry;
    }

    public static void Return(LogEntry entry)
    {
        // Free entries must not keep exception graphs or other event metadata alive.
        entry.Reset();
        if (Interlocked.Increment(ref _retained) > MaxRetained)
        {
            Interlocked.Decrement(ref _retained);
            return;
        }

        entry.TrimIfOversized();
        Free.Enqueue(entry);
    }
}
