using System.Buffers;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// Compact Log Event Format (CLEF), shaped to match what Serilog's <c>CompactJsonFormatter</c>
/// produces: <c>@t</c>, <c>@mt</c> when a template exists (<c>@m</c> only when none does — never
/// both), <c>@r</c> renderings for formatted tokens, <c>@l</c> omitted for Information with
/// Serilog level names otherwise, <c>@x</c> as the complete exception chain, <c>@tr</c>/<c>@sp</c>
/// from the captured activity, plus <c>SourceContext</c>, <c>ThreadId</c>, <c>EventId</c> in the
/// Serilog provider's ordinary-property shape, and every captured field as a top-level typed
/// property. <c>@i</c> is deliberately not emitted: rendered CLEF reserves it for the
/// template-hash event type, which this formatter does not compute.
/// </summary>
public sealed class ClefLogFormatter : ILogFormatter
{
    private static readonly byte[] NewLine = "\n"u8.ToArray();

    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Indented = false,
        SkipValidation = true,
    };

    private readonly Utf8JsonWriter _writer = new(Stream.Null, WriterOptions);
    private readonly int _maxExceptionLength;

    /// <param name="maxExceptionLength">Cap on the encoded <c>@x</c> text; longer chains are
    /// truncated with an explicit marker.</param>
    public ClefLogFormatter(int maxExceptionLength = 32 * 1024)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxExceptionLength, 1);
        _maxExceptionLength = maxExceptionLength;
    }

    public void Format(in LogRecord record, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        _writer.Reset(writer);
        _writer.WriteStartObject();
        // UtcDateTime keeps DateTimeKind.Utc, so the timestamp serializes with the CLEF 'Z'.
        _writer.WriteString("@t"u8, record.Timestamp.UtcDateTime);
        if (record.Template is { } template)
        {
            _writer.WriteString("@mt"u8, template);
        }
        else
        {
            _writer.WriteString("@m"u8, record.Message);
        }

        WriteRenderings(record);

        if (record.Level != LogLevel.Information)
        {
            _writer.WriteString("@l"u8, LevelName(record.Level));
        }

        if (record.Exception is { } exception)
        {
            _writer.WriteString("@x"u8, ExceptionText.Render(exception, _maxExceptionLength));
        }

        if (record.HasActivity)
        {
            _writer.WriteString("@tr"u8, record.TraceId.ToHexString());
            _writer.WriteString("@sp"u8, record.SpanId.ToHexString());
        }

        _writer.WriteString("SourceContext"u8, record.Category);
        _writer.WriteNumber("ThreadId"u8, record.ThreadId);
        WriteEventId(record.EventId);

        for (var i = 0; i < record.FieldCount; i++)
        {
            record.GetField(i, out var name, out var value, out var kind);
            switch (kind)
            {
                case LogFieldKind.Number:
                case LogFieldKind.Json:
                    _writer.WritePropertyName(name);
                    _writer.WriteRawValue(value, skipInputValidation: true);
                    break;
                case LogFieldKind.Boolean:
                    _writer.WriteBoolean(name, value[0] == (byte)'t');
                    break;
                case LogFieldKind.Null:
                    _writer.WriteNull(name);
                    break;
                default:
                    _writer.WriteString(name, value);
                    break;
            }
        }

        _writer.WriteEndObject();
        _writer.Flush();
        writer.Write(NewLine);
    }

    /// <summary>
    /// The reified CLEF names plus the core Serilog-provider properties. A single leading
    /// <c>@</c> marks formatter territory; user names arrive here already escaped to <c>@@</c>,
    /// which is exempt — CLEF readers unescape it back to the caller's original name.
    /// </summary>
    public bool OwnsFieldName(ReadOnlySpan<byte> name) =>
        name.SequenceEqual("SourceContext"u8)
        || name.SequenceEqual("ThreadId"u8)
        || name.SequenceEqual("EventId"u8)
        || (name.Length > 1 && name[0] == (byte)'@' && name[1] != (byte)'@');

    private void WriteRenderings(in LogRecord record)
    {
        var any = false;
        for (var i = 0; i < record.FieldCount; i++)
        {
            if (!record.TryGetRendering(i, out var rendering))
            {
                continue;
            }

            if (!any)
            {
                _writer.WritePropertyName("@r"u8);
                _writer.WriteStartArray();
                any = true;
            }

            _writer.WriteStringValue(rendering);
        }

        if (any)
        {
            _writer.WriteEndArray();
        }
    }

    private void WriteEventId(EventId eventId)
    {
        if (eventId.Id == 0 && eventId.Name is null)
        {
            return;
        }

        // The Serilog MEL provider's shape: a structure carrying Id and/or Name.
        _writer.WritePropertyName("EventId"u8);
        _writer.WriteStartObject();
        if (eventId.Id != 0)
        {
            _writer.WriteNumber("Id"u8, eventId.Id);
        }

        if (eventId.Name is { } name)
        {
            _writer.WriteString("Name"u8, name);
        }

        _writer.WriteEndObject();
    }

    private static string LevelName(LogLevel level) =>
        level switch
        {
            LogLevel.Trace => "Verbose",
            LogLevel.Debug => "Debug",
            LogLevel.Warning => "Warning",
            LogLevel.Error => "Error",
            LogLevel.Critical => "Fatal",
            _ => "Information",
        };
}
