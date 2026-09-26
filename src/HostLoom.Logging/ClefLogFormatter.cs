using System.Buffers;
using System.Globalization;
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
        // Round-trip format, as Serilog writes it: always seven fractional digits and the 'Z' that
        // UtcDateTime's DateTimeKind.Utc produces. The writer's own DateTime encoding trims
        // trailing zeros, which a fixed-pattern timestamp parser would reject.
        Span<byte> timestamp = stackalloc byte[33];
        record.Timestamp.UtcDateTime.TryFormat(
            timestamp,
            out var length,
            "O",
            CultureInfo.InvariantCulture
        );
        _writer.WriteString("@t"u8, timestamp[..length]);
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

        // A caller's hole with one of these names keeps its value, as it does under Serilog; the
        // core property is written only when no captured field carries the name.
        if (!HasField(record, "SourceContext"u8))
        {
            _writer.WriteString("SourceContext"u8, record.Category);
        }

        if (!HasField(record, "ThreadId"u8))
        {
            _writer.WriteNumber("ThreadId"u8, record.ThreadId);
        }

        if (!HasField(record, "EventId"u8))
        {
            WriteEventId(record.EventId);
        }

        _writer.WriteFields(record);

        _writer.WriteEndObject();
        _writer.Flush();
        writer.Write(NewLine);
    }

    /// <summary>
    /// The reified CLEF names. A single leading <c>@</c> marks formatter territory; user names
    /// arrive here already escaped to <c>@@</c>, which is exempt — CLEF readers unescape it back
    /// to the caller's original name. The core <c>SourceContext</c>, <c>ThreadId</c>, and
    /// <c>EventId</c> properties are not reserved: a captured field of that name replaces them.
    /// </summary>
    public bool OwnsFieldName(ReadOnlySpan<byte> name) =>
        name.Length > 1 && name[0] == (byte)'@' && name[1] != (byte)'@';

    private static bool HasField(in LogRecord record, ReadOnlySpan<byte> name)
    {
        for (var i = 0; i < record.FieldCount; i++)
        {
            record.GetField(i, out var fieldName, out _, out _);
            if (fieldName.SequenceEqual(name))
            {
                return true;
            }
        }

        return false;
    }

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
