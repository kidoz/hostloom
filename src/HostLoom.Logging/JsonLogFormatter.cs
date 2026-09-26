using System.Buffers;
using System.Text.Json;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// Compact JSON, one object per line. Field names follow the Elastic Common Schema, which is the
/// same default Spring Boot 3.4 chose for built-in structured logging.
/// </summary>
public sealed class JsonLogFormatter : ILogFormatter
{
    private static readonly byte[] NewLine = "\n"u8.ToArray();

    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Indented = false,
        SkipValidation = true,
    };

    private readonly Utf8JsonWriter _writer = new(Stream.Null, WriterOptions);
    private readonly Lock _gate = new();
    private readonly int _maxExceptionLength;

    /// <param name="maxExceptionLength">Cap on the encoded exception text; longer chains are
    /// truncated with an explicit marker.</param>
    public JsonLogFormatter(int maxExceptionLength = 32 * 1024)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxExceptionLength, 1);
        _maxExceptionLength = maxExceptionLength;
    }

    /// <summary>
    /// Safe to call from several pipelines at once: one instance may be registered with every
    /// container built from a service collection, while each keeps its own writer thread.
    /// </summary>
    public void Format(in LogRecord record, IBufferWriter<byte> writer)
    {
        // The JSON writer is reused per record, so concurrent formats would interleave in it.
        lock (_gate)
        {
            FormatCore(record, writer);
        }
    }

    private void FormatCore(in LogRecord record, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        // Reset rather than allocate: the gate in Format gives this writer one record at a time.
        _writer.Reset(writer);
        _writer.WriteStartObject();
        _writer.WriteString("@timestamp"u8, record.Timestamp);
        _writer.WriteString("log.level"u8, ToLevel(record.Level));
        _writer.WriteString("log.logger"u8, record.Category);
        _writer.WriteString("message"u8, record.Message);
        _writer.WriteNumber("process.thread.id"u8, record.ThreadId);

        if (record.EventId.Id != 0)
        {
            _writer.WriteNumber("event.code"u8, record.EventId.Id);
        }

        if (record.HasActivity)
        {
            _writer.WriteString("trace.id"u8, record.TraceId.ToHexString());
            _writer.WriteString("span.id"u8, record.SpanId.ToHexString());
        }

        _writer.WriteFields(record);

        if (record.Exception is { } exception)
        {
            _writer.WriteString(
                "error.type"u8,
                exception.GetType().FullName ?? exception.GetType().Name
            );
            _writer.WriteString(
                "error.message"u8,
                ExceptionText.Message(exception, _maxExceptionLength)
            );
            // The full ToString chain — inner exceptions and aggregate children included — not
            // just the top frame's stack; chain analysis is routine incident work.
            _writer.WriteString(
                "error.stack_trace"u8,
                ExceptionText.Render(exception, _maxExceptionLength)
            );
        }

        _writer.WriteEndObject();
        _writer.Flush();
        writer.Write(NewLine);
    }

    /// <summary>
    /// The ECS names this formatter reifies itself. Reserved unconditionally — even when a
    /// particular record would omit one (no exception, no activity) — because a name that means
    /// "error.type" only on quiet records is worse for a parser than no name at all.
    /// </summary>
    public bool OwnsFieldName(ReadOnlySpan<byte> name) =>
        name.SequenceEqual("@timestamp"u8)
        || name.SequenceEqual("log.level"u8)
        || name.SequenceEqual("log.logger"u8)
        || name.SequenceEqual("message"u8)
        || name.SequenceEqual("process.thread.id"u8)
        || name.SequenceEqual("event.code"u8)
        || name.SequenceEqual("trace.id"u8)
        || name.SequenceEqual("span.id"u8)
        || name.SequenceEqual("error.type"u8)
        || name.SequenceEqual("error.message"u8)
        || name.SequenceEqual("error.stack_trace"u8);

    private static ReadOnlySpan<byte> ToLevel(LogLevel level) =>
        level switch
        {
            LogLevel.Trace => "TRACE"u8,
            LogLevel.Debug => "DEBUG"u8,
            LogLevel.Information => "INFO"u8,
            LogLevel.Warning => "WARN"u8,
            LogLevel.Error => "ERROR"u8,
            LogLevel.Critical => "FATAL"u8,
            _ => "NONE"u8,
        };
}
