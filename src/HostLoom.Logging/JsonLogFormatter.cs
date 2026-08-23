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
        SkipValidation = true
    };

    private readonly Utf8JsonWriter _writer = new(Stream.Null, WriterOptions);

    public void Format(in LogRecord record, IBufferWriter<byte> writer)
    {
        ArgumentNullException.ThrowIfNull(writer);

        // Reset rather than allocate: one writer per pipeline, and the pipeline has one thread.
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
            Span<char> id = stackalloc char[32];
            record.TraceId.ToHexString().CopyTo(id);
            _writer.WriteString("trace.id"u8, id);
            _writer.WriteString("span.id"u8, record.SpanId.ToHexString());
        }

        for (var i = 0; i < record.FieldCount; i++)
        {
            record.GetField(i, out var name, out var value);
            _writer.WriteString(name, value);
        }

        if (record.Exception is { } exception)
        {
            _writer.WriteString("error.type"u8, exception.GetType().FullName ?? exception.GetType().Name);
            _writer.WriteString("error.message"u8, exception.Message);
            if (exception.StackTrace is { } stack)
            {
                _writer.WriteString("error.stack_trace"u8, stack);
            }
        }

        _writer.WriteEndObject();
        _writer.Flush();
        writer.Write(NewLine);
    }

    private static ReadOnlySpan<byte> ToLevel(LogLevel level) => level switch
    {
        LogLevel.Trace => "TRACE"u8,
        LogLevel.Debug => "DEBUG"u8,
        LogLevel.Information => "INFO"u8,
        LogLevel.Warning => "WARN"u8,
        LogLevel.Error => "ERROR"u8,
        LogLevel.Critical => "FATAL"u8,
        _ => "NONE"u8
    };
}
