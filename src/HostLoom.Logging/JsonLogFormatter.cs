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
            record.GetField(i, out var name, out var value, out var kind);
            switch (kind)
            {
                case LogFieldKind.Number:
                case LogFieldKind.Json:
                    // Tokens the library itself produced; re-validating them would be pure cost.
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

        if (record.Exception is { } exception)
        {
            _writer.WriteString(
                "error.type"u8,
                exception.GetType().FullName ?? exception.GetType().Name
            );
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
