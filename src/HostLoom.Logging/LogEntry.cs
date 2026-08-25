using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>Offsets of one structured field. Values are slices of the rendered message.</summary>
internal readonly record struct LogField(
    int NameStart,
    int NameLength,
    int ValueStart,
    int ValueLength
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
    private LogField[] _fields = new LogField[8];
    private int _messageLength;
    private int _namesLength;
    private int _fieldCount;

    public LogLevel Level { get; set; }

    public string Category { get; set; } = string.Empty;

    public EventId EventId { get; set; }

    public Exception? Exception { get; set; }

    /// <summary>Raw ticks, converted to wall-clock on the writer thread. Cheaper than DateTime.UtcNow.</summary>
    public long Timestamp { get; set; }

    public int ThreadId { get; set; }

    public ActivityTraceId TraceId { get; set; }

    public ActivitySpanId SpanId { get; set; }

    public bool HasActivity { get; set; }

    public ReadOnlySpan<byte> Message => _message.AsSpan(0, _messageLength);

    public int FieldCount => _fieldCount;

    public void GetField(int index, out ReadOnlySpan<byte> name, out ReadOnlySpan<byte> value)
    {
        var field = _fields[index];
        name = _names.AsSpan(field.NameStart, field.NameLength);
        value = _message.AsSpan(field.ValueStart, field.ValueLength);
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
    /// </summary>
    public void AppendFormattable<T>(T value, string? format, string? name)
        where T : IUtf8SpanFormattable
    {
        var start = _messageLength;
        int written;
        while (
            !value.TryFormat(
                _message.AsSpan(_messageLength),
                out written,
                format,
                CultureInfo.InvariantCulture
            )
        )
        {
            EnsureMessage(Math.Max(64, _message.Length));
        }

        _messageLength += written;
        RecordField(name, start, _messageLength - start);
    }

    /// <summary>Booleans get their own path: <see cref="bool"/> has no UTF-8 formatter to constrain to.</summary>
    public void AppendBoolean(bool value, string? name)
    {
        var start = _messageLength;
        var text = value ? "true"u8 : "false"u8;
        EnsureMessage(text.Length);
        text.CopyTo(_message.AsSpan(_messageLength));
        _messageLength += text.Length;
        RecordField(name, start, _messageLength - start);
    }

    public void AppendText(ReadOnlySpan<char> value, string? name)
    {
        var start = _messageLength;
        EnsureMessage(Encoding.UTF8.GetMaxByteCount(value.Length));
        _messageLength += Encoding.UTF8.GetBytes(value, _message.AsSpan(_messageLength));
        RecordField(name, start, _messageLength - start);
    }

    public void Reset()
    {
        _messageLength = 0;
        _namesLength = 0;
        _fieldCount = 0;
        Exception = null;
        Category = string.Empty;
        EventId = default;
        HasActivity = false;
    }

    private void RecordField(string? name, int valueStart, int valueLength)
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
            valueLength
        );
    }

    private void EnsureMessage(int additional)
    {
        if (_messageLength + additional <= _message.Length)
        {
            return;
        }

        Array.Resize(ref _message, Math.Max(_message.Length * 2, _messageLength + additional));
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
