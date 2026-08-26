using System.Buffers;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>Turns one record into bytes. Formatters write UTF-8 and never build a string.</summary>
public interface ILogFormatter
{
    void Format(in LogRecord record, IBufferWriter<byte> writer);
}

/// <summary>Consumes formatted bytes. Called only from the single writer thread.</summary>
public interface ILogSink : IAsyncDisposable
{
    void Write(ReadOnlySpan<byte> payload);

    ValueTask FlushAsync(CancellationToken cancellationToken);
}

/// <summary>What to do when the queue is full. Borrowed from Log4j2, which makes this explicit too.</summary>
public enum QueueFullPolicy
{
    /// <summary>Block the caller until there is room. Loses nothing; propagates backpressure.</summary>
    Block,

    /// <summary>Drop the incoming record. Protects latency; loses logs under sustained overload.</summary>
    DropNewest,

    /// <summary>Drop the incoming record unless it is at least <see cref="LogLevel.Warning"/>.</summary>
    DropBelowWarning,
}

public sealed class HostLoomLoggerOptions
{
    /// <summary>Bounded on purpose: an unbounded queue turns a logging burst into an OutOfMemoryException.</summary>
    public int QueueCapacity { get; set; } = 8192;

    public QueueFullPolicy QueueFullPolicy { get; set; } = QueueFullPolicy.DropBelowWarning;

    /// <summary>Records formatted per flush. Larger batches trade latency for syscalls.</summary>
    public int BatchSize { get; set; } = 256;

    public bool CaptureActivity { get; set; } = true;

    /// <summary>
    /// The clock each event's timestamp is read from, on the calling thread at capture time.
    /// Reading the wall clock per event follows operating-system clock corrections, so timestamps
    /// stay comparable across services; a backward step after a correction is intentional.
    /// </summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
}
