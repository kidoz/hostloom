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
    /// <summary>
    /// Writes one formatted batch. The token cancels when disposal has reached its deadline and
    /// given up waiting; a sink that can abort mid-write should observe it, because one that
    /// cannot is abandoned together with the writer thread stuck inside it.
    /// </summary>
    void Write(ReadOnlySpan<byte> payload, CancellationToken cancellationToken);

    ValueTask FlushAsync(CancellationToken cancellationToken);
}

/// <summary>What to do when the queue is full. Borrowed from Log4j2, which makes this explicit too.</summary>
public enum QueueFullPolicy
{
    /// <summary>
    /// Block the caller until there is room. Loses nothing; propagates backpressure. The wait is
    /// synchronous on the calling thread — bound it with
    /// <see cref="HostLoomLoggerOptions.EnqueueTimeout"/> if a stalled sink must not be able to
    /// stall the application with it.
    /// </summary>
    Block,

    /// <summary>Drop the incoming record. Protects latency; loses logs under sustained overload.</summary>
    DropNewest,

    /// <summary>
    /// Drop the incoming record unless it is at least <see cref="LogLevel.Warning"/>. Warning and
    /// above block exactly like <see cref="Block"/>, with the same
    /// <see cref="HostLoomLoggerOptions.EnqueueTimeout"/> bound.
    /// </summary>
    DropBelowWarning,
}

public sealed class HostLoomLoggerOptions
{
    /// <summary>Bounded on purpose: an unbounded queue turns a logging burst into an OutOfMemoryException.</summary>
    public int QueueCapacity { get; set; } = 8192;

    public QueueFullPolicy QueueFullPolicy { get; set; } = QueueFullPolicy.DropBelowWarning;

    /// <summary>Records formatted per flush. Larger batches trade latency for syscalls.</summary>
    public int BatchSize { get; set; } = 256;

    /// <summary>
    /// How long disposal may spend draining and flushing before abandoning the writer. Records
    /// still queued at the deadline are counted as dropped rather than waited for: logging must
    /// never be the reason a service cannot shut down.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Upper bound on one blocking enqueue (the <see cref="QueueFullPolicy.Block"/> policy, and
    /// Warning-and-above under <see cref="QueueFullPolicy.DropBelowWarning"/>). Null blocks
    /// without limit. When the bound is reached the record is dropped and counted, trading
    /// completeness for liveness.
    /// </summary>
    public TimeSpan? EnqueueTimeout { get; set; }

    public bool CaptureActivity { get; set; } = true;

    /// <summary>
    /// The clock each event's timestamp is read from, on the calling thread at capture time.
    /// Reading the wall clock per event follows operating-system clock corrections, so timestamps
    /// stay comparable across services; a backward step after a correction is intentional.
    /// </summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
}
