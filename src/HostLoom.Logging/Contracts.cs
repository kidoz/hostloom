using System.Buffers;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// Turns one record into bytes. Formatters write UTF-8 and never build a string. An exception from
/// <see cref="Format"/> or <see cref="OwnsFieldName"/> costs only the record being formatted: the
/// pipeline cuts that record's partial output from its batch, counts it as dropped with reason
/// <c>format_failed</c>, and keeps formatting later records with the same instance, so a formatter
/// must remain usable after it throws.
/// </summary>
public interface ILogFormatter
{
    void Format(in LogRecord record, IBufferWriter<byte> writer);

    /// <summary>
    /// Whether the formatter itself emits a property with this name (its reified schema fields).
    /// A captured field carrying a reserved name is dropped and counted before formatting, so a
    /// record can never emit the same key twice. Names arrive here after <c>@@</c> escaping, so
    /// an escaped user name never matches. The default reserves nothing.
    /// </summary>
    bool OwnsFieldName(ReadOnlySpan<byte> name) => false;
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
    internal const int DefaultMaxMessageLength = 16 * 1024;

    internal const int DefaultMaxTextFieldLength = 8 * 1024;

    internal const int DefaultMaxFieldsPerRecord = 64;

    /// <summary>Bounded on purpose: an unbounded queue turns a logging burst into an OutOfMemoryException.</summary>
    public int QueueCapacity { get; set; } = 8192;

    public QueueFullPolicy QueueFullPolicy { get; set; } = QueueFullPolicy.DropBelowWarning;

    /// <summary>Records formatted per flush. Larger batches trade latency for syscalls.</summary>
    public int BatchSize { get; set; } = 256;

    /// <summary>
    /// How long disposal may spend draining and flushing before abandoning the writer. Records
    /// still queued at the deadline are counted as dropped rather than waited for: logging must
    /// never be the reason a service cannot shut down. Sink disposal has a separate budget of
    /// the same length, including its synchronous prefix. Cancellation has a 250 ms grace;
    /// stalled external code may remain on an abandoned background thread.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Upper bound on one blocking enqueue (the <see cref="QueueFullPolicy.Block"/> policy, and
    /// Warning-and-above under <see cref="QueueFullPolicy.DropBelowWarning"/>). Null blocks
    /// without limit. When the bound is reached the record is dropped and counted, trading
    /// completeness for liveness.
    /// </summary>
    public TimeSpan? EnqueueTimeout { get; set; }

    /// <summary>
    /// Longest accepted field name, in UTF-8 bytes before escaping. A longer name drops the
    /// field (never the record), counted in <c>hostloom.logging.fields.dropped</c>.
    /// </summary>
    public int MaxFieldNameLength { get; set; } = 128;

    /// <summary>
    /// Most fields one record may carry after deduplication. Overflow fields are dropped and
    /// counted; the record itself still ships.
    /// </summary>
    public int MaxFieldsPerRecord { get; set; } = DefaultMaxFieldsPerRecord;

    /// <summary>
    /// Longest rendered message one record may carry, in UTF-8 bytes. A longer message is cut on
    /// a character boundary and closed with a trailing "…"; the fields of holes past the cut
    /// still ship subject to their field caps. Oversized original templates are discarded after
    /// safe rendering; template-aware formatters then receive only the capped message.
    /// </summary>
    public int MaxMessageLength { get; set; } = DefaultMaxMessageLength;

    /// <summary>
    /// Longest text one plain field, string hole, enricher value, or scope text may carry, in
    /// UTF-8 bytes; longer text is cut on a character boundary and closed with a trailing "…".
    /// Strings inside destructured objects are bounded separately by
    /// <see cref="DestructuringOptions.MaxStringLength"/>.
    /// </summary>
    public int MaxTextFieldLength { get; set; } = DefaultMaxTextFieldLength;

    /// <summary>Caps and protection policy for <c>{@...}</c> object destructuring.</summary>
    public DestructuringOptions Destructuring { get; } = new();

    /// <summary>
    /// Producer-side enrichers, run in registration order on every event before it is queued —
    /// the point where <c>AsyncLocal</c> ambient context is still visible. Later enrichers win
    /// name collisions within the enricher rank; event holes outrank them all.
    /// </summary>
    public IList<ILogEnricher> Enrichers { get; } = [];

    /// <summary>
    /// Attach <see cref="Environment.MachineName"/> once at provider start as a static field
    /// named <c>MachineName</c> — Serilog's <c>WithMachineName</c> parity. Static fields have
    /// the lowest collision precedence.
    /// </summary>
    public bool AttachMachineName { get; set; } = true;

    /// <summary>Logical service name, attached as a static <c>ServiceName</c> field when set.</summary>
    public string? ServiceName { get; set; }

    public bool CaptureActivity { get; set; } = true;

    /// <summary>
    /// The clock each event's timestamp is read from, on the calling thread at capture time.
    /// Reading the wall clock per event follows operating-system clock corrections, so timestamps
    /// stay comparable across services; a backward step after a correction is intentional.
    /// </summary>
    public TimeProvider TimeProvider { get; set; } = TimeProvider.System;
}
