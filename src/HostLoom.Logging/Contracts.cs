using System.Buffers;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// Turns one record into bytes. Formatters write UTF-8 and never build a string. One instance
/// can be shared: registering it with <c>AddHostLoomLogging</c> hands it to the provider of every
/// container built from that service collection, and each provider formats on its own writer
/// thread, so <see cref="Format"/> may run concurrently and must be safe for that, as the
/// built-in formatters are. <see cref="OwnsFieldName"/> must be a pure function of its input. An exception from
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

/// <summary>
/// Consumes formatted bytes. Called only from the single writer thread. An exception from
/// <see cref="Write"/> costs only the batch being written: the pipeline counts its records as
/// dropped with reason <c>sink_failed</c> and goes on with the next batch on the same instance,
/// so a sink must remain usable after it throws. An exception from <see cref="FlushAsync"/> is
/// counted and costs nothing further.
/// </summary>
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
    /// Block the caller until there is room, propagating backpressure. The wait is synchronous on
    /// the calling thread and lasts at most <see cref="HostLoomLoggerOptions.EnqueueTimeout"/>,
    /// one second by default, after which the record is dropped and counted. Set the timeout to
    /// null to lose nothing, at the price of stalling the application with a stalled sink.
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
    /// stalled external code may remain on an abandoned background thread. A process that ends
    /// without disposing the provider, through <c>Environment.Exit</c>, <c>Main</c> returning,
    /// or an unhandled exception, spends up to this long getting the queued records out.
    /// </summary>
    public TimeSpan ShutdownTimeout { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>
    /// Upper bound on one blocking enqueue (the <see cref="QueueFullPolicy.Block"/> policy, and
    /// Warning-and-above under <see cref="QueueFullPolicy.DropBelowWarning"/>). When the bound is
    /// reached the record is dropped and counted, trading completeness for liveness. One second
    /// by default, so a sink that stops draining, such as stdout nobody reads, costs a blocked
    /// call at most that long instead of stalling it for as long as the sink stalls. Null blocks
    /// without limit.
    /// </summary>
    public TimeSpan? EnqueueTimeout { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// Longest accepted field name, in UTF-8 bytes before escaping. A longer name drops the
    /// field (never the record), counted in <c>hostloom.logging.fields.dropped</c>. Names are
    /// rejected on the producer before allocating their encoded buffers.
    /// </summary>
    public int MaxFieldNameLength { get; set; } = DefaultMaxFieldNameLength;

    internal const int DefaultMaxFieldNameLength = 128;

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

    /// <summary>
    /// The output format by name, so configuration can choose it: <see cref="LogFormatterNames.Json"/>
    /// for <see cref="JsonLogFormatter"/> or <see cref="LogFormatterNames.Clef"/> for
    /// <see cref="ClefLogFormatter"/>, matched without regard to case. Null keeps the default,
    /// JSON for the hosted provider and CLEF for the bootstrap logger. A formatter passed in code
    /// takes precedence over the name. Any other name, the empty one included, fails validation,
    /// even when a formatter passed in code makes the name unused.
    /// </summary>
    public string? Formatter { get; set; }

    /// <summary>
    /// Longest exception text, in characters, that a formatter created from
    /// <see cref="Formatter"/> or by default writes; a longer message or chain is cut and closed
    /// with a trailing "…". A formatter passed in code keeps the cap it was constructed with.
    /// </summary>
    public int MaxExceptionLength { get; set; } = DefaultMaxExceptionLength;

    internal const int DefaultMaxExceptionLength = 32 * 1024;

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

    /// <summary>
    /// Applies configuration over the current values, as the configuration overloads of
    /// <c>AddHostLoomLogging</c> do. Values set before the call are defaults that configuration
    /// overrides, and values set after it win, so a wrapper can layer its own defaults under
    /// configuration by calling this from the callback of an overload without configuration.
    /// Keys follow the property names. An unknown key or invalid value throws, as does any key
    /// under <see cref="Enrichers"/> or <see cref="TimeProvider"/>, which only code can set; an
    /// empty <see cref="EnqueueTimeout"/> lifts the limit.
    /// </summary>
    /// <returns>This instance.</returns>
    public HostLoomLoggerOptions Bind(IConfiguration configuration)
    {
        ArgumentNullException.ThrowIfNull(configuration);

        // The binder accepts both names as known properties but can bind neither, so without this
        // check a configured enricher list would be ignored without a word.
        foreach (var codeOnly in (ReadOnlySpan<string>)[nameof(Enrichers), nameof(TimeProvider)])
        {
            if (configuration.GetSection(codeOnly).Exists())
            {
                throw new InvalidOperationException(
                    $"{codeOnly} cannot be set from configuration, found at "
                        + $"'{configuration.GetSection(codeOnly).Path}'. Set "
                        + $"{nameof(HostLoomLoggerOptions)}.{codeOnly} in code instead."
                );
            }
        }

        // Strict on purpose: a typo in a cap or policy name should fail startup loudly rather
        // than silently leave the default in place. The binding generator compiles this call.
        configuration.Bind(this, binder => binder.ErrorOnUnknownConfiguration = true);

        // An empty value lifts the limit, as the reflection binder made it do; the generated
        // binder leaves a nullable value alone when its configuration value is empty.
        if (configuration[nameof(EnqueueTimeout)] is "")
        {
            EnqueueTimeout = null;
        }

        return this;
    }
}
