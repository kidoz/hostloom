using System.Buffers;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>One static enrichment field, its value encoded once for the provider's lifetime.</summary>
internal readonly record struct StaticField(string Name, byte[] Value);

/// <summary>
/// Single-reader queue plus a background writer on a dedicated thread. The calling thread renders
/// and enqueues; all formatting and I/O happens off it, which is what keeps a log call off the
/// tail latency path. A record the formatter fails on is cut from its batch and dropped alone, so
/// one unformattable record never costs the records around it. An unexpected sink failure faults
/// the pipeline instead of silently killing the writer: the channel closes, queued and in-flight
/// records are counted as dropped, and no caller is ever left waiting on a writer that has
/// stopped reading.
/// </summary>
internal sealed class LogPipeline : IAsyncDisposable
{
    private const int StateRunning = 0;
    private const int StateFaulted = 1;
    private const int StateDisposed = 2;

    /// <summary>Extra wait after cancelling the sink, so a cooperative abort can finish.</summary>
    private static readonly TimeSpan AbandonGrace = TimeSpan.FromMilliseconds(250);

    private readonly Channel<LogEntry> _queue;
    private readonly ILogFormatter _formatter;
    private readonly ILogSink _sink;
    private readonly HostLoomLoggerOptions _options;
    // CA2213: both are disposed by Shutdown(), on the shutdown thread DisposeAsync starts; the
    // cancellation source deliberately is not when an abandoned writer may still observe it.
#pragma warning disable CA2213
    private readonly LoggingMetrics _metrics;
    private readonly CancellationTokenSource _shutdown = new();
#pragma warning restore CA2213
    private readonly TaskCompletionSource _completion = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    /// <summary>Entries of the batch being formatted or written, held until the sink accepted
    /// them so a fault or abandonment can still count them.</summary>
    private readonly List<LogEntry> _batch;

    private long _dropped;
    private int _state;

    // Monitor for producers blocked on a full queue; see BlockingWrite. A Monitor, not a Lock,
    // because it is waited on and pulsed.
    private readonly object _space = new();
    private int _spaceWaiters;
    private int _disposeStarted;
    private volatile bool _abandoned;
    private volatile int _inFlight;
    private volatile Exception? _writerFault;

    /// <summary>Which component the writer thread is currently calling. Writer-thread only.</summary>
    private string _component = LoggingMetrics.ComponentFormatter;

    public LogPipeline(ILogFormatter formatter, ILogSink sink, HostLoomLoggerOptions options)
    {
        Validate(options);

        _formatter = formatter;
        _sink = sink;
        _options = options;
        _batch = new List<LogEntry>(options.BatchSize);
        _queue = Channel.CreateBounded<LogEntry>(
            new BoundedChannelOptions(options.QueueCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
                // The writer and blocked producers wait synchronously, so completing their waits
                // inline only releases a waiting thread. Deferred to the thread pool instead, the
                // wake-up stalled for seconds whenever the pool was saturated.
                AllowSynchronousContinuations = true,
            }
        );
        _metrics = new LoggingMetrics(
            () => _queue.Reader.Count,
            () => _state == StateRunning,
            StateName
        );
        Destructurer = new Destructurer(options.Destructuring, _metrics);
        Capture = new EventCapture(options, Destructurer, _metrics);
        StaticFields = BuildStaticFields(options);

        // A real dedicated thread, not a long-running task: an async method leaves its
        // LongRunning thread at the first incomplete await, and this writer must be able to sit
        // in a synchronous sink write without occupying a thread-pool worker. Background, so an
        // abandoned writer can never keep the process alive.
        var writer = new Thread(Run) { IsBackground = true, Name = "HostLoom Logging Writer" };
        writer.Start();
    }

    /// <summary>Records dropped for any reason. Surfaced so overload is visible, not silent.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>The sink failure that faulted the background writer. Null while it is healthy.</summary>
    public Exception? WriterFault => _writerFault;

    /// <summary>Serializes '@' hole values on the producer thread; shared by every logger.</summary>
    public Destructurer Destructurer { get; }

    /// <summary>The shared producer-side capture engine for state, scopes, and rendering.</summary>
    public EventCapture Capture { get; }

    /// <summary>Fields attached to every event, values UTF-8-encoded once at provider start.</summary>
    public StaticField[] StaticFields { get; }

    /// <summary>Producer-side counters for enricher and destructurer failures.</summary>
    public LoggingMetrics Metrics => _metrics;

    internal static StaticField[] BuildStaticFields(HostLoomLoggerOptions options)
    {
        var fields = new List<StaticField>(2);
        if (options.AttachMachineName)
        {
            fields.Add(
                new StaticField(
                    "MachineName",
                    System.Text.Encoding.UTF8.GetBytes(Environment.MachineName)
                )
            );
        }

        if (options.ServiceName is { Length: > 0 } service)
        {
            fields.Add(new StaticField("ServiceName", System.Text.Encoding.UTF8.GetBytes(service)));
        }

        return [.. fields];
    }

    internal static void Validate(HostLoomLoggerOptions options)
    {
        if (options.QueueCapacity < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.QueueCapacity,
                "QueueCapacity must be at least 1."
            );
        }

        if (options.BatchSize < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.BatchSize,
                "BatchSize must be at least 1."
            );
        }

        if (options.ShutdownTimeout <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.ShutdownTimeout,
                "ShutdownTimeout must be positive."
            );
        }

        if (options.EnqueueTimeout is { } wait && wait <= TimeSpan.Zero)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                wait,
                "EnqueueTimeout must be positive when set."
            );
        }

        if (!Enum.IsDefined(options.QueueFullPolicy))
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.QueueFullPolicy,
                "QueueFullPolicy must be a defined policy."
            );
        }

        if (options.TimeProvider is null)
        {
            throw new ArgumentException("TimeProvider must not be null.", nameof(options));
        }

        if (options.MaxFieldNameLength < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.MaxFieldNameLength,
                "MaxFieldNameLength must be at least 1."
            );
        }

        if (options.MaxFieldsPerRecord < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.MaxFieldsPerRecord,
                "MaxFieldsPerRecord must be at least 1."
            );
        }

        if (options.MaxMessageLength < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.MaxMessageLength,
                "MaxMessageLength must be at least 1."
            );
        }

        if (options.MaxTextFieldLength < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.MaxTextFieldLength,
                "MaxTextFieldLength must be at least 1."
            );
        }

        var destructuring = options.Destructuring;
        if (
            destructuring.MaxDepth < 1
            || destructuring.MaxCollectionItems < 1
            || destructuring.MaxObjectMembers < 1
            || destructuring.MaxStringLength < 1
            || destructuring.MaxEncodedBytesPerRecord < 1
        )
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                "Destructuring caps must all be at least 1."
            );
        }
    }

    public void Enqueue(LogEntry entry)
    {
        if (_state != StateRunning)
        {
            Discard(
                entry,
                _state == StateFaulted
                    ? LoggingMetrics.ReasonWriterFault
                    : LoggingMetrics.ReasonProviderDisposed
            );
            return;
        }

        if (_queue.Writer.TryWrite(entry))
        {
            return;
        }

        switch (_options.QueueFullPolicy)
        {
            case QueueFullPolicy.Block:
                BlockingWrite(entry);
                return;

            case QueueFullPolicy.DropBelowWarning when entry.Level >= LogLevel.Warning:
                BlockingWrite(entry);
                return;

            default:
                Discard(entry, LoggingMetrics.ReasonQueueFull);
                return;
        }
    }

    /// <summary>
    /// Deliberately synchronous: the caller chose backpressure over loss. The wait is bounded by
    /// <see cref="HostLoomLoggerOptions.EnqueueTimeout"/> when one is set, and always ends when
    /// the pipeline is disposed or faults, so a closed pipeline never strands a caller. Producers
    /// wait on <see cref="_space"/>, not on the channel: the channel completes a blocked write
    /// through the thread pool whatever its options say, so under pool starvation a producer
    /// could not wake even after the writer made room, and its timeout could not fire either.
    /// </summary>
    private void BlockingWrite(LogEntry entry)
    {
        var level = entry.Level;
        _metrics.RecordBlocked(level);
        var started = Stopwatch.GetTimestamp();
        string? dropReason = null;
        lock (_space)
        {
            Interlocked.Increment(ref _spaceWaiters);
            try
            {
                while (!_queue.Writer.TryWrite(entry))
                {
                    var state = Volatile.Read(ref _state);
                    if (state != StateRunning)
                    {
                        dropReason =
                            state == StateFaulted
                                ? LoggingMetrics.ReasonWriterFault
                                : LoggingMetrics.ReasonProviderDisposed;
                        break;
                    }

                    var wait = Timeout.Infinite;
                    if (_options.EnqueueTimeout is { } limit)
                    {
                        var remaining = limit - Stopwatch.GetElapsedTime(started);
                        if (remaining <= TimeSpan.Zero)
                        {
                            dropReason = LoggingMetrics.ReasonEnqueueTimeout;
                            break;
                        }

                        wait = Milliseconds(remaining);
                    }

                    Monitor.Wait(_space, wait);
                }
            }
            finally
            {
                Interlocked.Decrement(ref _spaceWaiters);
            }
        }

        if (dropReason is not null)
        {
            Discard(entry, dropReason);
        }

        _metrics.RecordBlockedFor(Stopwatch.GetElapsedTime(started).TotalSeconds, level);
    }

    /// <summary>Wakes producers blocked on a full queue, after the writer made room or the
    /// pipeline closed. Free when nobody waits.</summary>
    private void SignalSpace()
    {
        if (Volatile.Read(ref _spaceWaiters) > 0)
        {
            lock (_space)
            {
                Monitor.PulseAll(_space);
            }
        }
    }

    private static int Milliseconds(TimeSpan span) =>
        span.TotalMilliseconds >= int.MaxValue
            ? Timeout.Infinite
            : (int)Math.Ceiling(span.TotalMilliseconds);

    private void Run()
    {
        try
        {
            RunLoop();
        }
        finally
        {
            _completion.TrySetResult();
        }
    }

    private void RunLoop()
    {
        var buffer = new ArrayBufferWriter<byte>(64 * 1024);
        var reader = _queue.Reader;

        try
        {
            while (WaitToRead(reader))
            {
                FormatBatch(reader, buffer);
                WriteBuffer(buffer);
            }

            // False from WaitToRead means completed and empty: every accepted record is out.
            _component = LoggingMetrics.ComponentSink;
            var flush = _sink.FlushAsync(_shutdown.Token);
            if (!flush.IsCompletedSuccessfully)
            {
                flush.AsTask().GetAwaiter().GetResult();
            }
        }
        catch (OperationCanceledException) when (_shutdown.IsCancellationRequested)
        {
            // The shutdown deadline expired and disposal cancelled the sink mid-batch. Any other
            // OperationCanceledException is a sink failure and faults the pipeline below —
            // treating it as shutdown would leave producers facing an open channel nobody reads.
            DiscardBatch(LoggingMetrics.ReasonShutdownTimeout);
            DiscardQueued(LoggingMetrics.ReasonShutdownTimeout);
        }
        catch (Exception failure)
        {
            Fault(failure);
        }
    }

    private static bool WaitToRead(ChannelReader<LogEntry> reader)
    {
        var pending = reader.WaitToReadAsync(CancellationToken.None);
        return pending.IsCompleted
            ? pending.GetAwaiter().GetResult()
            : pending.AsTask().GetAwaiter().GetResult();
    }

    private void FormatBatch(ChannelReader<LogEntry> reader, ArrayBufferWriter<byte> buffer)
    {
        try
        {
            FormatEntries(reader, buffer);
        }
        finally
        {
            // Whatever the batch took from the queue is room a blocked producer can use now.
            SignalSpace();
        }
    }

    private void FormatEntries(ChannelReader<LogEntry> reader, ArrayBufferWriter<byte> buffer)
    {
        _component = LoggingMetrics.ComponentFormatter;
        while (_batch.Count < _options.BatchSize && reader.TryRead(out var entry))
        {
            // Added before formatting, so the entry is accounted for however formatting ends.
            _batch.Add(entry);
            var start = buffer.WrittenCount;
            try
            {
                entry.NormalizeFields(
                    _options.MaxFieldNameLength,
                    _options.MaxFieldsPerRecord,
                    _formatter,
                    _metrics
                );
                _formatter.Format(new LogRecord(entry), buffer);
            }
            catch (Exception)
            {
                // A failure while formatting one record costs that record, never the pipeline:
                // its partial output is cut from the batch and it is dropped and counted, while
                // the records before and after it are written as usual.
                buffer.Truncate(start);
                _batch.RemoveAt(_batch.Count - 1);
                _metrics.RecordFailure(LoggingMetrics.ComponentFormatter);
                if (_abandoned)
                {
                    // Disposal already counted this record in aggregate when it gave up.
                    LogEntryPool.Return(entry);
                }
                else
                {
                    Discard(entry, LoggingMetrics.ReasonFormatFailed);
                }
            }
        }
    }

    private void WriteBuffer(ArrayBufferWriter<byte> buffer)
    {
        if (buffer.WrittenCount > 0)
        {
            _component = LoggingMetrics.ComponentSink;
            _inFlight = _batch.Count;
            _sink.Write(buffer.WrittenSpan, _shutdown.Token);
            buffer.ResetWrittenCount();
            _inFlight = 0;
        }

        ReleaseBatch();
    }

    private void ReleaseBatch()
    {
        for (var i = 0; i < _batch.Count; i++)
        {
            LogEntryPool.Return(_batch[i]);
        }

        _batch.Clear();
    }

    private void Fault(Exception failure)
    {
        Interlocked.CompareExchange(ref _state, StateFaulted, StateRunning);
        // Closing the channel releases every producer blocked on the full queue; their waits end
        // in ChannelClosedException, which Enqueue counts as writer-fault drops.
        _queue.Writer.TryComplete();
        SignalSpace();
        _metrics.RecordFailure(_component);
        DiscardBatch(LoggingMetrics.ReasonWriterFault);
        DiscardQueued(LoggingMetrics.ReasonWriterFault);
        // Published last: anyone who observes the fault is guaranteed to find the pipeline
        // already faulted and the channel already closed.
        _writerFault = failure;
    }

    /// <summary>Counts and releases the formatted-but-unwritten batch, with real levels.</summary>
    private void DiscardBatch(string reason)
    {
        for (var i = 0; i < _batch.Count; i++)
        {
            if (_abandoned)
            {
                // Disposal already counted these in aggregate when it gave the writer up.
                LogEntryPool.Return(_batch[i]);
            }
            else
            {
                Discard(_batch[i], reason);
            }
        }

        _batch.Clear();
        _inFlight = 0;
    }

    private void DiscardQueued(string reason)
    {
        while (_queue.Reader.TryRead(out var entry))
        {
            if (_abandoned)
            {
                LogEntryPool.Return(entry);
            }
            else
            {
                Discard(entry, reason);
            }
        }
    }

    private void Discard(LogEntry entry, string reason)
    {
        Interlocked.Increment(ref _dropped);
        _metrics.RecordDropped(reason, entry.Level);
        LogEntryPool.Return(entry);
    }

    private string StateName() =>
        _state switch
        {
            StateFaulted => "faulted",
            StateDisposed => "disposed",
            _ => "running",
        };

    /// <summary>
    /// Idempotent and bounded: disposal never waits longer than the shutdown timeout per phase
    /// (drain, then sink disposal) plus a short grace, even when the sink has stopped making
    /// progress. Flushing logs must never be the thing that hangs or throws on the way down.
    /// </summary>
    public ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) == 1)
        {
            return ValueTask.CompletedTask;
        }

        Interlocked.CompareExchange(ref _state, StateDisposed, StateRunning);
        _queue.Writer.TryComplete();
        SignalSpace();

        // Every deadline of the shutdown is a kernel-timed wait on a thread of its own. Timed
        // awaits fire on the thread pool, so under pool starvation, often the very condition a
        // process is shutting down under, the shutdown budget stretched until the pool injected
        // threads. The caller only waits for the outcome.
        var shutdown = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var thread = new Thread(() =>
        {
            try
            {
                Shutdown();
            }
            catch (Exception)
            {
                // Disposal must never throw; every phase already accounts for its own failures.
                _metrics.RecordFailure(LoggingMetrics.ComponentSink);
            }
            finally
            {
                shutdown.TrySetResult();
            }
        })
        {
            IsBackground = true,
            Name = "HostLoom Logging Shutdown",
        };
        thread.Start();
        return new ValueTask(shutdown.Task);
    }

    private void Shutdown()
    {
        var finished = _completion.Task.Wait(Milliseconds(_options.ShutdownTimeout));
        if (!finished)
        {
            // Callbacks can block synchronously too. Neither they nor sink disposal may occupy
            // the caller or a shared thread-pool worker.
#pragma warning disable CA1849 // Synchronous callbacks stay on our owned background thread, not the shared pool.
            var cancellation = RunShutdownWorker(() =>
            {
                _shutdown.Cancel();
                return ValueTask.CompletedTask;
            });
#pragma warning restore CA1849
            // Neither task faults: the writer's completion is only ever set, and the worker
            // returns its failure as a value.
            Task.WaitAll([_completion.Task, cancellation], AbandonGrace);
            finished = _completion.Task.IsCompleted && cancellation.IsCompleted;
            if (cancellation.IsCompletedSuccessfully && cancellation.Result is not null)
            {
                _metrics.RecordFailure(LoggingMetrics.ComponentSink);
            }
        }

        if (finished)
        {
            // Bounded like the drain: a sink that hangs inside its own flush-on-dispose must not
            // be able to hang application shutdown.
            var disposal = RunShutdownWorker(_sink.DisposeAsync);
            if (
                !disposal.Wait(Milliseconds(_options.ShutdownTimeout))
                || disposal.Result is not null
            )
            {
                _metrics.RecordFailure(LoggingMetrics.ComponentSink);
            }

            _shutdown.Dispose();
        }
        else
        {
            // The writer is stuck inside a sink call that ignores cancellation. Abandon it: the
            // records it will never write — still queued plus the batch in flight — are counted
            // here in aggregate, the sink is not disposed because the abandoned thread may still
            // be inside Write, and the cancellation source stays undisposed for the same reason.
            // The abandoned thread is the pipeline's own dedicated background writer, never a
            // caller's, and it cannot keep the process alive.
            _abandoned = true;
            _metrics.RecordFailure(LoggingMetrics.ComponentSink);
            var stranded = _queue.Reader.Count + _inFlight;
            if (stranded > 0)
            {
                Interlocked.Add(ref _dropped, stranded);
                _metrics.RecordDropped(
                    LoggingMetrics.ReasonShutdownTimeout,
                    LogLevel.None,
                    stranded
                );
            }
        }

        _metrics.Dispose();
    }

    // Returning failures as values also observes a worker that finishes after its deadline.
    private static Task<Exception?> RunShutdownWorker(Func<ValueTask> action)
    {
        var completion = new TaskCompletionSource<Exception?>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var worker = new Thread(() =>
        {
            try
            {
                action().AsTask().GetAwaiter().GetResult();
                completion.TrySetResult(null);
            }
            catch (Exception exception)
            {
                completion.TrySetResult(exception);
            }
        })
        {
            IsBackground = true,
            Name = "HostLoom Logging Shutdown",
        };
        worker.Start();
        return completion.Task;
    }
}
