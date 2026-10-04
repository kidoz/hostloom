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
/// one unformattable record never costs the records around it, and a failed sink write costs its
/// batch alone, so a sink that recovers gets the records after it. Only a defect in the writer
/// itself faults the pipeline, instead of silently killing the writer: the channel closes, queued
/// and in-flight records are counted as dropped, and no caller is ever left waiting on a writer
/// that has stopped reading. A process that ends without disposing the pipeline, through
/// Environment.Exit, Main returning, or an unhandled exception, still gets the queued records
/// out: its exit events flush the queue within the shutdown budget.
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
    private readonly TaskCompletionSource _shutdownCompletion = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );

    // Counts real entries owned by Enqueue, including blocked producers and formatting work.
    // -1 atomically closes accounting at abandonment; late completion cannot count them twice.
    private long _outstanding;
    private volatile Exception? _writerFault;

    /// <summary>Set once the writer wrote and flushed everything disposal left queued.</summary>
    private volatile bool _drained;

    /// <summary>Which component the writer thread is currently calling. Writer-thread only.</summary>
    private string _component = LoggingMetrics.ComponentFormatter;

    private readonly Thread _writer;

    /// <summary>Queued by <see cref="Flush"/> behind the records it waits for; never a record,
    /// never pooled. The queue is FIFO, so once the writer takes it they are all written.</summary>
    private readonly LogEntry _flushMarker = new();

    // Serializes queueing markers so their order in the queue is the order of their tickets.
    private readonly Lock _flushGate = new();
    private long _flushesRequested;

    // Monitor that flush callers wait on; the writer pulses it after each flush it completes.
    private readonly object _flushed = new();
    private long _flushesCompleted;

    /// <summary>Whether the current batch ended at a flush marker. Writer-thread only.</summary>
    private bool _flushTaken;

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
        _writer = new Thread(Run) { IsBackground = true, Name = "HostLoom Logging Writer" };
        _writer.Start();

        // A process that ends without disposing the pipeline would take the queue with it, since
        // the writer is a background thread. Environment.Exit and Main returning raise
        // ProcessExit; an unhandled exception raises UnhandledException instead. Since .NET 10 a
        // SIGTERM raises nothing unless the app handles the signal, as the Generic Host does.
        // FailFast and a killed process raise neither. Removed again on disposal.
        AppDomain.CurrentDomain.ProcessExit += OnProcessExit;
        AppDomain.CurrentDomain.UnhandledException += OnUnhandledException;
    }

    /// <summary>Records dropped for any reason. Surfaced so overload is visible, not silent.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>The defect that stopped the background writer. Null while it is healthy; sink and
    /// formatter failures cost only their records and never set it.</summary>
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

        if (options.Formatter is { } formatter && !LogFormatterNames.IsKnown(formatter))
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                formatter,
                $"Formatter must be '{LogFormatterNames.Json}' or '{LogFormatterNames.Clef}', "
                    + "or unset for the default."
            );
        }

        if (options.MaxExceptionLength < 1)
        {
            throw new ArgumentOutOfRangeException(
                nameof(options),
                options.MaxExceptionLength,
                "MaxExceptionLength must be at least 1."
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

        if (!Track(entry))
        {
            Discard(entry, LoggingMetrics.ReasonProviderDisposed);
            return;
        }

        if (_queue.Writer.TryWrite(entry))
        {
            return;
        }

        // A disposal or writer fault that closed the channel after the check above fails the
        // write too; the drop belongs to it, not to a full queue.
        var state = Volatile.Read(ref _state);
        if (state != StateRunning)
        {
            Discard(
                entry,
                state == StateFaulted
                    ? LoggingMetrics.ReasonWriterFault
                    : LoggingMetrics.ReasonProviderDisposed
            );
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
        var dropReason = WriteWhenRoom(entry, _options.EnqueueTimeout, started);
        if (dropReason is not null)
        {
            Discard(entry, dropReason);
        }

        _metrics.RecordBlockedFor(Stopwatch.GetElapsedTime(started).TotalSeconds, level);
    }

    /// <summary>
    /// Queues <paramref name="entry"/> as soon as there is room. Returns null once it is queued,
    /// or the reason it never was: the pipeline closed, or <paramref name="limit"/>, counted from
    /// <paramref name="started"/>, ran out first.
    /// </summary>
    private string? WriteWhenRoom(LogEntry entry, TimeSpan? limit, long started)
    {
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
                        return state == StateFaulted
                            ? LoggingMetrics.ReasonWriterFault
                            : LoggingMetrics.ReasonProviderDisposed;
                    }

                    var wait = Timeout.Infinite;
                    if (limit is { } bound)
                    {
                        var remaining = bound - Stopwatch.GetElapsedTime(started);
                        if (remaining <= TimeSpan.Zero)
                        {
                            return LoggingMetrics.ReasonEnqueueTimeout;
                        }

                        wait = Milliseconds(remaining);
                    }

                    Monitor.Wait(_space, wait);
                }

                return null;
            }
            finally
            {
                Interlocked.Decrement(ref _spaceWaiters);
            }
        }
    }

    /// <summary>
    /// Waits, at most <paramref name="timeout"/>, until the writer has processed records queued
    /// before the flush marker and completed a sink flush attempt; <see cref="Timeout.InfiniteTimeSpan"/>
    /// waits without limit. Records queued after the marker are not waited for, and the pipeline
    /// keeps accepting them. True means completion, not successful delivery: records may have
    /// been dropped and the sink flush may have failed. False when the deadline passed first,
    /// the writer stopped short of it, or this is the writer.
    /// </summary>
    internal bool Flush(TimeSpan timeout)
    {
        if (Thread.CurrentThread == _writer)
        {
            // The writer cannot wait for itself; a sink or formatter ending the process lands here.
            return false;
        }

        var started = Stopwatch.GetTimestamp();
        long ticket;
        while (!_flushGate.TryEnter(Budget(timeout, started)))
        {
            if (Budget(timeout, started) == 0)
            {
                return false;
            }
        }

        try
        {
            // The marker waits for room like any record would, within what is left of the timeout.
            var refused = WriteWhenRoom(
                _flushMarker,
                timeout == Timeout.InfiniteTimeSpan ? null : timeout,
                started
            );
            if (refused is not null)
            {
                // Disposal already closed the queue and is draining it; waiting for that is the
                // flush. A faulted or overdue pipeline has nothing left to wait for.
                return refused == LoggingMetrics.ReasonProviderDisposed
                    && WaitForCompletion(_completion.Task, timeout, started)
                    && _drained;
            }

            ticket = ++_flushesRequested;
        }
        finally
        {
            _flushGate.Exit();
        }

        lock (_flushed)
        {
            while (_flushesCompleted < ticket && !_completion.Task.IsCompleted)
            {
                var wait = Budget(timeout, started);
                if (wait == 0)
                {
                    return false;
                }

                Monitor.Wait(_flushed, wait);
            }

            return _flushesCompleted >= ticket;
        }
    }

    /// <summary>Milliseconds left of <paramref name="timeout"/>, counted from
    /// <paramref name="started"/> and never negative; an infinite timeout stays infinite.
    /// Long finite budgets wait in finite chunks and are recalculated after each wait.</summary>
    internal static int Budget(TimeSpan timeout, long started)
    {
        if (timeout == Timeout.InfiniteTimeSpan)
        {
            return Timeout.Infinite;
        }

        var remaining = timeout - Stopwatch.GetElapsedTime(started);
        return remaining > TimeSpan.Zero ? Milliseconds(remaining) : 0;
    }

    // The process is ending and nothing will dispose the pipeline, so nothing else would get the
    // queue out: flush it within the same budget disposal would have had.
    private void OnProcessExit(object? sender, EventArgs e) => Flush(_options.ShutdownTimeout);

    private void OnUnhandledException(object? sender, UnhandledExceptionEventArgs e) =>
        Flush(_options.ShutdownTimeout);

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
            ? int.MaxValue
            : (int)Math.Ceiling(span.TotalMilliseconds);

    private static bool WaitForCompletion(Task task, TimeSpan timeout, long started)
    {
        while (!task.IsCompleted)
        {
            var wait = Budget(timeout, started);
            if (wait == 0)
            {
                return task.IsCompleted;
            }

            if (task.Wait(wait))
            {
                return true;
            }
        }

        return true;
    }

    private void Run()
    {
        try
        {
            RunLoop();
        }
        finally
        {
            _completion.TrySetResult();
            // A flush caller still waiting now waits for nothing: release it.
            lock (_flushed)
            {
                Monitor.PulseAll(_flushed);
            }
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
                if (_flushTaken)
                {
                    // The batch ended at a flush marker, so preceding records were processed,
                    // including any dropped on failure. Completion includes a failed flush attempt.
                    _flushTaken = false;
                    FlushSink();
                    lock (_flushed)
                    {
                        _flushesCompleted++;
                        Monitor.PulseAll(_flushed);
                    }
                }
            }

            // False from WaitToRead means completed and empty: every accepted record was processed.
            FlushSink();
            _drained = true;
        }
        catch (Exception) when (_shutdown.IsCancellationRequested)
        {
            // The shutdown deadline expired and disposal cancelled the sink mid-batch; whatever
            // the sink threw on its way out is that cancellation's doing.
            DiscardBatch(LoggingMetrics.ReasonShutdownTimeout);
            DiscardQueued(LoggingMetrics.ReasonShutdownTimeout);
        }
        catch (Exception failure)
        {
            // Sink and formatter failures are handled where they happen; this is a defect.
            Fault(failure);
        }
    }

    /// <summary>
    /// A failed flush loses nothing the pipeline can still count, since the records are already
    /// the sink's, and it stops nothing: it is counted and the writer carries on. Only disposal
    /// giving up on the writer ends the loop from here.
    /// </summary>
    private void FlushSink()
    {
        _component = LoggingMetrics.ComponentSink;
        try
        {
            var flush = _sink.FlushAsync(_shutdown.Token);
            if (!flush.IsCompletedSuccessfully)
            {
                flush.AsTask().GetAwaiter().GetResult();
            }
        }
        catch (Exception) when (!_shutdown.IsCancellationRequested)
        {
            _metrics.RecordFailure(LoggingMetrics.ComponentSink);
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
            if (ReferenceEquals(entry, _flushMarker))
            {
                // The batch ends here, so the flush follows exactly the records queued before it.
                _flushTaken = true;
                break;
            }

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
                Discard(entry, LoggingMetrics.ReasonFormatFailed);
            }
        }
    }

    private void WriteBuffer(ArrayBufferWriter<byte> buffer)
    {
        if (buffer.WrittenCount > 0)
        {
            _component = LoggingMetrics.ComponentSink;
            try
            {
                _sink.Write(buffer.WrittenSpan, _shutdown.Token);
            }
            catch (Exception) when (!_shutdown.IsCancellationRequested)
            {
                // A failed write costs its batch, never the pipeline: the records are counted and
                // the writer goes on, so a sink that recovers, a disk with space again or a
                // reconnected socket, gets the records after it. The batch is not retried: the
                // sink may have written part of it already.
                _metrics.RecordFailure(LoggingMetrics.ComponentSink);
                buffer.ResetWrittenCount();
                DiscardBatch(LoggingMetrics.ReasonSinkFailed);
                return;
            }

            buffer.ResetWrittenCount();
        }

        ReleaseBatch();
    }

    private void ReleaseBatch()
    {
        for (var i = 0; i < _batch.Count; i++)
        {
            CompleteRecord(_batch[i]);
            LogEntryPool.Return(_batch[i]);
        }

        _batch.Clear();
    }

    private void Fault(Exception failure)
    {
        Interlocked.CompareExchange(ref _state, StateFaulted, StateRunning);
        // Completing the channel ends the writer's reads; SignalSpace below releases producers
        // blocked on the full queue, which then observe the fault and count their waits as
        // writer-fault drops.
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
            Discard(_batch[i], reason);
        }

        _batch.Clear();
    }

    private void DiscardQueued(string reason)
    {
        while (_queue.Reader.TryRead(out var entry))
        {
            if (ReferenceEquals(entry, _flushMarker))
            {
                // Not a record: nothing to count, and never the pool's.
                continue;
            }

            Discard(entry, reason);
        }
    }

    private void Discard(LogEntry entry, string reason)
    {
        if (CompleteRecord(entry))
        {
            Interlocked.Increment(ref _dropped);
            _metrics.RecordDropped(reason, entry.Level);
        }
        LogEntryPool.Return(entry);
    }

    private bool Track(LogEntry entry)
    {
        while (true)
        {
            var count = Interlocked.Read(ref _outstanding);
            if (count < 0)
            {
                return false;
            }
            if (Interlocked.CompareExchange(ref _outstanding, count + 1, count) == count)
            {
                entry.AccountingPending = true;
                return true;
            }
        }
    }

    // False means shutdown already counted this entry when it abandoned outstanding work.
    private bool CompleteRecord(LogEntry entry)
    {
        if (!entry.AccountingPending)
        {
            return true;
        }
        entry.AccountingPending = false;
        while (true)
        {
            var count = Interlocked.Read(ref _outstanding);
            if (count < 0)
            {
                return false;
            }
            if (Interlocked.CompareExchange(ref _outstanding, count - 1, count) == count)
            {
                return true;
            }
        }
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
            return new ValueTask(_shutdownCompletion.Task);
        }

        // Disposal drains the queue itself, and the handlers would keep the pipeline reachable.
        AppDomain.CurrentDomain.ProcessExit -= OnProcessExit;
        AppDomain.CurrentDomain.UnhandledException -= OnUnhandledException;
        Interlocked.CompareExchange(ref _state, StateDisposed, StateRunning);
        _queue.Writer.TryComplete();
        SignalSpace();

        // Every deadline of the shutdown is a kernel-timed wait on a thread of its own. Timed
        // awaits fire on the thread pool, so under pool starvation, often the very condition a
        // process is shutting down under, the shutdown budget stretched until the pool injected
        // threads. The caller only waits for the outcome.
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
                _shutdownCompletion.TrySetResult();
            }
        })
        {
            IsBackground = true,
            Name = "HostLoom Logging Shutdown",
        };
        thread.Start();
        return new ValueTask(_shutdownCompletion.Task);
    }

    private void Shutdown()
    {
        var finished = WaitForCompletion(
            _completion.Task,
            _options.ShutdownTimeout,
            Stopwatch.GetTimestamp()
        );
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
                !WaitForCompletion(disposal, _options.ShutdownTimeout, Stopwatch.GetTimestamp())
                || disposal.Result is not null
            )
            {
                _metrics.RecordFailure(LoggingMetrics.ComponentSink);
            }

            _shutdown.Dispose();
        }
        else
        {
            // The writer or cancellation callbacks are stuck. Count every outstanding record,
            // including formatting work and blocked producers, but never flush markers. The
            // sink is not disposed because the abandoned thread may still
            // be inside Write, and the cancellation source stays undisposed for the same reason.
            // The abandoned thread is the pipeline's own dedicated background writer, never a
            // caller's, and it cannot keep the process alive.
            var stranded = Interlocked.Exchange(ref _outstanding, -1);
            _metrics.RecordFailure(LoggingMetrics.ComponentSink);
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
