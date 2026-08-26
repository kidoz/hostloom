using System.Buffers;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// Single-reader queue plus a background writer. The calling thread renders and enqueues; all
/// formatting and I/O happens off it, which is what keeps a log call off the tail latency path.
/// An unexpected formatter or sink failure faults the pipeline instead of silently killing the
/// writer: the channel closes, queued and later records are counted as dropped, and no caller is
/// ever left waiting on a writer that has stopped reading.
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
    private readonly LoggingMetrics _metrics;
    private readonly CancellationTokenSource _shutdown = new();
    private readonly Task _writer;
    private long _dropped;
    private int _state;
    private int _disposeStarted;
    private volatile bool _abandoned;
    private volatile Exception? _writerFault;

    /// <summary>Which component the writer thread is currently calling. Writer-thread only.</summary>
    private string _component = LoggingMetrics.ComponentFormatter;

    public LogPipeline(ILogFormatter formatter, ILogSink sink, HostLoomLoggerOptions options)
    {
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

        _formatter = formatter;
        _sink = sink;
        _options = options;
        _queue = Channel.CreateBounded<LogEntry>(
            new BoundedChannelOptions(options.QueueCapacity)
            {
                SingleReader = true,
                SingleWriter = false,
                FullMode = BoundedChannelFullMode.Wait,
            }
        );
        _metrics = new LoggingMetrics(
            () => _queue.Reader.Count,
            () => _state == StateRunning,
            StateName
        );

        _writer = Task
            .Factory.StartNew(
                RunAsync,
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default
            )
            .Unwrap();
    }

    /// <summary>Records dropped for any reason. Surfaced so overload is visible, not silent.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    /// <summary>The failure that faulted the background writer. Null while it is healthy.</summary>
    public Exception? WriterFault => _writerFault;

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
    /// the channel closes, so a faulted or disposed pipeline never strands a caller.
    /// </summary>
    private void BlockingWrite(LogEntry entry)
    {
        var level = entry.Level;
        _metrics.RecordBlocked(level);
        var started = Stopwatch.GetTimestamp();
        try
        {
            if (_options.EnqueueTimeout is { } limit)
            {
                using var timeout = new CancellationTokenSource(limit);
                Wait(_queue.Writer.WriteAsync(entry, timeout.Token));
            }
            else
            {
                Wait(_queue.Writer.WriteAsync(entry, CancellationToken.None));
            }
        }
        catch (OperationCanceledException)
        {
            Discard(entry, LoggingMetrics.ReasonEnqueueTimeout);
        }
        catch (ChannelClosedException)
        {
            Discard(
                entry,
                _state == StateFaulted
                    ? LoggingMetrics.ReasonWriterFault
                    : LoggingMetrics.ReasonProviderDisposed
            );
        }
        finally
        {
            _metrics.RecordBlockedFor(Stopwatch.GetElapsedTime(started).TotalSeconds, level);
        }
    }

    private static void Wait(ValueTask pending)
    {
        if (!pending.IsCompletedSuccessfully)
        {
            pending.AsTask().GetAwaiter().GetResult();
        }
    }

    private async Task RunAsync()
    {
        var buffer = new ArrayBufferWriter<byte>(64 * 1024);
        var reader = _queue.Reader;

        try
        {
            while (await reader.WaitToReadAsync(CancellationToken.None).ConfigureAwait(false))
            {
                FormatBatch(reader, buffer);
                WriteBuffer(buffer);
            }

            // False from WaitToReadAsync means completed and empty: every accepted record is out.
            _component = LoggingMetrics.ComponentSink;
            await _sink.FlushAsync(_shutdown.Token).ConfigureAwait(false);
        }
        catch (OperationCanceledException)
        {
            // The shutdown deadline expired and disposal cancelled the sink mid-batch.
            DiscardQueued(LoggingMetrics.ReasonShutdownTimeout);
        }
        catch (Exception failure)
        {
            Fault(failure);
        }
    }

    private void FormatBatch(ChannelReader<LogEntry> reader, ArrayBufferWriter<byte> buffer)
    {
        _component = LoggingMetrics.ComponentFormatter;
        var batched = 0;
        while (batched < _options.BatchSize && reader.TryRead(out var entry))
        {
            try
            {
                _formatter.Format(new LogRecord(entry), buffer);
                batched++;
            }
            finally
            {
                LogEntryPool.Return(entry);
            }
        }
    }

    private void WriteBuffer(ArrayBufferWriter<byte> buffer)
    {
        if (buffer.WrittenCount == 0)
        {
            return;
        }

        _component = LoggingMetrics.ComponentSink;
        _sink.Write(buffer.WrittenSpan, _shutdown.Token);
        buffer.ResetWrittenCount();
    }

    private void Fault(Exception failure)
    {
        Interlocked.CompareExchange(ref _state, StateFaulted, StateRunning);
        // Closing the channel releases every producer blocked on the full queue; their waits end
        // in ChannelClosedException, which Enqueue counts as writer-fault drops.
        _queue.Writer.TryComplete();
        _metrics.RecordFailure(_component);
        DiscardQueued(LoggingMetrics.ReasonWriterFault);
        // Published last: anyone who observes the fault is guaranteed to find the pipeline
        // already faulted and the channel already closed.
        _writerFault = failure;
    }

    private void DiscardQueued(string reason)
    {
        while (_queue.Reader.TryRead(out var entry))
        {
            if (_abandoned)
            {
                // Disposal already counted these in aggregate when it gave the writer up.
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
    /// Idempotent and bounded: disposal never waits longer than the shutdown timeout plus a short
    /// grace, even when the sink has stopped making progress. Flushing logs must never be the
    /// thing that hangs or throws on the way down.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposeStarted, 1) == 1)
        {
            return;
        }

        Interlocked.CompareExchange(ref _state, StateDisposed, StateRunning);
        _queue.Writer.TryComplete();

        var finished = await WaitForWriterAsync(_options.ShutdownTimeout).ConfigureAwait(false);
        if (!finished)
        {
            // Deadline reached: ask the sink to abort cooperatively, then grant a short grace.
            await _shutdown.CancelAsync().ConfigureAwait(false);
            finished = await WaitForWriterAsync(AbandonGrace).ConfigureAwait(false);
        }

        if (finished)
        {
            try
            {
                await _sink.DisposeAsync().ConfigureAwait(false);
            }
            catch (Exception)
            {
                // A sink that fails on the way down must not break application shutdown.
                _metrics.RecordFailure(LoggingMetrics.ComponentSink);
            }

            _shutdown.Dispose();
        }
        else
        {
            // The writer is stuck inside a sink call that ignores cancellation. Abandon it: the
            // records it will never write are counted here in aggregate, the sink is not disposed
            // because the abandoned thread may still be inside Write, and the cancellation source
            // stays undisposed for the same reason. The abandoned thread is the pipeline's own
            // long-running writer, never a caller's.
            _abandoned = true;
            _metrics.RecordFailure(LoggingMetrics.ComponentSink);
            var stranded = _queue.Reader.Count;
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

    private async ValueTask<bool> WaitForWriterAsync(TimeSpan timeout)
    {
        try
        {
            await _writer.WaitAsync(timeout).ConfigureAwait(false);
            return true;
        }
        catch (TimeoutException)
        {
            return false;
        }
    }
}
