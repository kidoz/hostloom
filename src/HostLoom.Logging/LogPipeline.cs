using System.Buffers;
using System.Diagnostics;
using System.Threading.Channels;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// Single-reader queue plus a background writer. The calling thread renders and enqueues; all
/// formatting and I/O happens off it, which is what keeps a log call off the tail latency path.
/// </summary>
internal sealed class LogPipeline : IAsyncDisposable
{
    private static readonly long StartTimestamp = Stopwatch.GetTimestamp();
    private static readonly DateTimeOffset StartWallClock = DateTimeOffset.UtcNow;

    private readonly Channel<LogEntry> _queue;
    private readonly ILogFormatter _formatter;
    private readonly ILogSink _sink;
    private readonly HostLoomLoggerOptions _options;
    private readonly Task _writer;
    private readonly CancellationTokenSource _stopping = new();
    private long _dropped;
    private int _disposed;

    public LogPipeline(ILogFormatter formatter, ILogSink sink, HostLoomLoggerOptions options)
    {
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

        _writer = Task
            .Factory.StartNew(
                RunAsync,
                CancellationToken.None,
                TaskCreationOptions.LongRunning,
                TaskScheduler.Default
            )
            .Unwrap();
    }

    /// <summary>Records dropped because the queue was full. Surfaced so overload is visible, not silent.</summary>
    public long Dropped => Interlocked.Read(ref _dropped);

    public void Enqueue(LogEntry entry)
    {
        if (_queue.Writer.TryWrite(entry))
        {
            return;
        }

        switch (_options.QueueFullPolicy)
        {
            case QueueFullPolicy.Block:
                // Deliberately synchronous: the caller asked for backpressure over loss.
                var pending = _queue.Writer.WriteAsync(entry, CancellationToken.None);
                if (!pending.IsCompletedSuccessfully)
                {
                    pending.AsTask().GetAwaiter().GetResult();
                }

                return;

            case QueueFullPolicy.DropBelowWarning when entry.Level >= LogLevel.Warning:
                goto case QueueFullPolicy.Block;

            default:
                Interlocked.Increment(ref _dropped);
                LogEntryPool.Return(entry);
                return;
        }
    }

    private async Task RunAsync()
    {
        var buffer = new ArrayBufferWriter<byte>(64 * 1024);
        var reader = _queue.Reader;

        try
        {
            while (await reader.WaitToReadAsync(_stopping.Token).ConfigureAwait(false))
            {
                var batched = 0;
                while (batched < _options.BatchSize && reader.TryRead(out var entry))
                {
                    try
                    {
                        var record = new LogRecord(entry, ToWallClock(entry.Timestamp));
                        _formatter.Format(record, buffer);
                        batched++;
                    }
                    finally
                    {
                        LogEntryPool.Return(entry);
                    }
                }

                if (buffer.WrittenCount > 0)
                {
                    _sink.Write(buffer.WrittenSpan);
                    buffer.ResetWrittenCount();
                }
            }
        }
        catch (OperationCanceledException)
        {
            // Shutdown; the drain below still runs.
        }

        await DrainAsync(buffer).ConfigureAwait(false);
    }

    private async Task DrainAsync(ArrayBufferWriter<byte> buffer)
    {
        while (_queue.Reader.TryRead(out var entry))
        {
            try
            {
                _formatter.Format(new LogRecord(entry, ToWallClock(entry.Timestamp)), buffer);
            }
            finally
            {
                LogEntryPool.Return(entry);
            }
        }

        if (buffer.WrittenCount > 0)
        {
            _sink.Write(buffer.WrittenSpan);
            buffer.ResetWrittenCount();
        }

        await _sink.FlushAsync(CancellationToken.None).ConfigureAwait(false);
    }

    /// <summary>
    /// Timestamps are taken as raw ticks on the hot path and converted here, because
    /// <see cref="Stopwatch.GetTimestamp"/> is markedly cheaper than reading wall-clock time.
    /// </summary>
    private static DateTimeOffset ToWallClock(long timestamp) =>
        StartWallClock + Stopwatch.GetElapsedTime(StartTimestamp, timestamp);

    /// <summary>
    /// Idempotent: a provider is routinely disposed by the container and again by a using block,
    /// and flushing logs must never be the thing that throws on the way down.
    /// </summary>
    public async ValueTask DisposeAsync()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        _queue.Writer.TryComplete();
        await _stopping.CancelAsync().ConfigureAwait(false);
        try
        {
            await _writer.ConfigureAwait(false);
        }
        catch (Exception)
        {
            // A failed writer must not stop disposal from releasing the sink.
        }

        await _sink.DisposeAsync().ConfigureAwait(false);
        _stopping.Dispose();
    }
}
