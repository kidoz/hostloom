using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

[ProviderAlias("HostLoom")]
public sealed class HostLoomLoggerProvider
    : ILoggerProvider,
        ISupportExternalScope,
        IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, HostLoomLogger> _loggers = new(
        StringComparer.Ordinal
    );
    private readonly LogPipeline _pipeline;
    private readonly HostLoomLoggerOptions _options;

    // A real scope provider from the start, so BeginScope works standalone; the logger factory
    // replaces it through ISupportExternalScope when this provider runs under one.
    private volatile IExternalScopeProvider _scopeProvider = new LoggerExternalScopeProvider();

    public HostLoomLoggerProvider(
        ILogFormatter formatter,
        ILogSink sink,
        HostLoomLoggerOptions options
    )
    {
        ArgumentNullException.ThrowIfNull(formatter);
        ArgumentNullException.ThrowIfNull(sink);
        ArgumentNullException.ThrowIfNull(options);
        _options = options;
        _pipeline = new LogPipeline(formatter, sink, options);
    }

    /// <summary>Records dropped for any reason: overload, timeout, a record the formatter
    /// failed on, a failed sink write, a writer fault, or shutdown.</summary>
    public long Dropped => _pipeline.Dropped;

    /// <summary>The defect that stopped the background writer, if any. Null while healthy; a
    /// formatter failure drops only its own record and a failed sink write only its batch, and
    /// neither sets it.</summary>
    public Exception? WriterFault => _pipeline.WriterFault;

    internal IExternalScopeProvider ScopeProvider => _scopeProvider;

    /// <summary>
    /// Waits, at most <paramref name="timeout"/>, until the writer has processed records queued
    /// before the flush marker and completed a sink flush attempt. Records queued after the
    /// marker are accepted as usual and not waited for. The provider flushes like this when the
    /// process exits or an unhandled exception ends it, but handlers run in registration order: a crash
    /// handler registered after the provider was created logs after that flush, and should call
    /// this once it has logged. <see cref="Timeout.InfiniteTimeSpan"/> waits without limit.
    /// </summary>
    /// <returns>
    /// True once processing and the sink flush attempt completed; false when the timeout passed
    /// first, the pipeline stopped short of it, or the call came from the provider's own writer
    /// thread. True does not guarantee successful delivery: records may have been dropped and
    /// the sink flush may have failed. Sink flush failures are counted in
    /// <c>hostloom.logging.failures</c> with <c>component=sink</c> and do not stop the writer.
    /// </returns>
    public bool Flush(TimeSpan timeout)
    {
        if (timeout < TimeSpan.Zero && timeout != Timeout.InfiniteTimeSpan)
        {
            throw new ArgumentOutOfRangeException(
                nameof(timeout),
                timeout,
                "The timeout must be non-negative or Timeout.InfiniteTimeSpan."
            );
        }

        return _pipeline.Flush(timeout);
    }

    public void SetScopeProvider(IExternalScopeProvider scopeProvider)
    {
        ArgumentNullException.ThrowIfNull(scopeProvider);
        _scopeProvider = scopeProvider;
    }

    public ILogger CreateLogger(string categoryName) =>
        _loggers.GetOrAdd(
            categoryName,
            static (name, provider) =>
                new HostLoomLogger(name, provider._pipeline, provider._options, provider),
            this
        );

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public ValueTask DisposeAsync()
    {
        _loggers.Clear();
        GC.SuppressFinalize(this);
        // Drains the queue before releasing the sink; an async logger that skips this loses
        // whatever was still in flight at shutdown, which is exactly when the interesting logs
        // are written. Returned rather than awaited: a continuation here would need a free
        // thread-pool thread, and the shutdown bound must hold while the pool is starved.
        return _pipeline.DisposeAsync();
    }
}
