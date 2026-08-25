using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

[ProviderAlias("HostLoom")]
public sealed class HostLoomLoggerProvider : ILoggerProvider, IAsyncDisposable
{
    private readonly ConcurrentDictionary<string, HostLoomLogger> _loggers = new(
        StringComparer.Ordinal
    );
    private readonly LogPipeline _pipeline;
    private readonly HostLoomLoggerOptions _options;

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

    /// <summary>Records dropped because the queue was full.</summary>
    public long Dropped => _pipeline.Dropped;

    public ILogger CreateLogger(string categoryName) =>
        _loggers.GetOrAdd(
            categoryName,
            static (name, state) => new HostLoomLogger(name, state.Pipeline, state.Options),
            (Pipeline: _pipeline, Options: _options)
        );

    public void Dispose() => DisposeAsync().AsTask().GetAwaiter().GetResult();

    public async ValueTask DisposeAsync()
    {
        // Drains the queue before releasing the sink; an async logger that skips this loses whatever
        // was still in flight at shutdown, which is exactly when the interesting logs are written.
        await _pipeline.DisposeAsync().ConfigureAwait(false);
        _loggers.Clear();
        GC.SuppressFinalize(this);
    }
}
