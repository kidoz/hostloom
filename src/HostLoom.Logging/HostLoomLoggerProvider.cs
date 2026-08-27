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

    /// <summary>Records dropped for any reason: overload, timeout, fault, or shutdown.</summary>
    public long Dropped => _pipeline.Dropped;

    /// <summary>The failure that faulted the background writer, if any. Null while healthy.</summary>
    public Exception? WriterFault => _pipeline.WriterFault;

    internal IExternalScopeProvider ScopeProvider => _scopeProvider;

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

    public async ValueTask DisposeAsync()
    {
        // Drains the queue before releasing the sink; an async logger that skips this loses whatever
        // was still in flight at shutdown, which is exactly when the interesting logs are written.
        await _pipeline.DisposeAsync().ConfigureAwait(false);
        _loggers.Clear();
        GC.SuppressFinalize(this);
    }
}
