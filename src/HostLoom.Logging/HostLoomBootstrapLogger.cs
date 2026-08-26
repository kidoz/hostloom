using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// Synchronous pre-DI logger for the window before the host and provider exist. It emits the same
/// events as the hosted provider — same formatter (CLEF by default), typed property model,
/// masking policy, TimeProvider timestamps, static machine/service fields, and enrichers — but
/// formats and writes each event to the output stream on the calling thread, so a startup crash
/// cannot lose what was already logged. It honors a minimum level supplied at construction,
/// because no MEL filtering exists before the host is built. The hand-off is clean: it retains
/// nothing, so disposing it once the hosted provider is up neither replays nor duplicates events.
/// Failures are swallowed unless <c>failFast</c> is set — bootstrap logging must never be the
/// reason a host cannot start. Scopes require the host's scope infrastructure and are not
/// supported here; <see cref="BeginScope{TState}"/> is a no-op.
/// </summary>
public sealed class HostLoomBootstrapLogger : ILogger, IDisposable
{
    private readonly HostLoomLoggerOptions _options;
    private readonly ILogFormatter _formatter;
    private readonly Stream _output;
    private readonly bool _ownsOutput;
    private readonly bool _failFast;
    private readonly LogLevel _minimumLevel;
    private readonly string _category;
    private readonly EventCapture _capture;
    private readonly StaticField[] _staticFields;
    private readonly ArrayBufferWriter<byte> _buffer = new(4 * 1024);
    private readonly Lock _gate = new();
    private int _disposed;

    /// <param name="options">Shared with the future hosted provider so both emit identically;
    /// queue and shutdown settings do not apply to this synchronous logger.</param>
    /// <param name="formatter">Defaults to <see cref="ClefLogFormatter"/>.</param>
    /// <param name="output">Defaults to standard output, which is then owned and disposed.</param>
    /// <param name="minimumLevel">The only level filter that exists before the host.</param>
    /// <param name="category">Emitted as the logger category on every event.</param>
    /// <param name="failFast">Opt-in: rethrow bootstrap logging failures instead of swallowing.</param>
    public HostLoomBootstrapLogger(
        HostLoomLoggerOptions? options = null,
        ILogFormatter? formatter = null,
        Stream? output = null,
        LogLevel minimumLevel = LogLevel.Information,
        string category = "Bootstrap",
        bool failFast = false
    )
    {
        _options = options ?? new HostLoomLoggerOptions();
        LogPipeline.Validate(_options);
        _formatter = formatter ?? new ClefLogFormatter();
        _ownsOutput = output is null;
        _output = output ?? Console.OpenStandardOutput();
        _minimumLevel = minimumLevel;
        _category = category;
        _failFast = failFast;
        _capture = new EventCapture(
            _options,
            new Destructurer(_options.Destructuring, null),
            null
        );
        _staticFields = LogPipeline.BuildStaticFields(_options);
    }

    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => null;

    public bool IsEnabled(LogLevel logLevel) =>
        logLevel != LogLevel.None && logLevel >= _minimumLevel;

    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        ArgumentNullException.ThrowIfNull(formatter);
        try
        {
            WriteEvent(logLevel, eventId, state, exception, formatter);
        }
        catch (Exception) when (!_failFast)
        {
            // Swallowed by contract: a broken bootstrap log line must not stop host startup.
        }
    }

    private void WriteEvent<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        var entry = LogEntryPool.Rent();
        try
        {
            entry.Level = logLevel;
            var destructured = _capture.CaptureState(entry, state);
            if (destructured && entry.Template is { } template)
            {
                EventCapture.RenderTemplate(entry, template);
            }
            else
            {
                entry.AppendLiteral(formatter(state, exception));
            }

            entry.Category = _category;
            entry.EventId = eventId;
            entry.Exception = exception;
            entry.Timestamp = _options.TimeProvider.GetUtcNow();
            entry.ThreadId = Environment.CurrentManagedThreadId;
            if (_options.CaptureActivity && Activity.Current is { } activity)
            {
                entry.TraceId = activity.TraceId;
                entry.SpanId = activity.SpanId;
                entry.HasActivity = true;
            }

            var enrichers = _options.Enrichers;
            for (var i = 0; i < enrichers.Count; i++)
            {
                var writer = new LogEntryWriter(entry);
                try
                {
                    enrichers[i].Enrich(ref writer);
                }
                catch (Exception) when (!_failFast)
                {
                    // No metrics exist yet; isolation is all this logger can offer.
                }
            }

            for (var i = 0; i < _staticFields.Length; i++)
            {
                entry.AddFieldUtf8Text(
                    _staticFields[i].Name,
                    _staticFields[i].Value,
                    LogFieldSource.Static
                );
            }

            entry.NormalizeFields(
                _options.MaxFieldNameLength,
                _options.MaxFieldsPerRecord,
                _formatter,
                null
            );

            lock (_gate)
            {
                ObjectDisposedException.ThrowIf(_disposed == 1, this);
                _buffer.ResetWrittenCount();
                _formatter.Format(new LogRecord(entry), _buffer);
                _output.Write(_buffer.WrittenSpan);
            }
        }
        finally
        {
            LogEntryPool.Return(entry);
        }
    }

    /// <summary>Flushes the output stream; call before handing off to the hosted provider.</summary>
    public void Flush()
    {
        lock (_gate)
        {
            if (_disposed == 0)
            {
                _output.Flush();
            }
        }
    }

    public void Dispose()
    {
        if (Interlocked.Exchange(ref _disposed, 1) == 1)
        {
            return;
        }

        lock (_gate)
        {
            try
            {
                _output.Flush();
                if (_ownsOutput)
                {
                    _output.Dispose();
                }
            }
            catch (Exception) when (!_failFast)
            {
                // A stream that fails on the way down must not break startup either.
            }
        }
    }
}
