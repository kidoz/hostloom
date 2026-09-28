using System.Collections.Concurrent;
using Microsoft.Extensions.Logging;

namespace HostLoom.IntegrationTests.Infrastructure;

/// <summary>
/// Keeps every line a test's components log, so an assertion that fails against a real backend can
/// report what the components saw, such as a store failure a fail-open path absorbed, instead of
/// only the symptom.
/// </summary>
internal sealed class LogCapture : ILoggerProvider
{
    private readonly ConcurrentQueue<string> _lines = new();

    public ILogger CreateLogger(string categoryName) => new CapturingLogger(this, categoryName);

    /// <summary>Every captured line in order, for an assertion message.</summary>
    public string Describe() => _lines.IsEmpty ? "Nothing was logged." : string.Join(" | ", _lines);

    public void Dispose() { }

    private sealed class CapturingLogger(LogCapture capture, string category) : ILogger
    {
        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        ) =>
            capture._lines.Enqueue(
                exception is null
                    ? $"{logLevel} {category}: {formatter(state, exception)}"
                    : $"{logLevel} {category}: {formatter(state, exception)} ({exception.GetType().Name}: {exception.Message})"
            );
    }
}
