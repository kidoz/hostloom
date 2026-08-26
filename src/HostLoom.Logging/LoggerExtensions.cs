using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// The allocation-free call sites. Anything logged through the ordinary
/// <see cref="ILogger.Log{TState}"/> overloads still works, it just pays the boxing the interface
/// mandates — including logs from third-party libraries, which these extensions cannot reach.
/// </summary>
/// <remarks>
/// The no-box fast path engages only when the logger is HostLoom's own, obtained from
/// <see cref="HostLoomLoggerProvider.CreateLogger"/>. A dependency-injected
/// <c>ILogger&lt;T&gt;</c> is the framework's aggregating wrapper, so these extensions render
/// once and hand structured key/value state through the standard interface: the captured hole
/// names and values survive into any structured provider, but without the zero-allocation
/// guarantee, and values travel as strings rather than typed tokens.
/// </remarks>
public static class LoggerExtensions
{
    public static void LogFast(
        this ILogger logger,
        LogLevel level,
        [InterpolatedStringHandlerArgument(nameof(logger), nameof(level))]
            ref LogMessageHandler message
    ) => Emit(logger, ref message, default, null);

    public static void LogFast(
        this ILogger logger,
        LogLevel level,
        Exception? exception,
        [InterpolatedStringHandlerArgument(nameof(logger), nameof(level))]
            ref LogMessageHandler message
    ) => Emit(logger, ref message, default, exception);

    public static void LogFast(
        this ILogger logger,
        LogLevel level,
        EventId eventId,
        [InterpolatedStringHandlerArgument(nameof(logger), nameof(level))]
            ref LogMessageHandler message
    ) => Emit(logger, ref message, eventId, null);

    private static void Emit(
        ILogger logger,
        ref LogMessageHandler message,
        EventId eventId,
        Exception? exception
    )
    {
        if (message.Entry is not { } entry)
        {
            // Level disabled: the compiler never evaluated the interpolation holes.
            return;
        }

        if (logger is HostLoomLogger fast)
        {
            fast.Emit(entry, eventId, exception);
            return;
        }

        // Another provider (or a wrapper) is installed. Render once and hand over structured
        // state through the standard interface, so the captured hole names survive: any provider
        // that understands key/value state — including this library's own logger behind a
        // dependency-injected wrapper — keeps the fields. Values travel as strings here; only
        // the direct fast path preserves value kinds without boxing.
        try
        {
            // CA1873: the handler already proved the level is enabled — an entry only exists when
            // IsEnabled returned true — so this transcoding is not speculative work.
#pragma warning disable CA1873
            var fields = new KeyValuePair<string, object?>[entry.FieldCount];
            for (var i = 0; i < fields.Length; i++)
            {
                entry.GetField(i, out var name, out var value, out _);
                fields[i] = new KeyValuePair<string, object?>(
                    System.Text.Encoding.UTF8.GetString(name),
                    System.Text.Encoding.UTF8.GetString(value)
                );
            }

            var state = new HandoffState(
                System.Text.Encoding.UTF8.GetString(entry.Message),
                fields
            );
            logger.Log(entry.Level, eventId, state, exception, static (s, _) => s.ToString());
#pragma warning restore CA1873
        }
        finally
        {
            LogEntryPool.Return(entry);
        }
    }

    /// <summary>
    /// The rendered message plus the captured fields, in the key/value shape every structured
    /// provider recognizes. <see cref="ToString"/> returns the rendered message, for sinks that
    /// render state directly.
    /// </summary>
    private sealed class HandoffState(string message, KeyValuePair<string, object?>[] fields)
        : IReadOnlyList<KeyValuePair<string, object?>>
    {
        public KeyValuePair<string, object?> this[int index] => fields[index];

        public int Count => fields.Length;

        public IEnumerator<KeyValuePair<string, object?>> GetEnumerator() =>
            ((IEnumerable<KeyValuePair<string, object?>>)fields).GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();

        public override string ToString() => message;
    }
}
