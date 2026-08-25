using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// The allocation-free call sites. Anything logged through the ordinary
/// <see cref="ILogger.Log{TState}"/> overloads still works, it just pays the boxing the interface
/// mandates — including logs from third-party libraries, which these extensions cannot reach.
/// </summary>
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

        // Another provider is installed. Render once and hand it over through the standard
        // interface so the call site behaves identically, just without the fast path.
        try
        {
            // CA1873: the handler already proved the level is enabled — an entry only exists when
            // IsEnabled returned true — so this transcoding is not speculative work.
#pragma warning disable CA1873
            logger.Log(
                entry.Level,
                eventId,
                System.Text.Encoding.UTF8.GetString(entry.Message),
                exception,
                static (state, _) => state
            );
#pragma warning restore CA1873
        }
        finally
        {
            LogEntryPool.Return(entry);
        }
    }
}
