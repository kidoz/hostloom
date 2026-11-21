using System.Globalization;
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
/// <c>ILogger&lt;T&gt;</c> is the framework's aggregating wrapper, so these extensions hand
/// bounded key/value snapshots through the standard interface. Each HostLoom receiver uses
/// its configured capture limits; other providers receive the default snapshot. Numeric and boolean fields keep their types; text stays text.
/// </remarks>
public static class LoggerExtensions
{
    public static void LogFast(
        this ILogger logger,
        LogLevel level,
        [InterpolatedStringHandlerArgument(nameof(logger), nameof(level))]
            ref LogMessageHandler message
    ) => Emit(logger, level, ref message, default, null);

    public static void LogFast(
        this ILogger logger,
        LogLevel level,
        Exception? exception,
        [InterpolatedStringHandlerArgument(nameof(logger), nameof(level))]
            ref LogMessageHandler message
    ) => Emit(logger, level, ref message, default, exception);

    public static void LogFast(
        this ILogger logger,
        LogLevel level,
        EventId eventId,
        [InterpolatedStringHandlerArgument(nameof(logger), nameof(level))]
            ref LogMessageHandler message
    ) => Emit(logger, level, ref message, eventId, null);

    private static void Emit(
        ILogger logger,
        LogLevel level,
        ref LogMessageHandler message,
        EventId eventId,
        Exception? exception
    )
    {
        if (message.Entry is { } entry && logger is HostLoomLogger fast)
        {
            fast.Emit(entry, eventId, exception);
            return;
        }

        if (message.Wrapped is not { } capture)
            return;

        // The public MEL view remains valid if a provider retains the state. HostLoom
        // providers select the bounded snapshot for their own caps.
        try
        {
            var state = capture.Snapshot();
            logger.Log(level, eventId, state, exception, static (s, _) => s.ToString());
        }
        finally
        {
            capture.Release();
        }
    }

    internal static object? BoxValue(ReadOnlySpan<byte> value, LogFieldKind kind)
    {
        if (kind == LogFieldKind.Null)
            return null;
        if (kind == LogFieldKind.Boolean)
            return value.SequenceEqual("true"u8);
        if (kind != LogFieldKind.Number)
            return System.Text.Encoding.UTF8.GetString(value);

        // The handler's concrete numeric overloads emit canonical int/long/decimal/double
        // tokens. Never parse an exponent through decimal: tiny doubles can round to zero.
        // Preserve negative zero too, rather than converting it to an integer zero.
        if (value.SequenceEqual("-0"u8))
            return -0.0d;
        if (
            long.TryParse(
                value,
                NumberStyles.AllowLeadingSign,
                CultureInfo.InvariantCulture,
                out var integer
            )
        )
            return integer;
        if (
            decimal.TryParse(
                value,
                NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                CultureInfo.InvariantCulture,
                out var number
            )
        )
            return number;
        if (
            Int128.TryParse(
                value,
                NumberStyles.AllowLeadingSign,
                CultureInfo.InvariantCulture,
                out var signed
            )
        )
            return signed;
        if (
            UInt128.TryParse(
                value,
                NumberStyles.None,
                CultureInfo.InvariantCulture,
                out var unsigned
            )
        )
            return unsigned;
        return double.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture);
    }
}
