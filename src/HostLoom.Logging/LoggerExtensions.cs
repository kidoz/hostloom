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
/// <c>ILogger&lt;T&gt;</c> is the framework's aggregating wrapper, so these extensions render
/// once and hand structured key/value state through the standard interface: the captured hole
/// names and values survive into any structured provider, but without the zero-allocation
/// guarantee. Numeric and boolean fields keep their types; text stays text.
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
        // dependency-injected wrapper — keeps the fields. Boxing here preserves the captured
        // numeric and boolean kinds; only the direct fast path avoids these allocations.
        try
        {
            // CA1873: the handler already proved the level is enabled — an entry only exists when
            // IsEnabled returned true — so this transcoding is not speculative work.
#pragma warning disable CA1873
            var fields = new KeyValuePair<string, object?>[entry.FieldCount];
            for (var i = 0; i < fields.Length; i++)
            {
                entry.GetHandoffField(i, out var name, out var value, out var kind);
                fields[i] = new KeyValuePair<string, object?>(name, BoxValue(value, kind));
            }

            var state = new HandoffState(
                System.Text.Encoding.UTF8.GetString(entry.Message),
                fields,
                entry.CaptureFailures,
                entry.OverflowHoleFields
            );
            logger.Log(entry.Level, eventId, state, exception, static (s, _) => s.ToString());
#pragma warning restore CA1873
        }
        finally
        {
            LogEntryPool.Return(entry);
        }
    }

    private static object? BoxValue(ReadOnlySpan<byte> value, LogFieldKind kind)
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
        if (
            !value.SequenceEqual("-0"u8)
            && long.TryParse(
                value,
                NumberStyles.AllowLeadingSign,
                CultureInfo.InvariantCulture,
                out var integer
            )
        )
            return integer;
        if (
            !value.SequenceEqual("-0"u8)
            && decimal.TryParse(
                value,
                NumberStyles.AllowLeadingSign | NumberStyles.AllowDecimalPoint,
                CultureInfo.InvariantCulture,
                out var number
            )
        )
            return number;
        return double.Parse(value, NumberStyles.Float, CultureInfo.InvariantCulture);
    }

    /// <summary>
    /// The rendered message plus the captured fields, in the key/value shape every structured
    /// provider recognizes. <see cref="ToString"/> returns the rendered message, for sinks that
    /// render state directly.
    /// </summary>
    internal sealed class HandoffState(
        string message,
        KeyValuePair<string, object?>[] fields,
        int captureFailures,
        int overflowHoleFields
    ) : IReadOnlyList<KeyValuePair<string, object?>>
    {
        public int CaptureFailures => captureFailures;
        public int OverflowHoleFields => overflowHoleFields;

        public KeyValuePair<string, object?> this[int index] => fields[index];

        public int Count => fields.Length;

        public IEnumerator<KeyValuePair<string, object?>> GetEnumerator() =>
            ((IEnumerable<KeyValuePair<string, object?>>)fields).GetEnumerator();

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator() =>
            GetEnumerator();

        public override string ToString() => message;
    }
}
