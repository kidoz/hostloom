using System.Runtime.CompilerServices;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

/// <summary>
/// Renders an interpolated log message straight to UTF-8, capturing each hole as a named field.
/// </summary>
/// <remarks>
/// <para>
/// The constructor reports <c>shouldAppend: false</c> when the level is disabled, and the compiler
/// then skips evaluating the interpolation holes entirely. That closes the trap every logging
/// library warns about — an argument like <c>Expensive()</c> still running at a disabled level.
/// </para>
/// <para>
/// The concrete <c>AppendFormatted</c> overloads are not redundant. Overload resolution happens at
/// compile time, so <c>$"id {userId}"</c> binds to the <see cref="int"/> overload and reaches a
/// constrained generic call — no boxing. A single <c>AppendFormatted&lt;T&gt;</c> that tested
/// <c>value is IUtf8SpanFormattable</c> would box every value type on the way in.
/// </para>
/// </remarks>
[InterpolatedStringHandler]
public ref struct LogMessageHandler
{
    internal LogEntry? Entry;
    internal LogFastCapture? Wrapped;

    public LogMessageHandler(
        int literalLength,
        int formattedCount,
        ILogger logger,
        LogLevel level,
        out bool shouldAppend
    )
    {
        ArgumentNullException.ThrowIfNull(logger);
        if (!logger.IsEnabled(level))
        {
            Entry = null;
            shouldAppend = false;
            return;
        }

        if (logger is HostLoomLogger own)
        {
            Entry = LogEntryPool.Rent();
            Entry.Level = level;
            Entry.ApplyCaps(own.Options);
        }
        else
        {
            Wrapped = new LogFastCapture();
        }

        shouldAppend = true;
    }

    public void AppendLiteral(string value) => AppendFormatted(value.AsSpan(), name: null);

    public void AppendFormatted(
        string? value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => AppendFormatted(value.AsSpan(), name: name);

    public void AppendFormatted(
        ReadOnlySpan<char> value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    )
    {
        if (Wrapped is not null)
            foreach (var entry in Wrapped.Entries)
                entry.AppendText(value, name);
        Entry?.AppendText(value, name);
    }

    public void AppendFormatted(
        int value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => AppendValue(value, format, name, LogFieldKind.Number);

    public void AppendFormatted(
        long value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => AppendValue(value, format, name, LogFieldKind.Number);

    /// <summary>NaN and the infinities are not JSON numbers, so they degrade to text fields.</summary>
    public void AppendFormatted(
        double value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) =>
        AppendValue(
            value,
            format,
            name,
            double.IsFinite(value) ? LogFieldKind.Number : LogFieldKind.Text
        );

    public void AppendFormatted(
        decimal value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => AppendValue(value, format, name, LogFieldKind.Number);

    public void AppendFormatted(
        bool value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    )
    {
        if (Wrapped is not null)
            foreach (var entry in Wrapped.Entries)
                entry.AppendBoolean(value, name);
        Entry?.AppendBoolean(value, name);
    }

    public void AppendFormatted(
        Guid value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => AppendValue(value, format, name, LogFieldKind.Text);

    /// <summary>Defaults to ISO-8601 ("O"), the canonical timestamp shape for JSON consumers.</summary>
    public void AppendFormatted(
        DateTimeOffset value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => AppendValue(value, format, name, LogFieldKind.Text, "O");

    public void AppendFormatted(
        TimeSpan value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => AppendValue(value, format, name, LogFieldKind.Text);

    /// <summary>Fallback for types with no UTF-8 formatter. Allocates, and is meant to.</summary>
    public void AppendFormatted<T>(
        T value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    )
    {
        if (Entry is null && Wrapped is null)
        {
            return;
        }

        if (value is IUtf8SpanFormattable)
        {
            AppendBoxedFormattable(value, format, name);
            return;
        }

        string? text;
        try
        {
            text = value is IFormattable formattable
                ? formattable.ToString(format, System.Globalization.CultureInfo.InvariantCulture)
                : value?.ToString();
        }
        catch (Exception)
        {
            if (Wrapped is not null)
                foreach (var entry in Wrapped.Entries)
                    entry.AppendCaptureFailure(name);
            Entry?.AppendCaptureFailure(name);
            return;
        }
        AppendFormatted(text.AsSpan(), name: name);
    }

    private readonly void AppendValue<T>(
        T value,
        string? format,
        string? name,
        LogFieldKind kind,
        string? canonicalFormat = null
    )
        where T : IUtf8SpanFormattable
    {
        if (Wrapped is not null)
            foreach (var entry in Wrapped.Entries)
                entry.AppendFormattable(value, format, name, kind, canonicalFormat);
        Entry?.AppendFormattable(value, format, name, kind, canonicalFormat);
    }

    private readonly void AppendBoxedFormattable<T>(T value, string? format, string? name)
    {
        var kind = value switch
        {
            byte
            or sbyte
            or short
            or ushort
            or int
            or uint
            or long
            or ulong
            or Int128
            or UInt128
            or nint
            or nuint
            or decimal => LogFieldKind.Number,
            float number when float.IsFinite(number) => LogFieldKind.Number,
            double number when double.IsFinite(number) => LogFieldKind.Number,
            Half number when Half.IsFinite(number) => LogFieldKind.Number,
            _ => LogFieldKind.Text,
        };
        IUtf8SpanFormattable formatted = (IUtf8SpanFormattable)value!;
        if (Wrapped is { Entries.Length: > 1 } && kind == LogFieldKind.Text)
        {
            formatted = new CapturedUtf8Value(formatted, format, Wrapped.FormatBudget);
        }
        AppendValue(formatted, format, name, kind);
    }
}
