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

        Entry = LogEntryPool.Rent();
        Entry.Level = level;
        shouldAppend = true;
    }

    public void AppendLiteral(string value) => Entry?.AppendLiteral(value);

    public void AppendFormatted(
        string? value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => Entry?.AppendText(value ?? string.Empty, name);

    public void AppendFormatted(
        ReadOnlySpan<char> value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => Entry?.AppendText(value, name);

    public void AppendFormatted(
        int value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => Entry?.AppendFormattable(value, format, name, LogFieldKind.Number);

    public void AppendFormatted(
        long value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => Entry?.AppendFormattable(value, format, name, LogFieldKind.Number);

    /// <summary>NaN and the infinities are not JSON numbers, so they degrade to text fields.</summary>
    public void AppendFormatted(
        double value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) =>
        Entry?.AppendFormattable(
            value,
            format,
            name,
            double.IsFinite(value) ? LogFieldKind.Number : LogFieldKind.Text
        );

    public void AppendFormatted(
        decimal value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => Entry?.AppendFormattable(value, format, name, LogFieldKind.Number);

    public void AppendFormatted(
        bool value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => Entry?.AppendBoolean(value, name);

    public void AppendFormatted(
        Guid value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => Entry?.AppendFormattable(value, format, name, LogFieldKind.Text);

    /// <summary>Defaults to ISO-8601 ("O"), the canonical timestamp shape for JSON consumers.</summary>
    public void AppendFormatted(
        DateTimeOffset value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => Entry?.AppendFormattable(value, format, name, LogFieldKind.Text, "O");

    public void AppendFormatted(
        TimeSpan value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    ) => Entry?.AppendFormattable(value, format, name, LogFieldKind.Text);

    /// <summary>Fallback for types with no UTF-8 formatter. Allocates, and is meant to.</summary>
    public void AppendFormatted<T>(
        T value,
        string? format = null,
        [CallerArgumentExpression(nameof(value))] string? name = null
    )
    {
        if (Entry is null)
        {
            return;
        }

        if (value is IUtf8SpanFormattable)
        {
            AppendBoxedFormattable(value, format, name);
            return;
        }

        var text = value is IFormattable formattable
            ? formattable.ToString(format, System.Globalization.CultureInfo.InvariantCulture)
            : value?.ToString();
        Entry.AppendText(text ?? string.Empty, name);
    }

    private readonly void AppendBoxedFormattable<T>(T value, string? format, string? name) =>
        // The static type is unknown here, so the safe kind is text: a numeric token would need
        // proof the rendering is valid JSON, and only the concrete overloads can promise that.
        Entry!.AppendFormattable((IUtf8SpanFormattable)value!, format, name, LogFieldKind.Text);
}
