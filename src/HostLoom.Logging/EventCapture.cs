using System.Buffers;
using System.Globalization;
using System.Text;

namespace HostLoom.Logging;

/// <summary>
/// The shared producer-side capture engine: structured state, template operators, destructuring,
/// scope flattening, and safe message rendering. Both the hosted logger and the pre-DI bootstrap
/// logger write through this class, so the two emit identical events.
/// </summary>
internal sealed class EventCapture(
    HostLoomLoggerOptions options,
    Destructurer destructurer,
    LoggingMetrics? metrics
)
{
    /// <summary>Captures MEL structured state as typed fields; returns whether any '@' hole was
    /// destructured, which decides whether the message must be safe-rendered.</summary>
    public bool CaptureState<TState>(LogEntry entry, TState state)
    {
        // One destructuring byte budget per record, shared across all its '@' holes.
        var remaining = options.Destructuring.MaxEncodedBytesPerRecord;
        var destructured = false;

        if (state is IReadOnlyList<KeyValuePair<string, object?>> list)
        {
            for (var i = 0; i < list.Count; i++)
            {
                destructured |= CapturePair(entry, list[i], ref remaining);
            }

            return destructured;
        }

        if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
        {
            foreach (var pair in pairs)
            {
                destructured |= CapturePair(entry, pair, ref remaining);
            }
        }

        return destructured;
    }

    private bool CapturePair(LogEntry entry, KeyValuePair<string, object?> pair, ref int remaining)
    {
        var name = pair.Key;
        if (name == "{OriginalFormat}")
        {
            // The template is metadata, not a field: stored for template-aware formatters.
            entry.Template = pair.Value as string;
            return false;
        }

        if (name.Length > 0 && (name[0] == '@' || name[0] == '$'))
        {
            // Serilog-compatible template operators: both strip their prefix from the emitted
            // name. '$' forces the invariant string; '@' destructures a non-scalar into nested
            // JSON while a scalar keeps its typed value.
            var stripped = name[1..];
            if (name[0] == '$')
            {
                CaptureStringified(entry, stripped, pair.Value);
                return false;
            }

            CaptureDestructured(entry, stripped, pair.Value, ref remaining);
            return true;
        }

        CaptureValue(entry, name, pair.Value);
        return false;
    }

    private void CaptureDestructured(LogEntry entry, string name, object? value, ref int remaining)
    {
        if (TryCaptureScalar(entry, name, value))
        {
            return;
        }

        if (remaining <= 0)
        {
            // The record's destructuring budget is spent: an explicit sentinel, never silence.
            entry.AddFieldText(name, "…");
            return;
        }

        // The span points into thread-local scratch; AddFieldJson copies it out immediately.
        var json = destructurer.Destructure(value!, remaining);
        remaining -= json.Length;
        entry.AddFieldJson(name, json);
    }

    /// <summary>
    /// One active scope, outermost first. Structured pairs flatten into Scope-rank fields, so an
    /// inner scope's value beats an outer one and any event hole beats them both. A scope that
    /// carried a message template additionally contributes its rendered text to the
    /// <c>Scope</c> array, as does every non-structured scope — nothing is silently dropped.
    /// </summary>
    public void CaptureScope(object? scope, LogEntry entry)
    {
        try
        {
            if (scope is IEnumerable<KeyValuePair<string, object?>> pairs)
            {
                var templated = false;
                foreach (var pair in pairs)
                {
                    if (pair.Key == "{OriginalFormat}")
                    {
                        templated = true;
                        continue;
                    }

                    var name = pair.Key;
                    if (name.Length > 0 && (name[0] == '@' || name[0] == '$'))
                    {
                        name = name[1..];
                    }

                    CaptureValue(entry, name, pair.Value, LogFieldSource.Scope);
                }

                if (templated)
                {
                    entry.EnsureScopeTexts().Add(scope.ToString() ?? string.Empty);
                }

                return;
            }

            entry.EnsureScopeTexts().Add(ToInvariantText(scope));
        }
        catch (Exception)
        {
            // One unreadable scope must not cost the event or the scopes around it.
            metrics?.RecordFailure(LoggingMetrics.ComponentScope);
        }
    }

    public static void AddScopeArray(LogEntry entry, List<string> scopeTexts)
    {
        var buffer = new ArrayBufferWriter<byte>(128);
        using (var writer = new System.Text.Json.Utf8JsonWriter(buffer))
        {
            writer.WriteStartArray();
            for (var i = 0; i < scopeTexts.Count; i++)
            {
                writer.WriteStringValue(scopeTexts[i]);
            }

            writer.WriteEndArray();
        }

        entry.AddFieldJson("Scope", buffer.WrittenSpan, LogFieldSource.Scope);
    }

    /// <summary>
    /// Renders the message from the template and the captured field representations. Holes
    /// resolve to canonical tokens (ISO dates, lowercase booleans, destructured JSON), so the
    /// text can differ cosmetically from MEL's rendering — the price of never echoing what the
    /// protection policy excluded. Format and alignment specifiers are ignored on this path.
    /// </summary>
    public static void RenderTemplate(LogEntry entry, string template)
    {
        var text = template.AsSpan();
        while (!text.IsEmpty)
        {
            var open = text.IndexOf('{');
            if (open < 0)
            {
                entry.AppendText(text, null);
                return;
            }

            if (open + 1 < text.Length && text[open + 1] == '{')
            {
                entry.AppendText(text[..(open + 1)], null);
                text = text[(open + 2)..];
                continue;
            }

            entry.AppendText(text[..open], null);
            text = text[(open + 1)..];
            var close = text.IndexOf('}');
            if (close < 0)
            {
                entry.AppendText("{", null);
                entry.AppendText(text, null);
                return;
            }

            var token = text[..close];
            text = text[(close + 1)..];
            var name = token;
            var separator = name.IndexOfAny(',', ':');
            if (separator >= 0)
            {
                name = name[..separator];
            }

            if (name.Length > 0 && (name[0] == '@' || name[0] == '$'))
            {
                name = name[1..];
            }

            if (!AppendField(entry, name))
            {
                entry.AppendText("{", null);
                entry.AppendText(token, null);
                entry.AppendText("}", null);
            }
        }
    }

    private static bool AppendField(LogEntry entry, ReadOnlySpan<char> name)
    {
        if (name.Length is 0 or > 128)
        {
            return false;
        }

        Span<byte> utf8 = stackalloc byte[512];
        var length = Encoding.UTF8.GetBytes(name, utf8);
        return entry.AppendFieldValueToMessage(utf8[..length]);
    }

    private static void CaptureValue(
        LogEntry entry,
        string name,
        object? value,
        LogFieldSource source = LogFieldSource.Hole
    )
    {
        if (!TryCaptureScalar(entry, name, value, source))
        {
            CaptureStringified(entry, name, value, source);
        }
    }

    private static bool TryCaptureScalar(
        LogEntry entry,
        string name,
        object? value,
        LogFieldSource source = LogFieldSource.Hole
    )
    {
        switch (value)
        {
            case null:
                entry.AddFieldNull(name, source);
                return true;
            case string text:
                entry.AddFieldText(name, text, source);
                return true;
            case bool flag:
                entry.AddFieldBoolean(name, flag, source);
                return true;
            case int number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number, null, source);
                return true;
            case long number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number, null, source);
                return true;
            case double number:
                entry.AddFieldFormattable(
                    name,
                    number,
                    double.IsFinite(number) ? LogFieldKind.Number : LogFieldKind.Text,
                    null,
                    source
                );
                return true;
            case float number:
                entry.AddFieldFormattable(
                    name,
                    number,
                    float.IsFinite(number) ? LogFieldKind.Number : LogFieldKind.Text,
                    null,
                    source
                );
                return true;
            case decimal number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number, null, source);
                return true;
            case short number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number, null, source);
                return true;
            case ushort number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number, null, source);
                return true;
            case byte number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number, null, source);
                return true;
            case sbyte number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number, null, source);
                return true;
            case uint number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number, null, source);
                return true;
            case ulong number:
                entry.AddFieldFormattable(name, number, LogFieldKind.Number, null, source);
                return true;
            case Guid id:
                entry.AddFieldFormattable(name, id, LogFieldKind.Text, null, source);
                return true;
            case DateTimeOffset when1:
                entry.AddFieldFormattable(name, when1, LogFieldKind.Text, "O", source);
                return true;
            case DateTime when1:
                entry.AddFieldFormattable(name, when1, LogFieldKind.Text, "O", source);
                return true;
            case TimeSpan duration:
                entry.AddFieldFormattable(name, duration, LogFieldKind.Text, null, source);
                return true;
            case DateOnly day:
                entry.AddFieldFormattable(name, day, LogFieldKind.Text, "O", source);
                return true;
            case TimeOnly time:
                entry.AddFieldFormattable(name, time, LogFieldKind.Text, "O", source);
                return true;
            case char letter:
                entry.AddFieldText(name, new ReadOnlySpan<char>(in letter), source);
                return true;
            case Enum:
                // The name, matching Serilog's scalar enum rendering, not the numeric value.
                entry.AddFieldText(name, value.ToString() ?? string.Empty, source);
                return true;
            default:
                return false;
        }
    }

    private static void CaptureStringified(
        LogEntry entry,
        string name,
        object? value,
        LogFieldSource source = LogFieldSource.Hole
    ) => entry.AddFieldText(name, ToInvariantText(value), source);

    public static string ToInvariantText(object? value) =>
        value switch
        {
            null => string.Empty,
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };
}
