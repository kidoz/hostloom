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
        // One destructuring byte budget per record, shared across all its '@' holes and, later,
        // across the values of its scopes.
        entry.DestructuringBudget = options.Destructuring.MaxEncodedBytesPerRecord;
        var destructured = false;

        if (state is IReadOnlyList<KeyValuePair<string, object?>> list)
        {
            for (var i = 0; i < list.Count; i++)
            {
                destructured |= CapturePair(entry, list[i]);
            }

            return destructured;
        }

        if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
        {
            foreach (var pair in pairs)
            {
                destructured |= CapturePair(entry, pair);
            }
        }

        return destructured;
    }

    private bool CapturePair(LogEntry entry, KeyValuePair<string, object?> pair)
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

            CaptureDestructured(entry, stripped, pair.Value);
            return true;
        }

        CaptureValue(entry, name, pair.Value);
        return false;
    }

    private void CaptureDestructured(
        LogEntry entry,
        string name,
        object? value,
        LogFieldSource source = LogFieldSource.Hole
    )
    {
        if (TryCaptureScalar(entry, name, value, source))
        {
            return;
        }

        var remaining = entry.DestructuringBudget;
        if (remaining <= 0)
        {
            // The record's destructuring budget is spent: an explicit sentinel, never silence.
            entry.AddFieldText(name, "…", source);
            return;
        }

        // The span points into thread-local scratch; AddFieldJson copies it out immediately.
        var json = destructurer.Destructure(value!, remaining);
        if (json.Length > remaining)
        {
            // Not even the cut fragment fits what is left, so the record's destructured bytes
            // can never exceed the budget: the hole degrades like one past it, spending nothing.
            entry.AddFieldText(name, "…", source);
            return;
        }

        // Clamped at zero: a negative budget means "not yet initialized", never "overspent".
        entry.DestructuringBudget = Math.Max(0, remaining - json.Length);
        entry.AddFieldJson(name, json, source);
    }

    /// <summary>
    /// One active scope, outermost first. Structured pairs flatten into Scope-rank fields, so an
    /// inner scope's value beats an outer one and any event hole beats them both. A scope that
    /// carried a message template additionally contributes its rendered text to the
    /// <c>Scope</c> array, as does every non-structured scope — nothing is silently dropped.
    /// No caller-side formatter renders a scope, so a non-scalar scope value is destructured
    /// under the record's protection policy and budget whether or not its hole carries
    /// <c>@</c>; only <c>$</c> forces <c>ToString()</c>. The templated text is rendered from
    /// those captured representations, never from the scope object's own <c>ToString()</c>.
    /// </summary>
    public void CaptureScope(object? scope, LogEntry entry)
    {
        try
        {
            if (scope is IEnumerable<KeyValuePair<string, object?>> pairs)
            {
                if (entry.DestructuringBudget < 0)
                {
                    // The interpolated fast path captured no state, so the scope walk is this
                    // record's first destructuring consumer.
                    entry.DestructuringBudget = options.Destructuring.MaxEncodedBytesPerRecord;
                }

                string? template = null;
                var firstField = entry.FieldCount;
                foreach (var pair in pairs)
                {
                    if (pair.Key == "{OriginalFormat}")
                    {
                        template = pair.Value as string;
                        continue;
                    }

                    CaptureScopePair(entry, pair);
                }

                if (template is not null)
                {
                    entry.EnsureScopeTexts().Add(RenderScopeText(entry, template, firstField));
                }

                return;
            }

            entry
                .EnsureScopeTexts()
                .Add(CapText(ToInvariantText(scope), options.MaxTextFieldLength));
        }
        catch (Exception)
        {
            // One unreadable scope must not cost the event or the scopes around it.
            metrics?.RecordFailure(LoggingMetrics.ComponentScope);
        }
    }

    private void CaptureScopePair(LogEntry entry, KeyValuePair<string, object?> pair)
    {
        var name = pair.Key;
        if (name.Length > 0 && (name[0] == '@' || name[0] == '$'))
        {
            var stripped = name[1..];
            if (name[0] == '$')
            {
                CaptureStringified(entry, stripped, pair.Value, LogFieldSource.Scope);
                return;
            }

            CaptureDestructured(entry, stripped, pair.Value, LogFieldSource.Scope);
            return;
        }

        // A plain hole too: stringifying here would print what the policy excluded.
        CaptureDestructured(entry, name, pair.Value, LogFieldSource.Scope);
    }

    private string RenderScopeText(LogEntry entry, string template, int firstField)
    {
        var target = new ScopeTextTarget(
            entry,
            firstField,
            new StringBuilder(Math.Min(template.Length + 64, 1024))
        );
        WalkTemplate(template, ref target);
        return CapText(target.Builder.ToString(), options.MaxTextFieldLength);
    }

    /// <summary>
    /// Bounds a producer-built text at <paramref name="maxBytes"/> of UTF-8 on a character
    /// boundary, closing it with "…" — the same sentinel the entry buffers use.
    /// </summary>
    internal static string CapText(string text, int maxBytes)
    {
        // A UTF-16 unit never encodes to more than three bytes, so short text needs no count.
        if (text.Length <= maxBytes / 3 || Encoding.UTF8.GetByteCount(text) <= maxBytes)
        {
            return text;
        }

        var bytes = 0;
        var cut = 0;
        foreach (var rune in text.EnumerateRunes())
        {
            if (bytes + rune.Utf8SequenceLength > maxBytes)
            {
                break;
            }

            bytes += rune.Utf8SequenceLength;
            cut += rune.Utf16SequenceLength;
        }

        return string.Concat(text.AsSpan(0, cut), "…");
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
        var target = new MessageTarget(entry);
        WalkTemplate(template, ref target);
    }

    /// <summary>Where a template walk lands: the record's message, or a scope's text.</summary>
    private interface ITemplateTarget
    {
        void Text(ReadOnlySpan<char> text);

        /// <summary>Renders the hole; false leaves the literal <c>{token}</c> in place.</summary>
        bool Hole(ReadOnlySpan<char> name);
    }

    /// <summary>The one template grammar shared by message and scope rendering: <c>{{</c>
    /// escapes, <c>@</c>/<c>$</c> operators stripped from the name, format and alignment
    /// specifiers ignored, and an unresolvable hole kept verbatim.</summary>
    private static void WalkTemplate<TTarget>(ReadOnlySpan<char> text, ref TTarget target)
        where TTarget : struct, ITemplateTarget
    {
        while (!text.IsEmpty)
        {
            var open = text.IndexOf('{');
            if (open < 0)
            {
                target.Text(text);
                return;
            }

            if (open + 1 < text.Length && text[open + 1] == '{')
            {
                target.Text(text[..(open + 1)]);
                text = text[(open + 2)..];
                continue;
            }

            target.Text(text[..open]);
            text = text[(open + 1)..];
            var close = text.IndexOf('}');
            if (close < 0)
            {
                target.Text("{");
                target.Text(text);
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

            if (!target.Hole(name))
            {
                target.Text("{");
                target.Text(token);
                target.Text("}");
            }
        }
    }

    /// <summary>Encodes a hole name for field lookup; -1 rejects names no field can carry.</summary>
    private static int EncodeName(ReadOnlySpan<char> name, Span<byte> utf8) =>
        name.Length is 0 or > 128 ? -1 : Encoding.UTF8.GetBytes(name, utf8);

    private readonly struct MessageTarget(LogEntry entry) : ITemplateTarget
    {
        public void Text(ReadOnlySpan<char> text) => entry.AppendText(text, null);

        public bool Hole(ReadOnlySpan<char> name)
        {
            Span<byte> utf8 = stackalloc byte[512];
            var length = EncodeName(name, utf8);
            return length >= 0 && entry.AppendFieldValueToMessage(utf8[..length]);
        }
    }

    private readonly struct ScopeTextTarget(LogEntry entry, int firstField, StringBuilder builder)
        : ITemplateTarget
    {
        public StringBuilder Builder => builder;

        public void Text(ReadOnlySpan<char> text) => builder.Append(text);

        public bool Hole(ReadOnlySpan<char> name)
        {
            Span<byte> utf8 = stackalloc byte[512];
            var length = EncodeName(name, utf8);
            return length >= 0 && entry.TryAppendFieldValue(utf8[..length], firstField, builder);
        }
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
