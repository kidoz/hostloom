using System.Buffers;
using System.Collections;
using System.Globalization;
using System.Runtime.CompilerServices;
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
    /// <summary>
    /// Captures the state and renders the message of one standard-path event, never throwing:
    /// caller code runs here (state enumeration, <c>ToString()</c>, the MEL formatter), and a log
    /// call must not fail because of it. State that throws part-way keeps the fields captured so
    /// far; a formatter that throws is replaced by the template rendered from those fields, or by
    /// <c>[MessageUnavailable]</c> when there is no template. Each failure is counted.
    /// </summary>
    public void CaptureEvent<TState>(
        LogEntry entry,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        bool renderFromFields;
        try
        {
            renderFromFields = CaptureState(entry, state);
        }
        catch (Exception)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentCapture);
            // The formatter would read the same state; render from what was captured instead.
            renderFromFields = true;
        }

        if (renderFromFields && entry.Template is { } template)
        {
            // Safe rendering for '@' events: the MEL formatter would stringify the hole through
            // the value's ToString(), and a record type's generated ToString prints every member
            // — including what [NotLogged] and [LogMasked] just excluded. Render the message
            // from the captured, protected representations instead, the way Serilog does.
            RenderTemplate(entry, template);
            return;
        }

        string message;
        try
        {
            // Rendered exactly once, through the caller's own formatter.
            message = formatter(state, exception);
        }
        catch (Exception)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentCapture);
            if (entry.Template is { } fallback)
            {
                RenderTemplate(entry, fallback);
            }
            else
            {
                entry.AppendLiteral("[MessageUnavailable]");
            }

            return;
        }

        entry.AppendLiteral(message);
    }

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
            // MEL puts the template last and serves it without touching the arguments, so read it
            // first: a state whose argument count does not match its template throws part-way
            // through the pairs, and the event keeps its template all the same.
            if (list.Count > 0 && list[^1] is { Key: "{OriginalFormat}", Value: string template })
            {
                entry.Template = template;
            }

            for (var i = 0; i < list.Count; i++)
            {
                destructured |= CapturePair(entry, list[i]);
            }

            CaptureRenderings(entry, list);
            return destructured;
        }

        if (state is IEnumerable<KeyValuePair<string, object?>> pairs)
        {
            foreach (var pair in pairs)
            {
                destructured |= CapturePair(entry, pair);
            }

            CaptureRenderings(entry, pairs);
        }

        return destructured;
    }

    /// <summary>
    /// CLEF <c>@r</c> for the standard path: every template token carrying a format
    /// (<c>{Amount:N2}</c>) contributes its value rendered with that format, in template order,
    /// the way Serilog's <c>CompactJsonFormatter</c> does. Tokens with only an alignment have
    /// none. Rendered in the invariant culture, like every other value this library writes.
    /// </summary>
    private void CaptureRenderings(LogEntry entry, IEnumerable<KeyValuePair<string, object?>> pairs)
    {
        if (entry.Template is not { } template || !template.Contains(':', StringComparison.Ordinal))
        {
            return;
        }

        var text = template.AsSpan();
        while (!text.IsEmpty)
        {
            var open = text.IndexOf('{');
            if (open < 0)
            {
                return;
            }

            if (open + 1 < text.Length && text[open + 1] == '{')
            {
                text = text[(open + 2)..];
                continue;
            }

            text = text[(open + 1)..];
            var close = text.IndexOf('}');
            if (close < 0)
            {
                return;
            }

            var token = text[..close];
            text = text[(close + 1)..];
            var colon = token.IndexOf(':');
            if (colon < 0)
            {
                continue;
            }

            var format = token[(colon + 1)..].ToString();
            var name = token[..colon];
            int? alignment = null;
            var comma = name.IndexOf(',');
            if (comma >= 0)
            {
                if (
                    int.TryParse(
                        name[(comma + 1)..],
                        NumberStyles.AllowLeadingSign,
                        CultureInfo.InvariantCulture,
                        out var width
                    )
                )
                {
                    alignment = width;
                }

                name = name[..comma];
            }

            if (!IsHoleName(name))
            {
                // Serilog's parser keeps such a token as literal text, so it has no rendering.
                continue;
            }

            string rendering;
            try
            {
                rendering =
                    !TryFindValue(pairs, name, out var value) ? string.Concat("{", token, "}")
                    : name[0] == '@' || value is IEnumerable and not string
                        ? CapturedText(entry, name)
                    : RenderToken(value, format, alignment);
            }
            catch (Exception)
            {
                // An invalid format or a throwing ToString() costs this rendering, never the
                // event; the token stands in for it, as for a value that is missing.
                rendering = string.Concat("{", token, "}");
            }

            entry.EnsureTemplateRenderings().Add(CapText(rendering, options.MaxTextFieldLength));
        }
    }

    /// <summary>
    /// Serilog's hole grammar: an optional <c>@</c> or <c>$</c>, then letters, digits, and
    /// underscores only. MEL captures any braced token; Serilog renders the rest as text.
    /// </summary>
    private static bool IsHoleName(ReadOnlySpan<char> name)
    {
        if (!name.IsEmpty && (name[0] == '@' || name[0] == '$'))
        {
            name = name[1..];
        }

        if (name.IsEmpty)
        {
            return false;
        }

        foreach (var c in name)
        {
            if (!char.IsLetterOrDigit(c) && c != '_')
            {
                return false;
            }
        }

        return true;
    }

    /// <summary>A destructured or structurally captured value renders as the JSON already
    /// captured for its field — under the masking policy, never through its own
    /// <c>ToString()</c>, which would print the members the policy excluded.</summary>
    private static string CapturedText(LogEntry entry, ReadOnlySpan<char> name)
    {
        if (name[0] is '@' or '$')
        {
            name = name[1..];
        }

        Span<byte> utf8 = stackalloc byte[512];
        var builder = new StringBuilder();
        var length = EncodeName(name, utf8);
        return length >= 0 && entry.TryAppendFieldValue(utf8[..length], 0, builder)
            ? builder.ToString()
            : string.Empty;
    }

    private static bool TryFindValue(
        IEnumerable<KeyValuePair<string, object?>> pairs,
        ReadOnlySpan<char> name,
        out object? value
    )
    {
        foreach (var pair in pairs)
        {
            if (name.SequenceEqual(pair.Key))
            {
                value = pair.Value;
                return true;
            }
        }

        value = null;
        return false;
    }

    /// <summary>Serilog's scalar rendering: <c>null</c>, a string quoted unless the format is
    /// <c>l</c>, a formattable value through its format, anything else through
    /// <c>ToString()</c>; then padded to the alignment.</summary>
    internal static string RenderToken(object? value, string format, int? alignment)
    {
        var text = value switch
        {
            null => "null",
            string s => format == "l"
                ? s
                : string.Concat("\"", s.Replace("\"", "\\\"", StringComparison.Ordinal), "\""),
            IFormattable formattable => formattable.ToString(format, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };

        return alignment switch
        {
            > 0 => text.PadLeft(alignment.Value),
            < 0 => text.PadRight(-alignment.Value),
            _ => text,
        };
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

        // A structurally captured collection renders the message from the captured fields too:
        // the MEL formatter would enumerate it a second time, and a lazy query would run twice.
        return CaptureValue(entry, name, pair.Value);
    }

    /// <summary>
    /// A hole without an operator. Scalars keep their JSON types; a collection or dictionary
    /// keeps its structure as Serilog captures it, with each element that is neither a scalar
    /// nor a collection written as its invariant <c>ToString()</c>; any other object becomes
    /// its invariant string.
    /// </summary>
    private bool CaptureValue(LogEntry entry, string name, object? value)
    {
        if (TryCaptureScalar(entry, name, value))
        {
            return false;
        }

        if (value is IEnumerable || (value is ITuple && value.GetType().IsValueType))
        {
            CaptureDestructured(entry, name, value, LogFieldSource.Hole, objectsAsText: true);
            return true;
        }

        CaptureStringified(entry, name, value);
        return false;
    }

    private void CaptureDestructured(
        LogEntry entry,
        string name,
        object? value,
        LogFieldSource source = LogFieldSource.Hole,
        bool objectsAsText = false
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
        var json = destructurer.Destructure(value!, remaining, objectsAsText);
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
            // Serilog.Extensions.Logging's scope shapes besides key/value pairs of object: any
            // dictionary with string keys, and a (string, value) tuple naming one property.
            var pairs = scope switch
            {
                IEnumerable<KeyValuePair<string, object?>> objectPairs => objectPairs,
                IDictionary dictionary => StringKeyedPairs(dictionary),
                ITuple { Length: 2 } tuple when tuple[0] is string key =>
                [
                    new KeyValuePair<string, object?>(key, tuple[1]),
                ],
                _ => null,
            };

            if (pairs is not null)
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

    private static IEnumerable<KeyValuePair<string, object?>> StringKeyedPairs(
        IDictionary dictionary
    )
    {
        foreach (DictionaryEntry item in dictionary)
        {
            if (item.Key is string key)
            {
                yield return new KeyValuePair<string, object?>(key, item.Value);
            }
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

    /// <summary>The one template grammar shared by message and scope rendering: <c>{{</c> and
    /// <c>}}</c> escapes, <c>@</c>/<c>$</c> operators stripped from the name, format and alignment
    /// specifiers ignored, and an unresolvable hole kept verbatim.</summary>
    private static void WalkTemplate<TTarget>(ReadOnlySpan<char> text, ref TTarget target)
        where TTarget : struct, ITemplateTarget
    {
        while (!text.IsEmpty)
        {
            var open = text.IndexOf('{');
            if (open < 0)
            {
                EmitText(text, ref target);
                return;
            }

            if (open + 1 < text.Length && text[open + 1] == '{')
            {
                EmitText(text[..(open + 1)], ref target);
                text = text[(open + 2)..];
                continue;
            }

            EmitText(text[..open], ref target);
            text = text[(open + 1)..];
            var close = text.IndexOf('}');
            if (close < 0)
            {
                target.Text("{");
                EmitText(text, ref target);
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

    /// <summary>Literal template text, with its <c>}}</c> escape written as <c>}</c>, as MEL and
    /// Serilog render it.</summary>
    private static void EmitText<TTarget>(ReadOnlySpan<char> text, ref TTarget target)
        where TTarget : struct, ITemplateTarget
    {
        while (true)
        {
            var escaped = text.IndexOf("}}", StringComparison.Ordinal);
            if (escaped < 0)
            {
                target.Text(text);
                return;
            }

            target.Text(text[..(escaped + 1)]);
            text = text[(escaped + 2)..];
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
            case byte[] bytes:
                entry.AddFieldText(name, Destructurer.ByteArrayText(bytes), source);
                return true;
            case ReadOnlyMemory<byte> memory:
                entry.AddFieldText(name, Destructurer.ByteArrayText(memory.Span), source);
                return true;
            case Memory<byte> memory:
                entry.AddFieldText(name, Destructurer.ByteArrayText(memory.Span), source);
                return true;
            case Enum:
                // The name, matching Serilog's scalar enum rendering, not the numeric value.
                entry.AddFieldText(name, value.ToString() ?? string.Empty, source);
                return true;
            default:
                return false;
        }
    }

    /// <summary>A value's own <c>ToString()</c> can throw; that costs this field, which carries
    /// the destructuring failure sentinel instead, never the event.</summary>
    private void CaptureStringified(
        LogEntry entry,
        string name,
        object? value,
        LogFieldSource source = LogFieldSource.Hole
    )
    {
        string text;
        try
        {
            text = ToInvariantText(value);
        }
        catch (Exception)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            text = "[DestructuringFailed]";
        }

        entry.AddFieldText(name, text, source);
    }

    public static string ToInvariantText(object? value) =>
        value switch
        {
            null => string.Empty,
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };
}
