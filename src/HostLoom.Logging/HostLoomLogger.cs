using System.Buffers;
using System.Diagnostics;
using Microsoft.Extensions.Logging;

namespace HostLoom.Logging;

internal sealed class HostLoomLogger(
    string category,
    LogPipeline pipeline,
    HostLoomLoggerOptions options,
    HostLoomLoggerProvider provider
) : ILogger
{
    public IDisposable? BeginScope<TState>(TState state)
        where TState : notnull => provider.ScopeProvider.Push(state);

    public bool IsEnabled(LogLevel logLevel) => logLevel != LogLevel.None;

    /// <summary>
    /// The Microsoft.Extensions.Logging entry point, used by every library that is not calling the
    /// interpolated fast path. It boxes <paramref name="state"/> and builds a string, because the
    /// interface signature requires both — a cost the fast path exists to avoid. Structured state
    /// (what <c>FormattedLogValues</c>, <c>LoggerMessage.Define</c>, and source-generated logger
    /// methods pass) is captured pair by pair as typed fields, so template holes stay queryable.
    /// </summary>
    public void Log<TState>(
        LogLevel logLevel,
        EventId eventId,
        TState state,
        Exception? exception,
        Func<TState, Exception?, string> formatter
    )
    {
        if (!IsEnabled(logLevel))
        {
            return;
        }

        ArgumentNullException.ThrowIfNull(formatter);
        var entry = LogEntryPool.Rent();
        entry.Level = logLevel;
        var destructured = CaptureState(entry, state);
        if (destructured && entry.Template is { } template)
        {
            // Safe rendering for '@' events: the MEL formatter would stringify the hole through
            // the value's ToString(), and a record type's generated ToString prints every member
            // — including what [NotLogged] and [LogMasked] just excluded. Render the message
            // from the captured, protected representations instead, the way Serilog does.
            RenderTemplate(entry, template);
        }
        else
        {
            // Rendered exactly once, through the caller's own formatter.
            entry.AppendLiteral(formatter(state, exception));
        }

        Emit(entry, eventId, exception);
    }

    /// <summary>
    /// One active scope, outermost first. Structured pairs flatten into Scope-rank fields, so an
    /// inner scope's value beats an outer one and any event hole beats them both. A scope that
    /// carried a message template additionally contributes its rendered text to the
    /// <c>Scope</c> array, as does every non-structured scope — nothing is silently dropped.
    /// </summary>
    private void CaptureScope(object? scope, LogEntry entry)
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
            pipeline.Metrics.RecordFailure(LoggingMetrics.ComponentScope);
        }
    }

    private static void AddScopeArray(LogEntry entry, List<string> scopeTexts)
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

    private static string ToInvariantText(object? value) =>
        value switch
        {
            null => string.Empty,
            IFormattable formattable => formattable.ToString(
                null,
                System.Globalization.CultureInfo.InvariantCulture
            ),
            _ => value.ToString() ?? string.Empty,
        };

    private bool CaptureState<TState>(LogEntry entry, TState state)
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

    private bool CapturePair(
        LogEntry entry,
        KeyValuePair<string, object?> pair,
        ref int remaining
    )
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

    /// <summary>
    /// Renders the message from the template and the captured field representations. Holes
    /// resolve to canonical tokens (ISO dates, lowercase booleans, destructured JSON), so the
    /// text can differ cosmetically from MEL's rendering — the price of never echoing what the
    /// protection policy excluded. Format and alignment specifiers are ignored on this path.
    /// </summary>
    private static void RenderTemplate(LogEntry entry, string template)
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
        var length = System.Text.Encoding.UTF8.GetBytes(name, utf8);
        return entry.AppendFieldValueToMessage(utf8[..length]);
    }

    private void CaptureDestructured(
        LogEntry entry,
        string name,
        object? value,
        ref int remaining
    )
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

        var buffer = new ArrayBufferWriter<byte>(256);
        pipeline.Destructurer.Destructure(value!, buffer, remaining);
        remaining -= buffer.WrittenCount;
        entry.AddFieldJson(name, buffer.WrittenSpan);
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

    /// <summary>A non-scalar (or '$'-forced) value: the invariant string representation. Full
    /// object destructuring for '@' holes is the destructurer's job, not this path's.</summary>
    private static void CaptureStringified(
        LogEntry entry,
        string name,
        object? value,
        LogFieldSource source = LogFieldSource.Hole
    ) => entry.AddFieldText(name, ToInvariantText(value), source);

    /// <summary>Fast path: the entry is already rendered, so only the ambient metadata is added.</summary>
    internal void Emit(LogEntry entry, EventId eventId, Exception? exception)
    {
        entry.Category = category;
        entry.EventId = eventId;
        entry.Exception = exception;
        // The wall clock, not a Stopwatch anchor: an anchor set at startup never sees an NTP
        // correction, and over weeks of uptime the drift breaks cross-service correlation.
        entry.Timestamp = options.TimeProvider.GetUtcNow();
        entry.ThreadId = Environment.CurrentManagedThreadId;

        // Captured here, never on the writer thread: Activity.Current is ambient to this thread, so
        // reading it after the hand-off would attach the wrong trace, or none at all.
        if (options.CaptureActivity && Activity.Current is { } activity)
        {
            entry.TraceId = activity.TraceId;
            entry.SpanId = activity.SpanId;
            entry.HasActivity = true;
        }

        // Scopes are snapshotted here, on the producer thread, for the same reason as
        // Activity.Current: the ambient scope chain is gone once the entry crosses the queue.
        try
        {
            provider.ScopeProvider.ForEachScope(
                static (scope, state) => state.Logger.CaptureScope(scope, state.Entry),
                (Logger: this, Entry: entry)
            );
        }
        catch (Exception)
        {
            // A scope provider that throws mid-walk costs the remaining scopes, never the event.
            pipeline.Metrics.RecordFailure(LoggingMetrics.ComponentScope);
        }

        if (entry.ScopeTexts is { Count: > 0 } scopeTexts)
        {
            AddScopeArray(entry, scopeTexts);
        }

        // Enrichers run here for the same reason: AsyncLocal context is gone after the hand-off.
        var enrichers = options.Enrichers;
        for (var i = 0; i < enrichers.Count; i++)
        {
            var writer = new LogEntryWriter(entry);
            try
            {
                enrichers[i].Enrich(ref writer);
            }
            catch (Exception)
            {
                // One broken enricher costs neither the event, the remaining enrichers, nor the
                // caller; the failure is counted instead of thrown.
                pipeline.Metrics.RecordFailure(LoggingMetrics.ComponentEnricher);
            }
        }

        var statics = pipeline.StaticFields;
        for (var i = 0; i < statics.Length; i++)
        {
            entry.AddFieldUtf8Text(statics[i].Name, statics[i].Value, LogFieldSource.Static);
        }

        pipeline.Enqueue(entry);
    }
}
