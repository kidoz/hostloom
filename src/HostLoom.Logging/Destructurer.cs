using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;
using System.Globalization;
using System.Reflection;
using System.Text.Json;

namespace HostLoom.Logging;

/// <summary>
/// Serializes one <c>{@...}</c> hole value into a complete, valid JSON fragment on the producer
/// thread, so the object is snapshotted at capture time. Bounded everywhere: depth, collection
/// items, object members, string length, and encoded bytes all cap with explicit non-sensitive
/// sentinels; cycles cut with <c>"[Cycle]"</c>. Protection is fail-closed: exclusion and masking
/// decisions come from a cached per-type plan applied before a member is ever read, an excluded
/// member simply does not exist to this walker, and any getter or serializer failure emits
/// <c>"[DestructuringFailed]"</c> — never the value's <c>ToString()</c>.
/// </summary>
internal sealed class Destructurer(DestructuringOptions options, LoggingMetrics metrics)
{
    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Indented = false,
        SkipValidation = true,
    };

    private readonly ConcurrentDictionary<Type, TypePlan> _plans = new();

    public void Destructure(object value, ArrayBufferWriter<byte> buffer, int byteBudget)
    {
        try
        {
            using var writer = new Utf8JsonWriter(buffer, WriterOptions);
            var walk = new Walk(new object?[options.MaxDepth], byteBudget);
            WriteValue(writer, value, 0, walk);
            writer.Flush();
        }
        catch (Exception)
        {
            // The writer's using-dispose already flushed whatever partial output existed; throw
            // it away and emit the sentinel so the fragment is always valid JSON.
            metrics.RecordFailure(LoggingMetrics.ComponentDestructurer);
            buffer.ResetWrittenCount();
            buffer.Write("\"[DestructuringFailed]\""u8);
        }
    }

    private sealed record Walk(object?[] Ancestors, int ByteLimit)
    {
        public bool OverBudget(Utf8JsonWriter writer) =>
            writer.BytesCommitted + writer.BytesPending >= ByteLimit;
    }

    private void WriteValue(Utf8JsonWriter writer, object? value, int depth, Walk walk)
    {
        if (TryWriteScalar(writer, value))
        {
            return;
        }

        if (depth >= options.MaxDepth)
        {
            writer.WriteStringValue("…");
            return;
        }

        for (var i = 0; i < depth; i++)
        {
            if (ReferenceEquals(walk.Ancestors[i], value))
            {
                writer.WriteStringValue("[Cycle]");
                return;
            }
        }

        walk.Ancestors[depth] = value;
        switch (value)
        {
            case IDictionary dictionary:
                WriteDictionary(writer, dictionary, depth, walk);
                break;
            case IEnumerable sequence:
                WriteSequence(writer, sequence, depth, walk);
                break;
            default:
                WriteObject(writer, value!, depth, walk);
                break;
        }

        walk.Ancestors[depth] = null;
    }

    /// <summary>The deterministic scalar table, mirroring the typed capture path: numbers stay
    /// numbers, non-finite floats and date/time/Guid/enum values are strings, byte arrays are
    /// Base64, and every string is subject to the length cap.</summary>
    private bool TryWriteScalar(Utf8JsonWriter writer, object? value)
    {
        switch (value)
        {
            case null:
                writer.WriteNullValue();
                return true;
            case string text:
                WriteCappedString(writer, text);
                return true;
            case bool flag:
                writer.WriteBooleanValue(flag);
                return true;
            case int number:
                writer.WriteNumberValue(number);
                return true;
            case long number:
                writer.WriteNumberValue(number);
                return true;
            case double number:
                if (double.IsFinite(number))
                {
                    writer.WriteNumberValue(number);
                }
                else
                {
                    writer.WriteStringValue(number.ToString(CultureInfo.InvariantCulture));
                }

                return true;
            case float number:
                if (float.IsFinite(number))
                {
                    writer.WriteNumberValue(number);
                }
                else
                {
                    writer.WriteStringValue(number.ToString(CultureInfo.InvariantCulture));
                }

                return true;
            case decimal number:
                writer.WriteNumberValue(number);
                return true;
            case short number:
                writer.WriteNumberValue(number);
                return true;
            case ushort number:
                writer.WriteNumberValue(number);
                return true;
            case byte number:
                writer.WriteNumberValue(number);
                return true;
            case sbyte number:
                writer.WriteNumberValue(number);
                return true;
            case uint number:
                writer.WriteNumberValue(number);
                return true;
            case ulong number:
                writer.WriteNumberValue(number);
                return true;
            case Guid id:
                writer.WriteStringValue(id);
                return true;
            case DateTimeOffset when1:
                writer.WriteStringValue(when1.ToString("O", CultureInfo.InvariantCulture));
                return true;
            case DateTime when1:
                writer.WriteStringValue(when1.ToString("O", CultureInfo.InvariantCulture));
                return true;
            case TimeSpan duration:
                writer.WriteStringValue(duration.ToString("c", CultureInfo.InvariantCulture));
                return true;
            case DateOnly day:
                writer.WriteStringValue(day.ToString("O", CultureInfo.InvariantCulture));
                return true;
            case TimeOnly time:
                writer.WriteStringValue(time.ToString("O", CultureInfo.InvariantCulture));
                return true;
            case char letter:
                writer.WriteStringValue(new ReadOnlySpan<char>(in letter));
                return true;
            case byte[] bytes:
                WriteCappedString(writer, Convert.ToBase64String(bytes));
                return true;
            case Uri uri:
                WriteCappedString(writer, uri.ToString());
                return true;
            case Enum:
                writer.WriteStringValue(value.ToString());
                return true;
            default:
                return false;
        }
    }

    private void WriteCappedString(Utf8JsonWriter writer, string text)
    {
        if (text.Length <= options.MaxStringLength)
        {
            writer.WriteStringValue(text);
            return;
        }

        writer.WriteStringValue(string.Concat(text.AsSpan(0, options.MaxStringLength), "…"));
    }

    private void WriteObject(Utf8JsonWriter writer, object value, int depth, Walk walk)
    {
        var plan = _plans.GetOrAdd(value.GetType(), BuildPlan);
        writer.WriteStartObject();
        var written = 0;
        foreach (var member in plan.Members)
        {
            if (written == options.MaxObjectMembers || walk.OverBudget(writer))
            {
                writer.WriteString("…"u8, "[Truncated]");
                break;
            }

            writer.WritePropertyName(member.Name);
            if (member.Mask is { } mask)
            {
                WriteMasked(writer, member, mask, value);
            }
            else
            {
                object? memberValue;
                try
                {
                    memberValue = member.Read(value);
                }
                catch (Exception)
                {
                    metrics.RecordFailure(LoggingMetrics.ComponentDestructurer);
                    writer.WriteStringValue("[DestructuringFailed]");
                    written++;
                    continue;
                }

                WriteValue(writer, memberValue, depth + 1, walk);
            }

            written++;
        }

        writer.WriteEndObject();
    }

    private void WriteMasked(Utf8JsonWriter writer, MemberPlan member, MaskRule mask, object owner)
    {
        if (mask.ShowFirst <= 0 && mask.ShowLast <= 0)
        {
            // Full mask: the protected value is never read at all.
            writer.WriteStringValue(mask.Text);
            return;
        }

        string text;
        try
        {
            text = ToInvariantString(member.Read(owner));
        }
        catch (Exception)
        {
            metrics.RecordFailure(LoggingMetrics.ComponentDestructurer);
            writer.WriteStringValue("[DestructuringFailed]");
            return;
        }

        var first = Math.Min(Math.Max(mask.ShowFirst, 0), text.Length);
        var last = Math.Min(Math.Max(mask.ShowLast, 0), text.Length - first);
        writer.WriteStringValue(
            string.Concat(text.AsSpan(0, first), mask.Text, text.AsSpan(text.Length - last))
        );
    }

    private void WriteSequence(Utf8JsonWriter writer, IEnumerable sequence, int depth, Walk walk)
    {
        writer.WriteStartArray();
        var items = 0;
        try
        {
            foreach (var item in sequence)
            {
                if (items == options.MaxCollectionItems || walk.OverBudget(writer))
                {
                    writer.WriteStringValue("…");
                    break;
                }

                WriteValue(writer, item, depth + 1, walk);
                items++;
            }
        }
        catch (Exception)
        {
            // A lazy sequence threw mid-enumeration; the array closes valid either way.
            metrics.RecordFailure(LoggingMetrics.ComponentDestructurer);
            writer.WriteStringValue("[DestructuringFailed]");
        }

        writer.WriteEndArray();
    }

    private void WriteDictionary(Utf8JsonWriter writer, IDictionary dictionary, int depth, Walk walk)
    {
        writer.WriteStartObject();
        var members = 0;
        try
        {
            foreach (DictionaryEntry pair in dictionary)
            {
                if (members == options.MaxObjectMembers || walk.OverBudget(writer))
                {
                    writer.WriteString("…"u8, "[Truncated]");
                    break;
                }

                writer.WritePropertyName(ToInvariantString(pair.Key));
                WriteValue(writer, pair.Value, depth + 1, walk);
                members++;
            }
        }
        catch (Exception)
        {
            metrics.RecordFailure(LoggingMetrics.ComponentDestructurer);
            writer.WriteString("…"u8, "[DestructuringFailed]");
        }

        writer.WriteEndObject();
    }

    private static string ToInvariantString(object? value) =>
        value switch
        {
            null => string.Empty,
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture),
            _ => value.ToString() ?? string.Empty,
        };

    private sealed record TypePlan(MemberPlan[] Members);

    private sealed record MemberPlan(
        string Name,
        PropertyInfo? Property,
        FieldInfo? Field,
        MaskRule? Mask
    )
    {
        public object? Read(object owner) =>
            Property is not null ? Property.GetValue(owner) : Field!.GetValue(owner);
    }

    /// <summary>Built once per runtime type: exclusion decisions happen here, so an excluded
    /// member is absent from the plan and can never be read on any later event.</summary>
    private TypePlan BuildPlan(Type type)
    {
        var members = new List<MemberPlan>();
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            if (!property.CanRead || property.GetIndexParameters().Length > 0)
            {
                continue;
            }

            AddMember(members, type, property.Name, property, null);
        }

        foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
        {
            AddMember(members, type, field.Name, null, field);
        }

        return new TypePlan([.. members]);
    }

    private void AddMember(
        List<MemberPlan> members,
        Type type,
        string name,
        PropertyInfo? property,
        FieldInfo? field
    )
    {
        MaskRule? mask = null;
        if (options.RuleFor(type, name) is { } rule)
        {
            if (rule.Excluded)
            {
                return;
            }

            mask = rule.Mask;
        }

        MemberInfo member = property is not null ? property : field!;
        foreach (var attribute in member.GetCustomAttributes(inherit: true))
        {
            switch (attribute)
            {
                case NotLoggedAttribute:
                    return;
                case LogMaskedAttribute masked:
                    mask ??= new MaskRule(masked.Text, masked.ShowFirst, masked.ShowLast);
                    break;
                default:
                    if (options.MapLegacyAttributes)
                    {
                        var legacyName = attribute.GetType().Name;
                        if (legacyName == "NotLoggedAttribute")
                        {
                            return;
                        }

                        if (legacyName == "LogMaskedAttribute" && mask is null)
                        {
                            mask = LegacyMask(attribute);
                        }
                    }

                    break;
            }
        }

        members.Add(new MemberPlan(name, property, field, mask));
    }

    /// <summary>Reads a legacy masking attribute's options by property name, so Destructurama
    /// annotations keep working without a package reference.</summary>
    private static MaskRule LegacyMask(object attribute)
    {
        var type = attribute.GetType();
        var text = type.GetProperty("Text")?.GetValue(attribute) as string ?? "***";
        var first = type.GetProperty("ShowFirst")?.GetValue(attribute) as int? ?? 0;
        var last = type.GetProperty("ShowLast")?.GetValue(attribute) as int? ?? 0;
        return new MaskRule(text, first, last);
    }
}
