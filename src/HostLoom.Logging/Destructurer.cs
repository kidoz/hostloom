using System.Buffers;
using System.Collections;
using System.Collections.Concurrent;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Numerics;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Text.Json;

namespace HostLoom.Logging;

/// <summary>
/// Serializes one <c>{@...}</c> hole value into a complete, valid JSON fragment on the producer
/// thread, so the object is snapshotted at capture time. Bounded everywhere: depth, collection
/// items, object members, the length of string values and dictionary keys, and encoded bytes all
/// cap with explicit non-sensitive sentinels; cycles cut with <c>"[Cycle]"</c>. Protection is
/// fail-closed: exclusion and masking decisions come from a cached per-type plan applied before a
/// member is ever read, an excluded member simply does not exist to this walker, and any getter
/// or serializer failure emits <c>"[DestructuringFailed]"</c> — never the value's
/// <c>ToString()</c>.
/// </summary>
/// <param name="nativeCode">Whether reflection sees only the members Native AOT kept; null
/// detects it. Tests set it to reach the native branch on the JIT.</param>
internal sealed class Destructurer(
    DestructuringOptions options,
    LoggingMetrics? metrics,
    bool? nativeCode = null
)
{
    private readonly bool _nativeCode = nativeCode ?? !RuntimeFeature.IsDynamicCodeSupported;

    /// <summary>
    /// Bytes the budget keeps free until a cut is marked, so the marker always fits: the longest
    /// one a container writes at an element boundary, <c>,"\u2026":"[DestructuringFailed]"</c>.
    /// </summary>
    private const int MarkerReserve = 33;

    private static readonly JsonWriterOptions WriterOptions = new()
    {
        Indented = false,
        SkipValidation = true,
    };

    /// <summary>What <c>WriteString("…", "[Truncated]")</c> emits, for a cut written raw.</summary>
    private static ReadOnlySpan<byte> TruncatedMember => "\"\\u2026\":\"[Truncated]\""u8;

    /// <summary>What <c>WriteStringValue("…")</c> emits, for a cut written raw.</summary>
    private static ReadOnlySpan<byte> TruncatedItem => "\"\\u2026\""u8;

    /// <summary>Serilog's byte-array cutoff: longer arrays are summarized, not dumped.</summary>
    private const int MaxByteArrayLength = 1024;

    /// <summary>Null marks a type whose plan could not be built, so the failure is not retried
    /// and counted on every event.</summary>
    private readonly ConcurrentDictionary<Type, TypePlan?> _plans = new();

    /// <summary>
    /// Per-thread scratch: Utf8JsonWriter demands multi-kilobyte chunks from its buffer writer,
    /// so a fresh small buffer per event costs ~5 KB of garbage. Reusing buffer, writer, and
    /// ancestors per thread leaves only the unavoidable work (getter boxing, date strings).
    /// </summary>
    [ThreadStatic]
    private static Scratch? _scratch;

    private sealed class Scratch
    {
        public ArrayBufferWriter<byte> Buffer = new(4 * 1024);
        public Utf8JsonWriter? Writer;
        public object?[] Ancestors = [];
        public bool Busy;
    }

    /// <summary>
    /// Serializes one value into a complete JSON fragment and returns it as a span over
    /// thread-local scratch. The caller must copy the span before the next call on this thread —
    /// the entry writers do, immediately. A container is cut at the element that would outgrow
    /// <paramref name="byteBudget"/>, so the fragment stays within it; only a budget too small
    /// for the cut marker itself, or a single scalar larger than the budget, can return more,
    /// and the caller must check the length.
    /// </summary>
    /// <param name="objectsAsText">Serilog's capture for a collection in a hole without an
    /// operator: sequences and dictionaries keep their structure and scalars their types, but
    /// every other object is written as its invariant <c>ToString()</c> instead of its
    /// members.</param>
    public ReadOnlySpan<byte> Destructure(object value, int byteBudget, bool objectsAsText = false)
    {
        var scratch = _scratch ??= new Scratch();
        if (scratch.Busy)
        {
            // Reentrancy: a property getter is logging while being destructured. Rare enough
            // that a throwaway buffer is fine; the thread-local one is mid-walk above us.
            var local = new ArrayBufferWriter<byte>(1024);
            using var localWriter = new Utf8JsonWriter(local, WriterOptions);
            DestructureInto(
                localWriter,
                local,
                value,
                byteBudget,
                objectsAsText,
                new object?[options.MaxDepth]
            );
            return local.WrittenSpan;
        }

        if (scratch.Buffer.Capacity > 128 * 1024)
        {
            // A burst grew the retained buffer past reason; a fresh one caps the retention.
            scratch.Buffer = new ArrayBufferWriter<byte>(4 * 1024);
            scratch.Writer = null;
        }

        if (scratch.Ancestors.Length < options.MaxDepth)
        {
            scratch.Ancestors = new object?[options.MaxDepth];
        }

        scratch.Busy = true;
        try
        {
            scratch.Buffer.ResetWrittenCount();
            var writer = scratch.Writer ??= new Utf8JsonWriter(Stream.Null, WriterOptions);
            writer.Reset(scratch.Buffer);
            if (
                !DestructureInto(
                    writer,
                    scratch.Buffer,
                    value,
                    byteBudget,
                    objectsAsText,
                    scratch.Ancestors
                )
            )
            {
                // The writer's state is unknown after a mid-write failure; rebuild it lazily.
                scratch.Writer = null;
            }

            return scratch.Buffer.WrittenSpan;
        }
        finally
        {
            scratch.Busy = false;
        }
    }

    private bool DestructureInto(
        Utf8JsonWriter writer,
        ArrayBufferWriter<byte> buffer,
        object value,
        int byteBudget,
        bool objectsAsText,
        object?[] ancestors
    )
    {
        try
        {
            var walk = new Walk(ancestors, buffer, byteBudget, objectsAsText);
            WriteValue(writer, value, 0, ref walk);
            writer.Flush();
            return true;
        }
        catch (Exception)
        {
            // Throw away whatever partial output exists and emit the sentinel, so the
            // fragment is always valid JSON.
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            buffer.ResetWrittenCount();
            buffer.Write("\"[DestructuringFailed]\""u8);
            return false;
        }
        finally
        {
            // A failed nested write can unwind before WriteValue clears its ancestor slot.
            Array.Clear(ancestors);
        }
    }

    /// <summary>
    /// The state of one walk. Positions come from the buffer plus the writer's pending bytes,
    /// not from the writer's own commit counter, which a cut leaves stale.
    /// </summary>
    private struct Walk(
        object?[] ancestors,
        ArrayBufferWriter<byte> buffer,
        int byteLimit,
        bool objectsAsText
    )
    {
        public readonly object?[] Ancestors = ancestors;

        public readonly ArrayBufferWriter<byte> Buffer = buffer;

        public readonly int ByteLimit = byteLimit;

        public readonly bool ObjectsAsText = objectsAsText;

        /// <summary>Set once the budget cut an element: every enclosing container then closes
        /// without writing anything more.</summary>
        public bool Truncated;

        public readonly int Position(Utf8JsonWriter writer) =>
            Buffer.WrittenCount + writer.BytesPending;
    }

    /// <summary>
    /// Checks the byte budget after an element of the container at <paramref name="depth"/>.
    /// The fragment must still have room to close that container and every one around it and,
    /// until a cut is marked, to mark one. An element that leaves less is removed again and the
    /// marker takes its place, so a finished fragment never outgrows the budget. Returns whether
    /// the container may write another element.
    /// </summary>
    private static bool Admit(
        Utf8JsonWriter writer,
        ref Walk walk,
        int depth,
        int mark,
        bool first,
        ReadOnlySpan<byte> marker
    )
    {
        var room = walk.ByteLimit - (depth + 1) - (walk.Truncated ? 0 : MarkerReserve);
        if (walk.Position(writer) <= room)
        {
            // A cut inside this element is already marked there, and nothing fits after it.
            return !walk.Truncated;
        }

        // The writer still counts the removed element as written, which the marker now stands
        // in for, so the marker goes straight to the buffer with the separator the element had.
        writer.Flush();
        walk.Buffer.Truncate(mark);
        if (!first)
        {
            walk.Buffer.Write(","u8);
        }

        walk.Buffer.Write(marker);
        walk.Truncated = true;
        return false;
    }

    private void WriteValue(Utf8JsonWriter writer, object? value, int depth, ref Walk walk)
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
                WriteDictionary(writer, dictionary, depth, ref walk);
                break;
            case IEnumerable sequence:
                WriteSequence(writer, sequence, depth, ref walk);
                break;
            case ITuple tuple when value.GetType().IsValueType:
                // Serilog writes a value tuple as a sequence of its items.
                WriteSequence(writer, TupleItems(tuple), depth, ref walk);
                break;
            default:
                if (walk.ObjectsAsText)
                {
                    WriteStringified(writer, value);
                }
                else
                {
                    WriteObject(writer, value!, depth, ref walk);
                }

                break;
        }

        walk.Ancestors[depth] = null;
    }

    private static IEnumerable<object?> TupleItems(ITuple tuple)
    {
        for (var i = 0; i < tuple.Length; i++)
        {
            yield return tuple[i];
        }
    }

    /// <summary>The deterministic scalar table, mirroring the typed capture path: numbers stay
    /// numbers, non-finite floats and date/time/Guid/enum values are strings, byte arrays are
    /// Serilog's uppercase hex, and every string is subject to the length cap.</summary>
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
            case nint number:
                writer.WriteNumberValue((long)number);
                return true;
            case nuint number:
                writer.WriteNumberValue((ulong)number);
                return true;
            case Int128 number:
                writer.WriteRawValue(number.ToString(CultureInfo.InvariantCulture));
                return true;
            case UInt128 number:
                writer.WriteRawValue(number.ToString(CultureInfo.InvariantCulture));
                return true;
            case BigInteger number:
                writer.WriteRawValue(number.ToString(CultureInfo.InvariantCulture));
                return true;
            case Half number:
                if (Half.IsFinite(number))
                {
                    writer.WriteNumberValue((float)number);
                }
                else
                {
                    writer.WriteStringValue(number.ToString(CultureInfo.InvariantCulture));
                }

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
                WriteCappedString(writer, ByteArrayText(bytes));
                return true;
            case ReadOnlyMemory<byte> memory:
                WriteCappedString(writer, MemoryText(memory));
                return true;
            case Memory<byte> memory:
                WriteCappedString(writer, MemoryText(memory));
                return true;
            case Delegate or MemberInfo or Assembly or Module:
                // Serilog's names for these. Walking a delegate reaches its closure's captured
                // locals through Target, and reflection objects expand into tens of kilobytes.
                WriteStringified(writer, value);
                return true;
            case Uri uri:
                WriteStringified(writer, uri);
                return true;
            case Enum:
                writer.WriteStringValue(value.ToString());
                return true;
            default:
                return false;
        }
    }

    /// <summary>
    /// Serilog's rendering: uppercase hex, or for an array longer than 1024 bytes its first 16
    /// bytes followed by <c>"... (N bytes)"</c>.
    /// </summary>
    internal static string ByteArrayText(ReadOnlySpan<byte> bytes) =>
        bytes.Length <= MaxByteArrayLength
            ? Convert.ToHexString(bytes)
            : string.Create(
                CultureInfo.InvariantCulture,
                $"{Convert.ToHexString(bytes[..16])}... ({bytes.Length} bytes)"
            );

    /// <summary>Accessing memory can execute a caller-owned MemoryManager. Resolve it before
    /// writing the value so a failure replaces only this member or collection element.</summary>
    internal string MemoryText(ReadOnlyMemory<byte> memory)
    {
        try
        {
            return ByteArrayText(memory.Span);
        }
        catch (Exception)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            return "[DestructuringFailed]";
        }
    }

    private void WriteCappedString(Utf8JsonWriter writer, string text) =>
        writer.WriteStringValue(Capped(text));

    /// <summary>Keeps <see cref="DestructuringOptions.MaxStringLength"/> characters and marks a
    /// cut with a trailing "…" — for string values and dictionary keys alike.</summary>
    private string Capped(string text) => ExceptionText.Cap(text, options.MaxStringLength);

    /// <summary>A value's own <c>ToString()</c> may throw. Caught here, before anything of the
    /// element is written, so the sentinel lands where the value would have and the enclosing
    /// container stays valid JSON.</summary>
    private void WriteStringified(Utf8JsonWriter writer, object? value)
    {
        string text;
        try
        {
            text = ToInvariantString(value);
        }
        catch (Exception)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            writer.WriteStringValue("[DestructuringFailed]");
            return;
        }

        WriteCappedString(writer, text);
    }

    private void WriteObject(Utf8JsonWriter writer, object value, int depth, ref Walk walk)
    {
        var plan = _plans.GetOrAdd(value.GetType(), TryBuildPlan);
        if (plan is null)
        {
            // Reflection failed for this type (a member attribute that cannot be constructed, a
            // type that cannot load). Nothing of the object is written yet, so the sentinel takes
            // its place and the enclosing container stays valid.
            writer.WriteStringValue("[DestructuringFailed]");
            return;
        }

        writer.WriteStartObject();
        var written = 0;
        foreach (var member in plan.Members)
        {
            if (written == options.MaxObjectMembers)
            {
                writer.WriteString("…"u8, "[Truncated]");
                break;
            }

            var mark = walk.Position(writer);
            writer.WritePropertyName(member.Name);
            if (member.Mask is { } mask)
            {
                WriteMasked(writer, member, mask, value);
            }
            else if (TryRead(member, value, out var memberValue))
            {
                WriteValue(writer, memberValue, depth + 1, ref walk);
            }
            else
            {
                writer.WriteStringValue("[DestructuringFailed]");
            }

            if (!Admit(writer, ref walk, depth, mark, written == 0, TruncatedMember))
            {
                break;
            }

            written++;
        }

        if (plan.TypeTag is { } typeTag && !walk.Truncated)
        {
            var mark = walk.Position(writer);
            writer.WriteString("$type"u8, typeTag);
            Admit(writer, ref walk, depth, mark, written == 0, TruncatedMember);
        }

        writer.WriteEndObject();
    }

    private bool TryRead(MemberPlan member, object owner, out object? value)
    {
        try
        {
            value = member.Read(owner);
            return true;
        }
        catch (Exception)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            value = null;
            return false;
        }
    }

    /// <summary>
    /// Reveals <c>ShowFirst</c>/<c>ShowLast</c> characters around the mask only while at least as
    /// many characters stay hidden as are shown: a value shorter than twice the requested reveal
    /// — a four-digit PIN under <c>ShowLast = 4</c> — is written as the mask text alone.
    /// </summary>
    private void WriteMasked(Utf8JsonWriter writer, MemberPlan member, MaskRule mask, object owner)
    {
        var first = Math.Max(mask.ShowFirst, 0);
        var last = Math.Max(mask.ShowLast, 0);
        if (first == 0 && last == 0)
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
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            writer.WriteStringValue("[DestructuringFailed]");
            return;
        }

        if (mask.MinimumLength > text.Length)
        {
            writer.WriteStringValue(mask.Text);
            return;
        }

        writer.WriteStringValue(
            string.Concat(text.AsSpan(0, first), mask.Text, text.AsSpan(text.Length - last))
        );
    }

    private void WriteSequence(
        Utf8JsonWriter writer,
        IEnumerable sequence,
        int depth,
        ref Walk walk
    )
    {
        writer.WriteStartArray();
        var items = 0;
        var writingValue = false;
        try
        {
            foreach (var item in sequence)
            {
                if (items == options.MaxCollectionItems)
                {
                    writer.WriteStringValue("…");
                    break;
                }

                var mark = walk.Position(writer);
                writingValue = true;
                WriteValue(writer, item, depth + 1, ref walk);
                var admitted = Admit(writer, ref walk, depth, mark, items == 0, TruncatedItem);
                writingValue = false;
                if (!admitted)
                {
                    break;
                }

                items++;
            }
        }
        // Only enumeration/key failures occur at a safe container boundary. A nested writer
        // failure must reach DestructureInto, which replaces the entire partial fragment.
        catch (Exception) when (!writingValue)
        {
            // A lazy sequence threw mid-enumeration; the array closes valid either way.
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            writer.WriteStringValue("[DestructuringFailed]");
        }

        writer.WriteEndArray();
    }

    private void WriteDictionary(
        Utf8JsonWriter writer,
        IDictionary dictionary,
        int depth,
        ref Walk walk
    )
    {
        writer.WriteStartObject();
        var members = 0;
        // Keys that render alike (the same protected representation, or long keys sharing a
        // capped prefix) would otherwise produce duplicate JSON keys; the first one wins.
        var written = new HashSet<string>(StringComparer.Ordinal);
        var omitted = false;
        var writingValue = false;
        try
        {
            var complete = true;
            foreach (DictionaryEntry pair in dictionary)
            {
                if (members == options.MaxObjectMembers)
                {
                    writer.WriteString("…"u8, "[Truncated]");
                    complete = false;
                    break;
                }

                // A key is caller data just like a value, so it is capped the same way.
                var key = DictionaryKey(pair.Key, writer, depth, ref walk);
                if (!written.Add(key))
                {
                    omitted = true;
                    continue;
                }

                var mark = walk.Position(writer);
                writingValue = true;
                writer.WritePropertyName(key);
                WriteValue(writer, pair.Value, depth + 1, ref walk);
                var admitted = Admit(writer, ref walk, depth, mark, members == 0, TruncatedMember);
                writingValue = false;
                if (!admitted)
                {
                    complete = false;
                    break;
                }

                members++;
            }

            // Omitted duplicates are marked like any other cut. The marker is shorter than the
            // reserve the walk keeps free, so it cannot outgrow the byte budget.
            if (complete && omitted && written.Add("…"))
            {
                writer.WriteString("…"u8, "[Truncated]");
            }
        }
        // Only enumeration/key failures occur at a safe container boundary. A nested writer
        // failure must reach DestructureInto, which replaces the entire partial fragment.
        catch (Exception) when (!writingValue)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            writer.WriteString("…"u8, "[DestructuringFailed]");
        }

        writer.WriteEndObject();
    }

    /// <summary>Complex keys in a destructured dictionary are protected JSON rendered as key
    /// text. They share the graph's depth, ancestors, and remaining byte budget; their own
    /// ToString() must never bypass a member's protection.</summary>
    private string DictionaryKey(object key, Utf8JsonWriter outer, int depth, ref Walk walk)
    {
        // Match the scalar table in TryWriteScalar. Plain collection holes keep their documented
        // stringification semantics; strings and other scalar keys keep their existing spelling.
        if (
            walk.ObjectsAsText
            || key
                is string
                    or bool
                    or int
                    or long
                    or double
                    or float
                    or decimal
                    or short
                    or ushort
                    or byte
                    or sbyte
                    or uint
                    or ulong
                    or Guid
                    or DateTimeOffset
                    or DateTime
                    or TimeSpan
                    or DateOnly
                    or TimeOnly
                    or char
                    or byte[]
                    or ReadOnlyMemory<byte>
                    or Memory<byte>
                    or Delegate
                    or MemberInfo
                    or Assembly
                    or Module
                    or Uri
                    or Enum
        )
        {
            return Capped(ToInvariantString(key));
        }

        var remaining = walk.ByteLimit - walk.Position(outer) - (depth + 1) - MarkerReserve;
        if (remaining <= 0)
        {
            return "…";
        }

        var buffer = new ArrayBufferWriter<byte>(256);
        using var writer = new Utf8JsonWriter(buffer, WriterOptions);
        var keyWalk = new Walk(walk.Ancestors, buffer, remaining, objectsAsText: false);
        try
        {
            WriteValue(writer, key, depth + 1, ref keyWalk);
            writer.Flush();
            return Capped(Encoding.UTF8.GetString(buffer.WrittenSpan));
        }
        catch (Exception)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            return "[DestructuringFailed]";
        }
        finally
        {
            // A failed nested key walk must not leave ancestors in the enclosing graph.
            Array.Clear(walk.Ancestors, depth + 1, walk.Ancestors.Length - depth - 1);
        }
    }

    private static string ToInvariantString(object? value) =>
        value switch
        {
            null => string.Empty,
            IFormattable formattable => formattable.ToString(null, CultureInfo.InvariantCulture)
                ?? string.Empty,
            _ => value.ToString() ?? string.Empty,
        };

    private sealed record TypePlan(MemberPlan[] Members, string? TypeTag);

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

    private TypePlan? TryBuildPlan(Type type)
    {
        try
        {
            return BuildPlan(type);
        }
        catch (Exception)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            return null;
        }
    }

    /// <summary>Built once per runtime type: exclusion decisions happen here, so an excluded
    /// member is absent from the plan and can never be read on any later event.</summary>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2070",
        Justification = "Destructuring reads the members a logged value's runtime type still has. "
            + "Trimming can remove members that nothing else uses; DestructuringOptions.Preserve "
            + "keeps them, a member that is gone is simply not written, and a type left with none "
            + "under Native AOT is counted."
    )]
    private TypePlan BuildPlan(Type type)
    {
        var members = new List<MemberPlan>();
        // A property hidden with 'new' is listed along with its replacement; like Serilog, keep
        // only the most derived one, so no object carries the same key twice.
        var properties = new Dictionary<string, PropertyInfo>(StringComparer.Ordinal);
        foreach (var property in type.GetProperties(BindingFlags.Public | BindingFlags.Instance))
        {
            // A public property with a private getter is not readable from outside, and Serilog
            // does not read it either: 'public string Password { private get; set; }'.
            if (
                property.GetMethod is not { IsPublic: true }
                || property.GetIndexParameters().Length > 0
            )
            {
                continue;
            }

            if (
                !properties.TryGetValue(property.Name, out var existing)
                || property.DeclaringType!.IsSubclassOf(existing.DeclaringType!)
            )
            {
                properties[property.Name] = property;
            }
        }

        foreach (var property in properties.Values)
        {
            AddMember(members, type, property.Name, property, null);
        }

        var discovered = properties.Count;
        if (options.IncludeFields)
        {
            foreach (var field in type.GetFields(BindingFlags.Public | BindingFlags.Instance))
            {
                discovered++;
                // A field hidden by a derived property or field of the same name is left out.
                if (
                    !properties.ContainsKey(field.Name)
                    && !members.Exists(member => member.Name == field.Name)
                )
                {
                    AddMember(members, type, field.Name, null, field);
                }
            }
        }

        if (discovered == 0 && _nativeCode)
        {
            // Native AOT keeps reflection data only for members it saw used, so a type that shows
            // none has most likely lost them all. The plan is cached, so this counts once per type.
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
        }

        return new TypePlan([.. members], TypeTagFor(type, members));
    }

    /// <summary>Serilog's type tag: the short type name, except for anonymous and other
    /// compiler-generated types, and never next to a member already named <c>$type</c>.</summary>
    private string? TypeTagFor(Type type, List<MemberPlan> members) =>
        !options.TypeTags
        || type.Name.StartsWith('<')
        || Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), inherit: false)
        || members.Exists(member => member.Name == "$type")
            ? null
            : type.Name;

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
        // Attribute.GetCustomAttributes walks a virtual property's override chain. The instance
        // MemberInfo.GetCustomAttributes(inherit) silently ignores the flag for properties, so an
        // override of an annotated property would otherwise lose its protection.
        foreach (var attribute in Attribute.GetCustomAttributes(member, inherit: true))
        {
            switch (attribute)
            {
                case NotLoggedAttribute:
                    return;
                case LogMaskedAttribute masked:
                    mask = MaskRule.Combine(
                        mask,
                        new MaskRule(masked.Text, masked.ShowFirst, masked.ShowLast)
                    );
                    break;
                default:
                    if (options.MapLegacyAttributes)
                    {
                        var legacyName = attribute.GetType().Name;
                        if (legacyName == "NotLoggedAttribute")
                        {
                            return;
                        }

                        if (legacyName == "LogMaskedAttribute")
                        {
                            mask = MaskRule.Combine(mask, LegacyMask(attribute));
                        }

                        // A regex replacement is not reproduced; the member is masked whole
                        // instead, so the value it was meant to hide never appears.
                        if (legacyName == "LogReplacedAttribute")
                        {
                            mask = MaskRule.Combine(mask, new MaskRule("***", 0, 0));
                        }
                    }

                    break;
            }
        }

        members.Add(new MemberPlan(name, property, field, mask));
    }

    /// <summary>Reads a legacy masking attribute's options by property name, so Destructurama
    /// annotations keep working without a package reference. Options that cannot be read mask
    /// the member whole, so it stays protected and the rest of the object is still written.
    /// Native AOT gets there when nothing else reads the attribute's getters: it keeps the
    /// properties but cannot call them.</summary>
    [UnconditionalSuppressMessage(
        "Trimming",
        "IL2075",
        Justification = "The legacy attribute's options are optional; when trimming removed or "
            + "disabled them, the member is masked whole instead."
    )]
    private MaskRule LegacyMask(object attribute)
    {
        try
        {
            var type = attribute.GetType();
            var text = type.GetProperty("Text")?.GetValue(attribute) as string ?? "***";
            var first = type.GetProperty("ShowFirst")?.GetValue(attribute) as int? ?? 0;
            var last = type.GetProperty("ShowLast")?.GetValue(attribute) as int? ?? 0;
            return new MaskRule(text, first, last);
        }
        catch (Exception)
        {
            metrics?.RecordFailure(LoggingMetrics.ComponentDestructurer);
            return new MaskRule("***", 0, 0);
        }
    }
}
