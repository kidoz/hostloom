using System.Collections;
using System.Text;

namespace HostLoom.Logging;

/// <summary>Capture under each distinct live limit set while the interpolation is on the
/// caller's stack. This keeps spans, long text, and even a long series of empty holes bounded.
/// The MEL view uses default limits. HostLoom selects its own snapshot during dispatch.</summary>
internal sealed class LogFastCapture
{
    private readonly LogCaptureLimits[] _limits = LogCaptureLimits.Active;
    public LogEntry[] Entries { get; }
    public int FormatBudget { get; }

    public LogFastCapture()
    {
        Entries = new LogEntry[_limits.Length];
        for (var i = 0; i < Entries.Length; i++)
        {
            var entry = LogEntryPool.Rent();
            entry.ApplyCaptureLimits(_limits[i]);
            entry.DeferNameValidation();
            Entries[i] = entry;
            FormatBudget = Math.Max(FormatBudget, Math.Max(_limits[i].Text, _limits[i].Message));
        }
    }

    public LogFastState Snapshot()
    {
        if (Entries.Length == 1)
            return new LogFastState(Entries[0]);

        var variants = new Dictionary<LogCaptureLimits, LogFastState>(Entries.Length - 1);
        for (var i = 1; i < Entries.Length; i++)
            variants.Add(_limits[i], new LogFastState(Entries[i]));
        return new LogFastState(Entries[0], variants);
    }

    public void Release()
    {
        foreach (var entry in Entries)
            LogEntryPool.Return(entry);
    }
}

internal sealed class LogFastState(
    LogEntry entry,
    Dictionary<LogCaptureLimits, LogFastState>? variants = null
) : IReadOnlyList<KeyValuePair<string, object?>>
{
    private readonly string _message = Encoding.UTF8.GetString(entry.Message);
    private readonly KeyValuePair<string, object?>[] _fields = CaptureFields(entry);
    public int CaptureFailures { get; } = entry.CaptureFailures;
    public int OverflowHoleFields { get; } = entry.OverflowHoleFields;

    public LogFastState For(HostLoomLoggerOptions options) =>
        variants is not null
        && variants.TryGetValue(LogCaptureLimits.From(options), out var snapshot)
            ? snapshot
            : this;

    private static KeyValuePair<string, object?>[] CaptureFields(LogEntry entry)
    {
        var fields = new KeyValuePair<string, object?>[entry.FieldCount];
        for (var i = 0; i < fields.Length; i++)
        {
            entry.GetHandoffField(i, out var name, out var value, out var kind);
            fields[i] = new(name, LoggerExtensions.BoxValue(value, kind));
        }
        return fields;
    }

    public int Count => _fields.Length;
    public KeyValuePair<string, object?> this[int index] => _fields[index];

    public IEnumerator<KeyValuePair<string, object?>> GetEnumerator() =>
        ((IEnumerable<KeyValuePair<string, object?>>)_fields).GetEnumerator();

    IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

    public override string ToString() => _message;
}
