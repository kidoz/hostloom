namespace HostLoom.Logging;

/// <summary>
/// Adds ambient fields to every event, invoked on the producer thread before queueing — the only
/// point where <c>AsyncLocal</c> context is still visible; by the time the background writer
/// runs, it is gone. Enrichers execute in registration order; within the enricher rank a later
/// field wins a name collision, and an event hole always outranks an enricher. A throwing
/// enricher is counted and skipped: it can cost neither the event, the remaining enrichers, nor
/// the logging caller.
/// </summary>
public interface ILogEnricher
{
    void Enrich(ref LogEntryWriter writer);
}

/// <summary>
/// The typed field surface handed to enrichers. Values are captured immediately into the record's
/// buffers, so nothing ambient is read after the hand-off — and only typed values are accepted:
/// there is deliberately no way to inject raw JSON.
/// </summary>
public readonly ref struct LogEntryWriter
{
    private readonly LogEntry _entry;

    internal LogEntryWriter(LogEntry entry)
    {
        _entry = entry;
    }

    /// <summary>A text field; null becomes an explicit JSON null.</summary>
    public void Add(string name, string? value)
    {
        if (value is null)
        {
            _entry.AddFieldNull(name, LogFieldSource.Enricher);
        }
        else
        {
            _entry.AddFieldText(name, value, LogFieldSource.Enricher);
        }
    }

    public void Add(string name, bool value) =>
        _entry.AddFieldBoolean(name, value, LogFieldSource.Enricher);

    public void Add(string name, int value) =>
        _entry.AddFieldFormattable(name, value, LogFieldKind.Number, null, LogFieldSource.Enricher);

    public void Add(string name, long value) =>
        _entry.AddFieldFormattable(name, value, LogFieldKind.Number, null, LogFieldSource.Enricher);

    public void Add(string name, double value) =>
        _entry.AddFieldFormattable(
            name,
            value,
            double.IsFinite(value) ? LogFieldKind.Number : LogFieldKind.Text,
            null,
            LogFieldSource.Enricher
        );

    public void Add(string name, decimal value) =>
        _entry.AddFieldFormattable(name, value, LogFieldKind.Number, null, LogFieldSource.Enricher);

    public void Add(string name, Guid value) =>
        _entry.AddFieldFormattable(name, value, LogFieldKind.Text, null, LogFieldSource.Enricher);

    /// <summary>ISO-8601, matching every other timestamp the library emits.</summary>
    public void Add(string name, DateTimeOffset value) =>
        _entry.AddFieldFormattable(name, value, LogFieldKind.Text, "O", LogFieldSource.Enricher);

    public void Add(string name, TimeSpan value) =>
        _entry.AddFieldFormattable(name, value, LogFieldKind.Text, null, LogFieldSource.Enricher);
}
