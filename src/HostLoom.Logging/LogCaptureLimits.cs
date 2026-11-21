namespace HostLoom.Logging;

/// <summary>The limits that change interpolation output before a wrapper dispatches to its
/// providers. Field names stay borrowed until each actual receiver validates them.</summary>
internal readonly record struct LogCaptureLimits(int Message, int Text, int Fields)
{
    public static LogCaptureLimits Default =>
        new(
            HostLoomLoggerOptions.DefaultMaxMessageLength,
            HostLoomLoggerOptions.DefaultMaxTextFieldLength,
            HostLoomLoggerOptions.DefaultMaxFieldsPerRecord
        );

    public static LogCaptureLimits From(HostLoomLoggerOptions options) =>
        new(options.MaxMessageLength, options.MaxTextFieldLength, options.MaxFieldsPerRecord);

    private static readonly Lock Gate = new();
    private static readonly Dictionary<LogCaptureLimits, int> Registrations = [];
    private static LogCaptureLimits[] _active = [Default];

    // Published arrays are immutable. The default view is always first for other MEL providers.
    public static LogCaptureLimits[] Active => Volatile.Read(ref _active);

    public static LogCaptureLimits Register(HostLoomLoggerOptions options)
    {
        var limits = From(options);
        if (limits == Default)
            return limits;
        lock (Gate)
        {
            Registrations.TryGetValue(limits, out var count);
            Registrations[limits] = count + 1;
            if (count == 0)
                Volatile.Write(ref _active, [Default, .. Registrations.Keys]);
        }
        return limits;
    }

    public void Unregister()
    {
        if (this == Default)
            return;
        lock (Gate)
        {
            if (--Registrations[this] == 0)
            {
                Registrations.Remove(this);
                Volatile.Write(ref _active, [Default, .. Registrations.Keys]);
            }
        }
    }
}
