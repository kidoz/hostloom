namespace HostLoom.Logging;

/// <summary>
/// The names <see cref="HostLoomLoggerOptions.Formatter"/> accepts, matched without regard to case.
/// </summary>
public static class LogFormatterNames
{
    /// <summary>ECS-style JSON, written by <see cref="JsonLogFormatter"/>.</summary>
    public const string Json = "Json";

    /// <summary>Compact Log Event Format, written by <see cref="ClefLogFormatter"/>.</summary>
    public const string Clef = "Clef";

    internal static bool IsKnown(string name) =>
        name.Equals(Json, StringComparison.OrdinalIgnoreCase)
        || name.Equals(Clef, StringComparison.OrdinalIgnoreCase);

    /// <summary>
    /// The formatter <paramref name="options"/> names, or the <paramref name="fallback"/> format
    /// when it names none. Validation has already rejected an unknown name.
    /// </summary>
    internal static ILogFormatter Create(HostLoomLoggerOptions options, string fallback) =>
        (options.Formatter ?? fallback).Equals(Clef, StringComparison.OrdinalIgnoreCase)
            ? new ClefLogFormatter(options.MaxExceptionLength)
            : new JsonLogFormatter(options.MaxExceptionLength);
}
