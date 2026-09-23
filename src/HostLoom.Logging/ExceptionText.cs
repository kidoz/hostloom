namespace HostLoom.Logging;

/// <summary>
/// Renders an exception the way Serilog's <c>@x</c> does: <see cref="Exception.ToString"/>, which
/// already carries the complete chain — inner exceptions and AggregateException children — in the
/// runtime's canonical text. Bounded by a cap with an explicit truncation marker, and guarded:
/// an exception whose own ToString throws must not be able to fault the writer.
/// </summary>
internal static class ExceptionText
{
    public static string Render(Exception exception, int maxLength)
    {
        string text;
        try
        {
            text = exception.ToString();
        }
        catch (Exception)
        {
            text = exception.GetType().FullName ?? "Exception";
        }

        return Cap(text, maxLength);
    }

    /// <summary>
    /// <see cref="Exception.Message"/>, guarded and capped like <see cref="Render"/>: a custom
    /// exception can compute its message and throw from the getter, and one that embeds a
    /// response body would otherwise make a single record arbitrarily large.
    /// </summary>
    public static string Message(Exception exception, int maxLength)
    {
        string text;
        try
        {
            text = exception.Message;
        }
        catch (Exception)
        {
            text = "[MessageUnavailable]";
        }

        return Cap(text, maxLength);
    }

    private static string Cap(string text, int maxLength) =>
        text.Length <= maxLength ? text : string.Concat(text.AsSpan(0, maxLength), "…");
}
