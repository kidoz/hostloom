namespace HostLoom.Logging;

/// <summary>Freeze a caller-owned UTF-8 formatter at its interpolation hole. Rendering the
/// same value for several providers must never call back into a changed object.</summary>
internal sealed class CapturedUtf8Value : IUtf8SpanFormattable
{
    private readonly record struct Token(byte[] Bytes, int Length, bool Failed)
    {
        public static Token Capture(IUtf8SpanFormattable value, string? format, int budget)
        {
            byte[] buffer = new byte[64];
            var fits = LogEntry.TryFormatBounded(
                ref buffer,
                0,
                budget,
                value,
                format,
                out var length,
                out var failed
            );
            return new Token(fits ? buffer : [], fits ? length : -1, failed);
        }
    }

    private readonly Token _display;
    private readonly Token _canonical;

    public CapturedUtf8Value(IUtf8SpanFormattable value, string? format, int budget)
    {
        _display = Token.Capture(value, format, budget);
        _canonical = string.IsNullOrEmpty(format) ? _display : Token.Capture(value, null, budget);
    }

    public bool TryFormat(
        Span<byte> utf8Destination,
        out int bytesWritten,
        ReadOnlySpan<char> format,
        IFormatProvider? provider
    )
    {
        var token = format.IsEmpty ? _canonical : _display;
        if (token.Failed)
            throw new FormatException("The log value could not be formatted.");
        if (token.Length < 0 || token.Length > utf8Destination.Length)
        {
            bytesWritten = 0;
            return false;
        }
        token.Bytes.AsSpan(0, token.Length).CopyTo(utf8Destination);
        bytesWritten = token.Length;
        return true;
    }
}
