namespace HostLoom;

public sealed class HostLoomOptions
{
    /// <summary>The default encoded-frame limit: one mebibyte.</summary>
    public const int DefaultMaxFrameBytes = 1024 * 1024;

    /// <summary>
    /// Maximum encoded request, event, response or fault frame size in bytes, inclusive.
    /// Must be positive. Checked before decoding JSON or Base64 payloads and before the built-in
    /// request, event, and outbox send paths perform broker or store I/O; broker SDK receive
    /// buffers are outside this limit. In-process typed streams have no encoded frame.
    /// </summary>
    public int MaxFrameBytes { get; set; } = DefaultMaxFrameBytes;

    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>
    /// Whether a fault envelope carries the real exception type name and message of any handler
    /// failure. Off by default: a caller then receives the fixed <c>HandlerFault</c> type and
    /// "The request handler failed." for everything except a <see cref="RemoteFaultException"/>,
    /// whose type and message are always forwarded. Turn it on only when every caller is trusted
    /// to see internal exception text; the full exception is logged on the handling side either way.
    /// </summary>
    public bool IncludeFaultDetails { get; set; }
}
