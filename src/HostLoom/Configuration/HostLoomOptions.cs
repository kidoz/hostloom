namespace HostLoom;

public sealed class HostLoomOptions
{
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
