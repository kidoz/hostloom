namespace HostLoom;

/// <summary>
/// A failure a request handler wants the remote caller to read. Its type name and message are
/// copied into the fault envelope verbatim, where every other exception is reduced to a fixed
/// <c>HandlerFault</c> so that internal type names and messages never leave the process.
/// </summary>
/// <remarks>
/// Throw it, or a subclass, from a handler or behavior when the message is written for the
/// caller: a validation verdict, a business rule, a "not found". Do not wrap an infrastructure
/// exception in it, because the wrapped message crosses the transport unchanged.
/// <see cref="HostLoomOptions.IncludeFaultDetails"/> is the blanket alternative for trusted
/// deployments where every exception may be reported in full.
/// </remarks>
public class RemoteFaultException : Exception
{
    public RemoteFaultException() { }

    public RemoteFaultException(string message)
        : base(message) { }

    public RemoteFaultException(string message, Exception innerException)
        : base(message, innerException) { }
}
