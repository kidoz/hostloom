namespace HostLoom;

public sealed class RequestTimeoutException : TimeoutException
{
    public RequestTimeoutException(
        RequestAddress address,
        TimeSpan timeout,
        Exception? innerException = null
    )
        : base($"No response was received from '{address}' within {timeout}.", innerException)
    {
        Address = address;
        Timeout = timeout;
    }

    public RequestAddress Address { get; }

    public TimeSpan Timeout { get; }
}
