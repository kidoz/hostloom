namespace HostLoom;

public sealed class RemoteRequestException : Exception
{
    internal RemoteRequestException(RemoteFault fault)
        : base($"Remote handler failed with {fault.ErrorType}: {fault.Message}")
    {
        ErrorType = fault.ErrorType;
    }

    public string ErrorType { get; }
}
