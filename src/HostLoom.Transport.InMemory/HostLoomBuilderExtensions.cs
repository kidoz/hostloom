namespace HostLoom.Transport.InMemory;

public static class HostLoomBuilderExtensions
{
    public static HostLoomBuilder UseInMemory(this HostLoomBuilder builder)
    {
        ArgumentNullException.ThrowIfNull(builder);
        return builder.UseTransport<InMemoryRequestBroker>();
    }
}
