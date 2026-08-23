namespace HostLoom;

public sealed class HostLoomOptions
{
    public TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(30);
}
