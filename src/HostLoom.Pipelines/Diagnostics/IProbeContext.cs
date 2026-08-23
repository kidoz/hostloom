namespace HostLoom.Pipelines;

public interface IProbeContext
{
    CancellationToken CancellationToken { get; }
    IProbeContext CreateScope(string name);
    void Set(string key, object? value);
}
