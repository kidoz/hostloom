namespace HostLoom.NodeLeasing.DependencyInjection;

/// <summary>Registration-time state shared by every <see cref="NodeLeasingBuilder"/> over one service collection: the pools registered so far.</summary>
internal sealed class NodeLeasingRegistration
{
    /// <summary>Pools registered, in order, so the hosted service starts each elector and a repeat is refused.</summary>
    public List<string> Pools { get; } = [];
}
