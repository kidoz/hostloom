namespace HostLoom.Leadership.DependencyInjection;

/// <summary>Registration-time state shared by every <see cref="LeadershipBuilder"/> over one service collection: the roles registered so far.</summary>
internal sealed class LeadershipRegistration
{
    /// <summary>Roles registered, in order, so the hosted service starts each elector and a repeat is refused.</summary>
    public List<string> Roles { get; } = [];
}
