namespace HostLoom.Caching.DependencyInjection;

/// <summary>
/// What the builder has decided so far. One instance per service collection, found through its
/// descriptor, so repeated <c>AddHostLoomCaching</c> calls share it and a second store choice can
/// name the first.
/// </summary>
internal sealed class CachingRegistration
{
    public string? StoreName { get; set; }

    public string? SerializerName { get; set; }

    public bool WarmupRegistered { get; set; }

    public bool StoreReadinessRegistered { get; set; }
}
