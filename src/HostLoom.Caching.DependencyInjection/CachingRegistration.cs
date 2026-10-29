namespace HostLoom.Caching.DependencyInjection;

/// <summary>
/// What the builder has decided so far. One instance per profile in a service collection, found
/// through its descriptor, so repeated registration of one profile shares its state and a second
/// store choice can name the first.
/// </summary>
internal sealed class CachingRegistration
{
    public string? Profile { get; init; }

    public string OptionsName => CachingProfile.OptionsName(Profile);

    public string? StoreName { get; set; }

    public string? SerializerName { get; set; }

    public bool WarmupRegistered { get; set; }

    public bool StoreReadinessRegistered { get; set; }
}
