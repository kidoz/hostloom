using HostLoom.Locking.DependencyInjection;

namespace HostLoom.Valkey;

/// <summary>Chooses Valkey as the provider of a HostLoom distributed lock.</summary>
public static class LockingBuilderValkeyExtensions
{
    /// <summary>
    /// Composes <see cref="ValkeyLockProvider"/> over the one connection this process holds.
    /// Calling it on the caching builder as well shares that connection.
    /// </summary>
    /// <exception cref="InvalidOperationException">A provider was already chosen.</exception>
    public static LockingBuilder UseValkey(
        this LockingBuilder builder,
        Action<ValkeyOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.UseProvider<ValkeyLockProvider>("Valkey");
        ValkeyRegistration.AddConnection(builder.Services, configure);
        return builder;
    }
}
