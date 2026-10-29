using HostLoom.Locking.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.Valkey;

/// <summary>Chooses Valkey as the provider of a HostLoom distributed lock.</summary>
public static class LockingBuilderValkeyExtensions
{
    /// <summary>
    /// Composes <see cref="ValkeyLockProvider"/> over this lock profile's connection.
    /// Calling it on the caching builder with the same profile shares that connection.
    /// </summary>
    /// <exception cref="InvalidOperationException">A provider was already chosen.</exception>
    public static LockingBuilder UseValkey(
        this LockingBuilder builder,
        Action<ValkeyOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        var profile = builder.Profile;
        builder.UseProvider<ValkeyLockProvider>("Valkey");
        if (profile is not null)
        {
            builder.Services.Replace(
                ServiceDescriptor.KeyedSingleton<ValkeyLockProvider>(
                    profile,
                    static (provider, key) =>
                        new ValkeyLockProvider(
                            provider.GetRequiredKeyedService<ValkeyConnection>(key)
                        )
                )
            );
        }
        ValkeyRegistration.AddConnection(builder.Services, profile, "locking", configure);
        return builder;
    }
}
