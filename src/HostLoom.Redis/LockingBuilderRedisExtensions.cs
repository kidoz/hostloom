using HostLoom.Locking.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.Redis;

/// <summary>Chooses Redis as the provider of a HostLoom distributed lock.</summary>
public static class LockingBuilderRedisExtensions
{
    /// <summary>
    /// Composes <see cref="RedisLockProvider"/> over this lock profile's connection.
    /// Calling it on the caching builder with the same profile shares that connection.
    /// The provider uses that connection even when an <c>IConnectionMultiplexer</c> is also registered.
    /// </summary>
    /// <exception cref="InvalidOperationException">A provider was already chosen.</exception>
    public static LockingBuilder UseRedis(
        this LockingBuilder builder,
        Action<RedisOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        var profile = builder.Profile;
        var existingProvider = builder.Services.FirstOrDefault(descriptor =>
            descriptor.ServiceType == typeof(RedisLockProvider)
            && (
                profile is null
                    ? !descriptor.IsKeyedService
                    : descriptor.IsKeyedService && Equals(descriptor.ServiceKey, profile)
            )
        );
        builder.UseProvider<RedisLockProvider>("Redis");
        if (existingProvider is null)
        {
            // Both public constructors can be resolvable when the application registers a multiplexer.
            if (profile is null)
            {
                builder.Services.Replace(
                    ServiceDescriptor.Singleton(static provider => new RedisLockProvider(
                        provider.GetRequiredService<RedisConnection>()
                    ))
                );
            }
            else
            {
                builder.Services.Replace(
                    ServiceDescriptor.KeyedSingleton<RedisLockProvider>(
                        profile,
                        static (provider, key) =>
                            new RedisLockProvider(
                                provider.GetRequiredKeyedService<RedisConnection>(key)
                            )
                    )
                );
            }
        }
        RedisRegistration.AddConnection(builder.Services, profile, "locking", configure);
        return builder;
    }
}
