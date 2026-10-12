using HostLoom.Locking.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.Redis;

/// <summary>Chooses Redis as the provider of a HostLoom distributed lock.</summary>
public static class LockingBuilderRedisExtensions
{
    /// <summary>
    /// Composes <see cref="RedisLockProvider"/> over the one connection this process holds.
    /// Calling it on the caching builder as well shares that connection.
    /// The provider uses that connection even when an <c>IConnectionMultiplexer</c> is also registered.
    /// </summary>
    /// <exception cref="InvalidOperationException">A provider was already chosen.</exception>
    public static LockingBuilder UseRedis(
        this LockingBuilder builder,
        Action<RedisOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        var existingProvider = builder.Services.FirstOrDefault(static descriptor =>
            !descriptor.IsKeyedService && descriptor.ServiceType == typeof(RedisLockProvider)
        );
        builder.UseProvider<RedisLockProvider>("Redis");
        if (existingProvider is null)
        {
            // Both public constructors can be resolvable when the application registers a multiplexer.
            builder.Services.Replace(
                ServiceDescriptor.Singleton(static provider => new RedisLockProvider(
                    provider.GetRequiredService<RedisConnection>()
                ))
            );
        }
        RedisRegistration.AddConnection(builder.Services, configure);
        return builder;
    }
}
