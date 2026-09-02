using HostLoom.Locking.DependencyInjection;

namespace HostLoom.Redis;

/// <summary>Chooses Redis as the provider of a HostLoom distributed lock.</summary>
public static class LockingBuilderRedisExtensions
{
    /// <summary>
    /// Composes <see cref="RedisLockProvider"/> over the one connection this process holds.
    /// Calling it on the caching builder as well shares that connection.
    /// </summary>
    /// <exception cref="InvalidOperationException">A provider was already chosen.</exception>
    public static LockingBuilder UseRedis(
        this LockingBuilder builder,
        Action<RedisOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.UseProvider<RedisLockProvider>("Redis");
        RedisRegistration.AddConnection(builder.Services, configure);
        return builder;
    }
}
