using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Redis;

/// <summary>Registers Redis connections for consumers that share HostLoom profiles.</summary>
public static class RedisConnectionServiceCollectionExtensions
{
    /// <summary>Registers or attaches to the default or named Redis connection using the same settings and ownership rules as UseRedis.</summary>
    public static IServiceCollection AddHostLoomRedisConnection(
        this IServiceCollection services,
        string? profile = null,
        Action<RedisOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        if (profile is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(profile);
        }
        RedisRegistration.AddConnection(services, profile, "Redis connection", configure);
        return services;
    }
}
