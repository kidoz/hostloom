using HostLoom.RateLimiting.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.Redis;

/// <summary>Chooses Redis as the store for a HostLoom rate-limiter profile.</summary>
public static class RateLimitingBuilderRedisExtensions
{
    /// <summary>Composes <see cref="RedisRateLimitStore"/> over this profile's shared connection.</summary>
    public static RateLimitingBuilder UseRedis(
        this RateLimitingBuilder builder,
        Action<RedisOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        var profile = builder.Profile;
        var existingStore = builder.Services.FirstOrDefault(descriptor =>
            descriptor.ServiceType == typeof(RedisRateLimitStore)
            && (
                profile is null
                    ? !descriptor.IsKeyedService
                    : descriptor.IsKeyedService && Equals(descriptor.ServiceKey, profile)
            )
        );
        builder.UseStore<RedisRateLimitStore>("Redis");
        if (existingStore is null)
        {
            if (profile is null)
            {
                builder.Services.Replace(
                    ServiceDescriptor.Singleton(static provider => new RedisRateLimitStore(
                        provider.GetRequiredService<RedisConnection>()
                    ))
                );
            }
            else
            {
                builder.Services.Replace(
                    ServiceDescriptor.KeyedSingleton<RedisRateLimitStore>(
                        profile,
                        static (provider, key) =>
                            new RedisRateLimitStore(
                                provider.GetRequiredKeyedService<RedisConnection>(key)
                            )
                    )
                );
            }
        }

        RedisRegistration.AddConnection(builder.Services, profile, "rate-limiting", configure);
        return builder;
    }
}
