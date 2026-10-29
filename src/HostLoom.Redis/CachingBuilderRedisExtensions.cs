using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Redis;

/// <summary>Chooses Redis as the distributed tier of a HostLoom cache.</summary>
public static class CachingBuilderRedisExtensions
{
    /// <summary>
    /// Composes <see cref="RedisCacheStore"/> as the distributed tier and
    /// <see cref="RedisCacheInvalidationChannel"/> as the invalidation fan-out, over this cache
    /// profile's connection. Calling it on the locking builder with the same profile shares that
    /// connection. A serializer is still required; see <c>UseSystemTextJson</c>.
    /// The store uses that connection even when an <c>IConnectionMultiplexer</c> is also registered.
    /// </summary>
    /// <exception cref="InvalidOperationException">A store was already chosen.</exception>
    public static CachingBuilder UseRedis(
        this CachingBuilder builder,
        Action<RedisOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        var profile = builder.Profile;
        var existingStore = builder.Services.FirstOrDefault(descriptor =>
            descriptor.ServiceType == typeof(RedisCacheStore)
            && (
                profile is null
                    ? !descriptor.IsKeyedService
                    : descriptor.IsKeyedService && Equals(descriptor.ServiceKey, profile)
            )
        );
        builder.UseStore<RedisCacheStore>("Redis");
        if (existingStore is null)
        {
            // Both public constructors can be resolvable when the application registers a multiplexer.
            if (profile is null)
            {
                builder.Services.Replace(
                    ServiceDescriptor.Singleton(static provider => new RedisCacheStore(
                        provider.GetRequiredService<RedisConnection>()
                    ))
                );
            }
            else
            {
                builder.Services.Replace(
                    ServiceDescriptor.KeyedSingleton<RedisCacheStore>(
                        profile,
                        static (provider, key) =>
                            new RedisCacheStore(
                                provider.GetRequiredKeyedService<RedisConnection>(key)
                            )
                    )
                );
            }
        }
        RedisRegistration.AddConnection(builder.Services, profile, "caching", configure);
        if (profile is null)
        {
            builder.Services.TryAddSingleton<ICacheInvalidationChannel>(
                static provider => new RedisCacheInvalidationChannel(
                    provider.GetRequiredService<RedisConnection>(),
                    provider.GetRequiredService<IOptions<CachingOptions>>().Value,
                    provider
                        .GetService<ILoggerFactory>()
                        ?.CreateLogger<RedisCacheInvalidationChannel>()
                )
                {
                    EnforceSingleProfileTransport = true,
                }
            );
        }
        else
        {
            builder.Services.TryAddKeyedSingleton<ICacheInvalidationChannel>(
                profile,
                (provider, key) =>
                    new RedisCacheInvalidationChannel(
                        provider.GetRequiredKeyedService<RedisConnection>(key),
                        provider.GetRequiredService<IOptionsMonitor<CachingOptions>>().Get(profile),
                        provider
                            .GetService<ILoggerFactory>()
                            ?.CreateLogger<RedisCacheInvalidationChannel>()
                    )
                    {
                        Profile = profile,
                        EnforceSingleProfileTransport = true,
                    }
            );
        }
        return builder;
    }
}
