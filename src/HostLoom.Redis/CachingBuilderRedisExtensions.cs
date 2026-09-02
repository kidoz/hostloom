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
    /// <see cref="RedisCacheInvalidationChannel"/> as the invalidation fan-out, over the one
    /// connection this process holds. Calling it on the locking builder as well shares that
    /// connection. A serializer is still required; see <c>UseSystemTextJson</c>.
    /// </summary>
    /// <exception cref="InvalidOperationException">A store was already chosen.</exception>
    public static CachingBuilder UseRedis(
        this CachingBuilder builder,
        Action<RedisOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.UseStore<RedisCacheStore>("Redis");
        RedisRegistration.AddConnection(builder.Services, configure);
        builder.Services.TryAddSingleton<ICacheInvalidationChannel>(
            static provider => new RedisCacheInvalidationChannel(
                provider.GetRequiredService<RedisConnection>(),
                provider.GetRequiredService<IOptions<CachingOptions>>().Value,
                provider.GetService<ILoggerFactory>()?.CreateLogger<RedisCacheInvalidationChannel>()
            )
        );
        return builder;
    }
}
