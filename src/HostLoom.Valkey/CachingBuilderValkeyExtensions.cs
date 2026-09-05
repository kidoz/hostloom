using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Valkey;

/// <summary>Chooses Valkey as the distributed tier of a HostLoom cache.</summary>
public static class CachingBuilderValkeyExtensions
{
    /// <summary>
    /// Composes <see cref="ValkeyCacheStore"/> as the distributed tier and
    /// <see cref="ValkeyCacheInvalidationChannel"/> as the invalidation fan-out, over the one
    /// connection this process holds. Calling it on the locking builder as well shares that
    /// connection. A serializer is still required; see <c>UseSystemTextJson</c>.
    /// </summary>
    /// <exception cref="InvalidOperationException">A store was already chosen.</exception>
    public static CachingBuilder UseValkey(
        this CachingBuilder builder,
        Action<ValkeyOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        builder.UseStore<ValkeyCacheStore>("Valkey");
        ValkeyRegistration.AddConnection(builder.Services, configure);
        builder
            .Services.AddOptions<CachingOptions>()
            .Validate(
                static options => options.Invalidation.Mode == CacheInvalidationMode.Auto,
                "HostLoom.Valkey supports Auto with explicit-channel invalidation only."
            )
            .ValidateOnStart();
        builder.Services.TryAddSingleton<ICacheInvalidationChannel>(
            static provider => new ValkeyCacheInvalidationChannel(
                provider.GetRequiredService<ValkeyConnection>(),
                provider.GetRequiredService<IOptions<CachingOptions>>().Value,
                provider
                    .GetService<ILoggerFactory>()
                    ?.CreateLogger<ValkeyCacheInvalidationChannel>()
            )
        );
        return builder;
    }
}
