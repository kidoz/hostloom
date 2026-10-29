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
    /// <see cref="ValkeyCacheInvalidationChannel"/> as the invalidation fan-out, over this cache
    /// profile's connection. Calling it on the locking builder with the same profile shares that
    /// connection. A serializer is still required; see <c>UseSystemTextJson</c>.
    /// </summary>
    /// <exception cref="InvalidOperationException">A store was already chosen.</exception>
    public static CachingBuilder UseValkey(
        this CachingBuilder builder,
        Action<ValkeyOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        var profile = builder.Profile;
        builder.UseStore<ValkeyCacheStore>("Valkey");
        if (profile is not null)
        {
            builder.Services.Replace(
                ServiceDescriptor.KeyedSingleton<ValkeyCacheStore>(
                    profile,
                    static (provider, key) =>
                        new ValkeyCacheStore(
                            provider.GetRequiredKeyedService<ValkeyConnection>(key)
                        )
                )
            );
        }
        ValkeyRegistration.AddConnection(builder.Services, profile, "caching", configure);
        builder
            .Services.AddOptions<CachingOptions>(profile ?? Options.DefaultName)
            .Validate(
                static options => options.Invalidation.Mode == CacheInvalidationMode.Auto,
                "HostLoom.Valkey supports Auto with explicit-channel invalidation only."
            )
            .ValidateOnStart();
        if (profile is null)
        {
            builder.Services.TryAddSingleton<ICacheInvalidationChannel>(
                static provider => new ValkeyCacheInvalidationChannel(
                    provider.GetRequiredService<ValkeyConnection>(),
                    provider.GetRequiredService<IOptions<CachingOptions>>().Value,
                    provider
                        .GetService<ILoggerFactory>()
                        ?.CreateLogger<ValkeyCacheInvalidationChannel>()
                )
            );
        }
        else
        {
            builder.Services.TryAddKeyedSingleton<ICacheInvalidationChannel>(
                profile,
                (provider, key) =>
                    new ValkeyCacheInvalidationChannel(
                        provider.GetRequiredKeyedService<ValkeyConnection>(key),
                        provider.GetRequiredService<IOptionsMonitor<CachingOptions>>().Get(profile),
                        provider
                            .GetService<ILoggerFactory>()
                            ?.CreateLogger<ValkeyCacheInvalidationChannel>()
                    )
            );
        }
        return builder;
    }
}
