using HostLoom.RateLimiting.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.Valkey;

/// <summary>Chooses standalone Valkey as the store for a HostLoom rate-limiter profile.</summary>
public static class RateLimitingBuilderValkeyExtensions
{
    /// <summary>Composes <see cref="ValkeyRateLimitStore"/> over this profile's shared connection.</summary>
    public static RateLimitingBuilder UseValkey(
        this RateLimitingBuilder builder,
        Action<ValkeyOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(builder);
        var profile = builder.Profile;
        builder.UseStore<ValkeyRateLimitStore>("Valkey");
        if (profile is not null)
        {
            builder.Services.Replace(
                ServiceDescriptor.KeyedSingleton<ValkeyRateLimitStore>(
                    profile,
                    static (provider, key) =>
                        new ValkeyRateLimitStore(
                            provider.GetRequiredKeyedService<ValkeyConnection>(key)
                        )
                )
            );
        }

        ValkeyRegistration.AddConnection(builder.Services, profile, "rate-limiting", configure);
        return builder;
    }
}
