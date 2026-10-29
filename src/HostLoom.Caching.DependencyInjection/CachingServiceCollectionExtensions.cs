using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Caching.DependencyInjection;

/// <summary>Registers HostLoom caching with Microsoft.Extensions.DependencyInjection.</summary>
public static class CachingServiceCollectionExtensions
{
    /// <summary>
    /// Registers <see cref="ICache"/> and returns a builder to choose the store, the serializer,
    /// warmups, and health checks. Repeated calls return a builder over the same default registration.
    /// </summary>
    /// <param name="services">The container being composed.</param>
    /// <param name="configure">Sets <see cref="CachingOptions"/>; validated at startup.</param>
    public static CachingBuilder AddHostLoomCaching(
        this IServiceCollection services,
        Action<CachingOptions>? configure = null
    ) => AddHostLoomCachingCore(services, null, configure);

    /// <summary>Registers an independently configured cache resolved by <paramref name="profile"/>.</summary>
    /// <param name="services">The container being composed.</param>
    /// <param name="profile">The keyed-service and named-options identity of this cache.</param>
    /// <param name="configure">Sets this profile's <see cref="CachingOptions"/>; validated at startup.</param>
    public static CachingBuilder AddHostLoomCaching(
        this IServiceCollection services,
        string profile,
        Action<CachingOptions>? configure = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profile);
        return AddHostLoomCachingCore(services, profile, configure);
    }

    private static CachingBuilder AddHostLoomCachingCore(
        IServiceCollection services,
        string? profile,
        Action<CachingOptions>? configure
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        var registration = FindRegistration(services, profile);
        if (registration is null)
        {
            registration = new CachingRegistration { Profile = profile };
            services.AddSingleton(registration);
            services.AddSingleton<IValidateOptions<CachingOptions>>(
                new CachingOptionsValidator(registration)
            );
            services.TryAddSingleton(TimeProvider.System);
            if (profile is null)
            {
                services.TryAddSingleton<ICache>(provider => CreateCache(provider, registration));
            }
            else
            {
                services.TryAddKeyedSingleton<ICache>(
                    profile,
                    (provider, _) => CreateCache(provider, registration)
                );
            }
        }

        var options = services.AddOptions<CachingOptions>(registration.OptionsName);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        return new CachingBuilder(services, registration);
    }

    private static TieredCache CreateCache(
        IServiceProvider provider,
        CachingRegistration registration
    )
    {
        var profile = registration.Profile;
        var options = provider
            .GetRequiredService<IOptionsMonitor<CachingOptions>>()
            .Get(registration.OptionsName);
        var validators = provider.GetRequiredService<
            IEnumerable<IValidateOptions<CachingOptions>>
        >();
        foreach (var check in validators)
        {
            var result = check.Validate(registration.OptionsName, options);
            if (result.Failed)
            {
                throw new OptionsValidationException(
                    registration.OptionsName,
                    typeof(CachingOptions),
                    result.Failures
                );
            }
        }

        return new TieredCache(
            options,
            CachingProfile.Get<IDistributedCacheStore>(provider, profile),
            CachingProfile.Get<ICacheValueSerializer>(provider, profile),
            CachingProfile.Get<ICacheInvalidationChannel>(provider, profile),
            provider.GetRequiredService<TimeProvider>(),
            provider.GetService<ILoggerFactory>()?.CreateLogger<TieredCache>()
        );
    }

    private static CachingRegistration? FindRegistration(
        IServiceCollection services,
        string? profile
    )
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (
                services[i].ServiceType == typeof(CachingRegistration)
                && services[i].ImplementationInstance is CachingRegistration registration
                && registration.Profile == profile
            )
            {
                return registration;
            }
        }

        return null;
    }
}
