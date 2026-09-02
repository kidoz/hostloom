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
    /// warmups, and health checks. Works without the HostLoom messaging kernel. Repeated calls
    /// return a builder over the same registration; every registration is <c>TryAdd</c>.
    /// </summary>
    /// <param name="services">The container being composed.</param>
    /// <param name="configure">Sets <see cref="CachingOptions"/>; validated at startup.</param>
    public static CachingBuilder AddHostLoomCaching(
        this IServiceCollection services,
        Action<CachingOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        var registration = FindRegistration(services);
        if (registration is null)
        {
            registration = new CachingRegistration();
            services.AddSingleton(registration);
            services.AddOptions<CachingOptions>().ValidateOnStart();
            services.TryAddEnumerable(
                ServiceDescriptor.Singleton<
                    IValidateOptions<CachingOptions>,
                    CachingOptionsValidator
                >()
            );
            services.TryAddSingleton(TimeProvider.System);
            services.TryAddSingleton<ICache>(static provider =>
            {
                var options = provider.GetRequiredService<IOptions<CachingOptions>>().Value;
                // Validated here as well as at startup, so a provider built without a host still
                // fails on first resolution with the same messages rather than deep in a call.
                var validator = provider.GetRequiredService<
                    IEnumerable<IValidateOptions<CachingOptions>>
                >();
                foreach (var check in validator)
                {
                    var result = check.Validate(Options.DefaultName, options);
                    if (result.Failed)
                    {
                        throw new OptionsValidationException(
                            Options.DefaultName,
                            typeof(CachingOptions),
                            result.Failures
                        );
                    }
                }

                return new TieredCache(
                    options,
                    provider.GetService<IDistributedCacheStore>(),
                    provider.GetService<ICacheValueSerializer>(),
                    provider.GetService<ICacheInvalidationChannel>(),
                    provider.GetRequiredService<TimeProvider>(),
                    provider.GetService<ILoggerFactory>()?.CreateLogger<TieredCache>()
                );
            });
        }

        if (configure is not null)
        {
            services.Configure(configure);
        }

        return new CachingBuilder(services, registration);
    }

    private static CachingRegistration? FindRegistration(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (
                services[i].ServiceType == typeof(CachingRegistration)
                && services[i].ImplementationInstance is CachingRegistration registration
            )
            {
                return registration;
            }
        }

        return null;
    }
}
