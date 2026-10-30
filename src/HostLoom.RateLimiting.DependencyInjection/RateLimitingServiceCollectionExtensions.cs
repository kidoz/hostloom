using System.Threading.RateLimiting;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.RateLimiting.DependencyInjection;

/// <summary>Registers HostLoom rate limiting with Microsoft.Extensions.DependencyInjection.</summary>
public static class RateLimitingServiceCollectionExtensions
{
    /// <summary>Adds the default unkeyed limiter.</summary>
    public static RateLimitingBuilder AddHostLoomRateLimiting(
        this IServiceCollection services,
        Action<RateLimitingOptions>? configure = null
    ) => AddHostLoomRateLimitingCore(services, null, configure);

    /// <summary>Adds an independently configured limiter resolved by <paramref name="profile"/>.</summary>
    public static RateLimitingBuilder AddHostLoomRateLimiting(
        this IServiceCollection services,
        string profile,
        Action<RateLimitingOptions>? configure = null
    )
    {
        RateLimitKey.Validate(profile);
        return AddHostLoomRateLimitingCore(services, profile, configure);
    }

    private static RateLimitingBuilder AddHostLoomRateLimitingCore(
        IServiceCollection services,
        string? profile,
        Action<RateLimitingOptions>? configure
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        var registration = FindRegistration(services, profile);
        if (registration is null)
        {
            registration = new RateLimitingRegistration { Profile = profile };
            services.AddSingleton(registration);
            services.AddSingleton<IValidateOptions<RateLimitingOptions>>(
                new RateLimitingOptionsValidator()
            );
            services.TryAddSingleton(TimeProvider.System);
            if (profile is null)
            {
                services.TryAddSingleton<DistributedRateLimiter>(provider =>
                    CreateLimiter(provider, registration)
                );
                services.TryAddSingleton<RateLimiter>(provider =>
                    provider.GetRequiredService<DistributedRateLimiter>()
                );
            }
            else
            {
                services.TryAddKeyedSingleton<DistributedRateLimiter>(
                    profile,
                    (provider, _) => CreateLimiter(provider, registration)
                );
                services.TryAddKeyedSingleton<RateLimiter>(
                    profile,
                    (provider, key) => provider.GetRequiredKeyedService<DistributedRateLimiter>(key)
                );
            }
        }

        var options = services.AddOptions<RateLimitingOptions>(registration.OptionsName);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        return new RateLimitingBuilder(services, registration);
    }

    private static DistributedRateLimiter CreateLimiter(
        IServiceProvider provider,
        RateLimitingRegistration registration
    )
    {
        var profile = registration.Profile;
        var options = provider
            .GetRequiredService<IOptionsMonitor<RateLimitingOptions>>()
            .Get(registration.OptionsName);
        return new DistributedRateLimiter(
            options,
            profile ?? "default",
            RateLimitingProfile.GetRequired<IRateLimitStore>(provider, profile),
            provider.GetRequiredService<TimeProvider>(),
            provider.GetService<ILogger<DistributedRateLimiter>>()
        );
    }

    internal static RateLimitingRegistration? FindRegistration(
        IServiceCollection services,
        string? profile = null
    )
    {
        foreach (var descriptor in services)
        {
            if (
                descriptor.ServiceType == typeof(RateLimitingRegistration)
                && descriptor.ImplementationInstance is RateLimitingRegistration registration
                && registration.Profile == profile
            )
            {
                return registration;
            }
        }

        return null;
    }
}
