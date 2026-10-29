using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Locking.DependencyInjection;

/// <summary>Registers HostLoom locking with Microsoft.Extensions.DependencyInjection.</summary>
public static class LockingServiceCollectionExtensions
{
    /// <summary>
    /// Adds <see cref="IDistributedLock"/> composed from <see cref="LockingOptions"/> and the
    /// provider chosen on the returned builder. Repeated calls share the default registration.
    /// </summary>
    /// <param name="services">The container being composed.</param>
    /// <param name="configure">Configures <see cref="LockingOptions"/>; may be omitted when the options are bound elsewhere.</param>
    public static LockingBuilder AddHostLoomLocking(
        this IServiceCollection services,
        Action<LockingOptions>? configure = null
    ) => AddHostLoomLockingCore(services, null, configure);

    /// <summary>Adds an independently configured lock resolved by <paramref name="profile"/>.</summary>
    /// <param name="services">The container being composed.</param>
    /// <param name="profile">The keyed-service and named-options identity of this lock.</param>
    /// <param name="configure">Configures this profile's <see cref="LockingOptions"/>.</param>
    public static LockingBuilder AddHostLoomLocking(
        this IServiceCollection services,
        string profile,
        Action<LockingOptions>? configure = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(profile);
        return AddHostLoomLockingCore(services, profile, configure);
    }

    private static LockingBuilder AddHostLoomLockingCore(
        IServiceCollection services,
        string? profile,
        Action<LockingOptions>? configure
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        var registration = FindRegistration(services, profile);
        if (registration is null)
        {
            registration = new LockingRegistration { Profile = profile };
            services.AddSingleton(registration);
            services.AddSingleton<IValidateOptions<LockingOptions>>(
                new LockingOptionsValidator(registration)
            );
            services.TryAddSingleton(TimeProvider.System);
            if (profile is null)
            {
                services.TryAddSingleton<IDistributedLock>(provider =>
                    CreateLock(provider, registration)
                );
            }
            else
            {
                services.TryAddKeyedSingleton<IDistributedLock>(
                    profile,
                    (provider, _) => CreateLock(provider, registration)
                );
            }
        }

        var options = services.AddOptions<LockingOptions>(registration.OptionsName);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        return new LockingBuilder(services, registration);
    }

    private static DistributedLock CreateLock(
        IServiceProvider provider,
        LockingRegistration registration
    )
    {
        var profile = registration.Profile;
        var options = provider
            .GetRequiredService<IOptionsMonitor<LockingOptions>>()
            .Get(registration.OptionsName);
        return new DistributedLock(
            options,
            options.Enabled ? LockingProfile.GetRequired<ILockProvider>(provider, profile) : null,
            provider.GetRequiredService<TimeProvider>(),
            provider.GetService<ILogger<DistributedLock>>()
        );
    }

    internal static LockingRegistration? FindRegistration(
        IServiceCollection services,
        string? profile = null
    )
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (
                services[i].ServiceType == typeof(LockingRegistration)
                && services[i].ImplementationInstance is LockingRegistration registration
                && registration.Profile == profile
            )
            {
                return registration;
            }
        }

        return null;
    }
}
