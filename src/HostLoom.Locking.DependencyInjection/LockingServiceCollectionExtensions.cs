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
    /// provider chosen on the returned builder. Works without the messaging kernel. Repeated calls
    /// return a builder over the same registration; every registration is <c>TryAdd</c>, and the
    /// options are validated when the host starts.
    /// </summary>
    /// <param name="services">The container being composed.</param>
    /// <param name="configure">Configures <see cref="LockingOptions"/>; may be omitted when the options are bound elsewhere.</param>
    public static LockingBuilder AddHostLoomLocking(
        this IServiceCollection services,
        Action<LockingOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        var registration = FindRegistration(services);
        if (registration is null)
        {
            registration = new LockingRegistration();
            services.AddSingleton(registration);
        }

        var options = services.AddOptions<LockingOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IValidateOptions<LockingOptions>, LockingOptionsValidator>()
        );
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<IDistributedLock>(static provider =>
        {
            var options = provider.GetRequiredService<IOptions<LockingOptions>>().Value;
            return new DistributedLock(
                options,
                // Only resolved when enabled: single-instance mode needs no provider at all.
                options.Enabled
                    ? provider.GetRequiredService<ILockProvider>()
                    : null,
                provider.GetRequiredService<TimeProvider>(),
                provider.GetService<ILogger<DistributedLock>>()
            );
        });
        return new LockingBuilder(services, registration);
    }

    internal static LockingRegistration? FindRegistration(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (
                services[i].ServiceType == typeof(LockingRegistration)
                && services[i].ImplementationInstance is LockingRegistration registration
            )
            {
                return registration;
            }
        }

        return null;
    }
}
