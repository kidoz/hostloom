using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Scheduling.DependencyInjection;

/// <summary>Registers HostLoom scheduling with Microsoft.Extensions.DependencyInjection.</summary>
public static class SchedulingServiceCollectionExtensions
{
    /// <summary>
    /// Adds a <see cref="Scheduler"/> composed from <see cref="SchedulingOptions"/>, every
    /// schedule registered on the returned builder, and the guard chosen there, plus the hosted
    /// service that starts and stops it with the host. Works without the messaging kernel.
    /// Repeated calls return a builder over the same registration; every registration is
    /// <c>TryAdd</c>, and the options are validated when the host starts.
    /// </summary>
    /// <param name="services">The container being composed.</param>
    /// <param name="configure">Configures <see cref="SchedulingOptions"/>; may be omitted when the options are bound elsewhere.</param>
    public static SchedulingBuilder AddHostLoomScheduling(
        this IServiceCollection services,
        Action<SchedulingOptions>? configure = null
    )
    {
        ArgumentNullException.ThrowIfNull(services);

        var registration = FindRegistration(services);
        if (registration is null)
        {
            registration = new SchedulingRegistration();
            services.AddSingleton(registration);
        }

        var options = services.AddOptions<SchedulingOptions>();
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<
                IValidateOptions<SchedulingOptions>,
                SchedulingOptionsValidator
            >()
        );
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton(static provider => new Scheduler(
            provider.GetRequiredService<IOptions<SchedulingOptions>>().Value,
            provider.GetServices<ScheduleDefinition>(),
            provider.GetService<IScheduleGuard>(),
            provider.GetRequiredService<TimeProvider>(),
            provider.GetService<ILogger<Scheduler>>()
        ));
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, SchedulerHostedService>()
        );
        return new SchedulingBuilder(services, registration);
    }

    internal static SchedulingRegistration? FindRegistration(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (
                services[i].ServiceType == typeof(SchedulingRegistration)
                && services[i].ImplementationInstance is SchedulingRegistration registration
            )
            {
                return registration;
            }
        }

        return null;
    }
}
