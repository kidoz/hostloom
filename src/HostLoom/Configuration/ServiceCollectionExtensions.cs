using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom;

public static class ServiceCollectionExtensions
{
    public static HostLoomBuilder AddHostLoom(this IServiceCollection services, Action<HostLoomOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);

        var configuration = services
            .FirstOrDefault(static descriptor => descriptor.ServiceType == typeof(HostLoomConfiguration))
            ?.ImplementationInstance as HostLoomConfiguration;

        if (configuration is null)
        {
            configuration = new HostLoomConfiguration();
            services.AddSingleton(configuration);
            services.AddSingleton<EndpointRuntimeState>();
            services.AddOptions<HostLoomOptions>();
            services.TryAddSingleton<IMessageSerializer, SystemTextJsonMessageSerializer>();
            services.TryAddSingleton<MessageDispatcher>();
            // Constructed by hand because HostLoomProbe keeps an internal constructor.
            services.TryAddSingleton(provider => new HostLoomProbe(provider.GetRequiredService<MessageDispatcher>()));
            services.TryAddEnumerable(ServiceDescriptor.Singleton<Microsoft.Extensions.Hosting.IHostedService, RequestEndpointHostedService>());
        }

        if (configure is not null)
        {
            services.Configure(configure);
        }

        return new HostLoomBuilder(services, configuration);
    }
}
