using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.Mapping.DependencyInjection;

/// <summary>Registers HostLoom mapping with Microsoft.Extensions.DependencyInjection.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the scoped mapping dispatcher and configures explicit source/destination mappings.
    /// </summary>
    public static IServiceCollection AddHostLoomMapping(
        this IServiceCollection services,
        Action<MappingBuilder> configure
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        AddDispatcher(services);
        configure(new MappingBuilder(services));
        return services;
    }

    /// <summary>
    /// Adds the scoped mapping dispatcher and returns a builder for explicit mappings.
    /// </summary>
    public static MappingBuilder AddHostLoomMapping(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        AddDispatcher(services);
        return new MappingBuilder(services);
    }

    private static void AddDispatcher(IServiceCollection services) =>
        services.TryAddScoped<IMapper, ServiceProviderMapper>();
}
