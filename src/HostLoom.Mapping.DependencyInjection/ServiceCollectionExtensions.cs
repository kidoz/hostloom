using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;

namespace HostLoom.Mapping.DependencyInjection;

/// <summary>Registers HostLoom mapping with Microsoft.Extensions.DependencyInjection.</summary>
public static class ServiceCollectionExtensions
{
    /// <summary>
    /// Adds the mapping dispatcher and configures explicit source/destination mappings.
    /// </summary>
    /// <param name="services">The container being composed.</param>
    /// <param name="configure">Declares the mappings.</param>
    /// <param name="dispatcherLifetime">
    /// The lifetime of the non-generic <see cref="IMapper"/> dispatcher. Scoped is the default and
    /// the safe choice: it resolves each pair from the current scope, so a map class may take
    /// scoped dependencies and anything disposable is released with the scope.
    /// <see cref="ServiceLifetime.Singleton"/> lets an <c>IHostedService</c> take the dispatcher
    /// directly, and then every registered map must itself be registered
    /// <see cref="ServiceLifetime.Singleton"/> — a non-singleton map is rejected at registration.
    /// A singleton dispatcher resolves from the root provider, which retains every disposable it
    /// creates for the life of the process and captures any scoped dependency reached through the
    /// map. Prefer injecting a closed <see cref="IMapper{TSource, TDestination}"/> whose own graph
    /// is singleton-safe.
    /// </param>
    public static IServiceCollection AddHostLoomMapping(
        this IServiceCollection services,
        Action<MappingBuilder> configure,
        ServiceLifetime dispatcherLifetime = ServiceLifetime.Scoped
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(configure);

        configure(
            new MappingBuilder(
                services,
                AddDispatcher(services, dispatcherLifetime),
                dispatcherLifetime
            )
        );
        return services;
    }

    /// <summary>
    /// Adds the mapping dispatcher and returns a builder for explicit mappings.
    /// </summary>
    /// <param name="services">The container being composed.</param>
    /// <param name="dispatcherLifetime">
    /// The dispatcher's lifetime; see the other overload for what a singleton one requires.
    /// </param>
    public static MappingBuilder AddHostLoomMapping(
        this IServiceCollection services,
        ServiceLifetime dispatcherLifetime = ServiceLifetime.Scoped
    )
    {
        ArgumentNullException.ThrowIfNull(services);
        return new MappingBuilder(
            services,
            AddDispatcher(services, dispatcherLifetime),
            dispatcherLifetime
        );
    }

    /// <summary>
    /// The pairs registered so far, for a service that wants to assert its expectations at startup
    /// rather than discover a missing pair on the code path that first needs it.
    /// </summary>
    public static MappedPairRegistry GetMappedPairs(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        return FindRegistry(services) ?? new MappedPairRegistry();
    }

    private static MappedPairRegistry AddDispatcher(
        IServiceCollection services,
        ServiceLifetime dispatcherLifetime
    )
    {
        MappedPairRegistry? registry = FindRegistry(services);
        if (registry is null)
        {
            registry = new MappedPairRegistry();
            services.AddSingleton(registry);
        }

        // TryAdd, so repeated calls register the dispatcher once. A second call asking for a
        // different lifetime therefore does not change the first one, which is the same rule the
        // container applies to every other TryAdd registration.
        services.TryAdd(
            new ServiceDescriptor(
                typeof(IMapper),
                typeof(ServiceProviderMapper),
                dispatcherLifetime
            )
        );
        return registry;
    }

    private static MappedPairRegistry? FindRegistry(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (
                services[i].ServiceType == typeof(MappedPairRegistry)
                && services[i].ImplementationInstance is MappedPairRegistry registry
            )
            {
                return registry;
            }
        }

        return null;
    }
}
