using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace HostLoom.NodeLeasing.DependencyInjection;

/// <summary>Registers HostLoom node-ID leasing with Microsoft.Extensions.DependencyInjection.</summary>
public static class NodeLeasingServiceCollectionExtensions
{
    /// <summary>
    /// Adds node-ID leasing: one <see cref="NodeIdElector"/> per pool registered on the returned
    /// builder, keyed by pool, plus the hosted service that starts and stops them with the host.
    /// Needs the lock registered by <c>AddHostLoomLocking</c>. Repeated calls return a builder
    /// over the same registration; every registration is <c>TryAdd</c>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <see cref="INodeLeasing"/> was already registered by something else, such as a test double
    /// left in the composition root; it would have taken precedence over the electors. To stand
    /// in for them deliberately, register the double after this call.
    /// </exception>
    public static NodeLeasingBuilder AddHostLoomNodeLeasing(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        var registration = FindRegistration(services);
        if (registration is null)
        {
            if (FindForeign(services, typeof(INodeLeasing), key: null) is { } foreign)
            {
                throw new InvalidOperationException(
                    "HostLoom node leasing cannot be added: INodeLeasing is already registered as "
                        + $"{Describe(foreign)}, which would have taken precedence over the "
                        + "electors. Remove that registration, or register it after "
                        + "AddHostLoomNodeLeasing to replace them deliberately."
                );
            }

            registration = new NodeLeasingRegistration();
            services.AddSingleton(registration);
        }

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<
                IValidateOptions<NodeLeasingOptions>,
                NodeLeasingOptionsValidator
            >()
        );
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<INodeLeasing>(provider =>
        {
            var pools = provider.GetRequiredService<NodeLeasingRegistration>().Pools;
            return pools.Count == 1
                ? provider.GetRequiredKeyedService<INodeLeasing>(pools[0])
                : throw new InvalidOperationException(
                    pools.Count == 0
                        ? "No node-leasing pool is registered. Call AddPool(pool) on the builder returned by AddHostLoomNodeLeasing."
                        : $"Several node-leasing pools are registered ({string.Join(", ", pools)}); resolve INodeLeasing keyed by pool, for example with [FromKeyedServices(\"{pools[0]}\")]."
                );
        });
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, NodeLeasingHostedService>()
        );
        return new NodeLeasingBuilder(services, registration);
    }

    internal static NodeLeasingRegistration? FindRegistration(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (
                services[i].ServiceType == typeof(NodeLeasingRegistration)
                && services[i].ImplementationInstance is NodeLeasingRegistration registration
            )
            {
                return registration;
            }
        }

        return null;
    }

    /// <summary>
    /// The registration of <paramref name="serviceType"/> under <paramref name="key"/> (unkeyed
    /// when <see langword="null"/>) that something other than this package made, or
    /// <see langword="null"/>.
    /// </summary>
    internal static ServiceDescriptor? FindForeign(
        IServiceCollection services,
        Type serviceType,
        string? key
    )
    {
        for (var i = 0; i < services.Count; i++)
        {
            var descriptor = services[i];
            if (descriptor.ServiceType != serviceType)
            {
                continue;
            }

            if (key is null ? !descriptor.IsKeyedService : Equals(descriptor.ServiceKey, key))
            {
                return descriptor;
            }
        }

        return null;
    }

    internal static string Describe(ServiceDescriptor descriptor)
    {
        // The unkeyed members throw on a keyed descriptor, and the other way round.
        var type = descriptor.IsKeyedService
            ? descriptor.KeyedImplementationType
                ?? descriptor.KeyedImplementationInstance?.GetType()
            : descriptor.ImplementationType ?? descriptor.ImplementationInstance?.GetType();
        var instance = descriptor.IsKeyedService
            ? descriptor.KeyedImplementationInstance
            : descriptor.ImplementationInstance;
        return type is null
            ? "a factory registration"
            : (type.FullName ?? type.Name) + (instance is null ? "" : " (an instance)");
    }
}
