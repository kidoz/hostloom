using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;

namespace HostLoom.Leadership.DependencyInjection;

/// <summary>Registers HostLoom leader election with Microsoft.Extensions.DependencyInjection.</summary>
public static class LeadershipServiceCollectionExtensions
{
    /// <summary>
    /// Adds leader election: one <see cref="LeaderElector"/> per role registered on the returned
    /// builder, keyed by role, plus the hosted service that starts and stops them with the host.
    /// Needs the lock registered by <c>AddHostLoomLocking</c>. Repeated calls return a builder
    /// over the same registration; every registration is <c>TryAdd</c>.
    /// </summary>
    /// <exception cref="InvalidOperationException">
    /// <see cref="ILeadership"/> was already registered by something else, such as a test double
    /// left in the composition root; it would have taken precedence over the electors. To stand
    /// in for them deliberately, register the double after this call.
    /// </exception>
    public static LeadershipBuilder AddHostLoomLeadership(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        var registration = FindRegistration(services);
        if (registration is null)
        {
            if (FindForeign(services, typeof(ILeadership), key: null) is { } foreign)
            {
                throw new InvalidOperationException(
                    "HostLoom leadership cannot be added: ILeadership is already registered as "
                        + $"{Describe(foreign)}, which would have taken precedence over the "
                        + "electors. Remove that registration, or register it after "
                        + "AddHostLoomLeadership to replace them deliberately."
                );
            }

            registration = new LeadershipRegistration();
            services.AddSingleton(registration);
        }

        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<
                IValidateOptions<LeadershipOptions>,
                LeadershipOptionsValidator
            >()
        );
        services.TryAddSingleton(TimeProvider.System);
        services.TryAddSingleton<ILeadership>(provider =>
        {
            var roles = provider.GetRequiredService<LeadershipRegistration>().Roles;
            return roles.Count == 1
                ? provider.GetRequiredKeyedService<ILeadership>(roles[0])
                : throw new InvalidOperationException(
                    roles.Count == 0
                        ? "No leadership role is registered. Call AddRole(role) on the builder returned by AddHostLoomLeadership."
                        : $"Several leadership roles are registered ({string.Join(", ", roles)}); resolve ILeadership keyed by role, for example with [FromKeyedServices(\"{roles[0]}\")]."
                );
        });
        services.TryAddEnumerable(
            ServiceDescriptor.Singleton<IHostedService, LeadershipHostedService>()
        );
        return new LeadershipBuilder(services, registration);
    }

    internal static LeadershipRegistration? FindRegistration(IServiceCollection services)
    {
        for (var i = 0; i < services.Count; i++)
        {
            if (
                services[i].ServiceType == typeof(LeadershipRegistration)
                && services[i].ImplementationInstance is LeadershipRegistration registration
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
