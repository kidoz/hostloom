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
    public static LeadershipBuilder AddHostLoomLeadership(this IServiceCollection services)
    {
        ArgumentNullException.ThrowIfNull(services);
        var registration = FindRegistration(services);
        if (registration is null)
        {
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
}
