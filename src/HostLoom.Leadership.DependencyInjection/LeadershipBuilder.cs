using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.Leadership.DependencyInjection;

/// <summary>Registers roles for one service collection.</summary>
public sealed class LeadershipBuilder
{
    private readonly LeadershipRegistration _registration;

    internal LeadershipBuilder(IServiceCollection services, LeadershipRegistration registration)
    {
        Services = services;
        _registration = registration;
    }

    /// <summary>The service collection receiving leadership registrations.</summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Registers an elector for <paramref name="role"/>, resolved as <see cref="ILeadership"/> and
    /// <see cref="LeaderElector"/> keyed by the role, over the <c>IDistributedLock</c> registered
    /// by <c>AddHostLoomLocking</c>. The options are named by the role and validated when the host
    /// starts. When exactly one role is registered, the unkeyed <see cref="ILeadership"/> resolves
    /// it as well.
    /// </summary>
    /// <exception cref="InvalidOperationException">The role was already registered.</exception>
    public LeadershipBuilder AddRole(string role, Action<LeadershipOptions>? configure = null)
    {
        LeadershipRole.Validate(role);
        if (_registration.Roles.Contains(role, StringComparer.Ordinal))
        {
            throw new InvalidOperationException(
                $"A leadership role named '{role}' is already registered. Roles are unique within "
                    + "a service collection."
            );
        }

        _registration.Roles.Add(role);
        var options = Services.AddOptions<LeadershipOptions>(role);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        Services.AddKeyedSingleton(
            role,
            static (provider, key) =>
            {
                var role = (string)key!;
                return new LeaderElector(
                    role,
                    provider.GetRequiredService<Locking.IDistributedLock>(),
                    provider.GetRequiredService<IOptionsMonitor<LeadershipOptions>>().Get(role),
                    provider.GetRequiredService<TimeProvider>(),
                    provider.GetService<ILogger<LeaderElector>>()
                );
            }
        );
        Services.AddKeyedSingleton<ILeadership>(
            role,
            static (provider, key) => provider.GetRequiredKeyedService<LeaderElector>(key)
        );
        return this;
    }
}
