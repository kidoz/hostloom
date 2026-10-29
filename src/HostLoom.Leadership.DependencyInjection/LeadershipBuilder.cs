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
    /// starts, including the resolved key against <c>Locking:MaxKeyLength</c> and
    /// <c>Leadership:Lease</c> against the lock's <c>Locking:MaxLease</c>. When
    /// exactly one role is registered, the unkeyed <see cref="ILeadership"/> resolves it as well.
    /// </summary>
    /// <param name="role">The unique leadership role.</param>
    /// <param name="configure">Configures this role's election options.</param>
    /// <param name="lockProfile">The keyed lock profile, or null for the default unkeyed lock.</param>
    /// <exception cref="InvalidOperationException">
    /// The role was already registered, or <see cref="ILeadership"/> keyed by the role was already
    /// registered by something else, such as a test double; the message names it. A double for a
    /// role that is not added here needs no elector: register it keyed by the role and skip
    /// <c>AddRole</c>.
    /// </exception>
    public LeadershipBuilder AddRole(
        string role,
        Action<LeadershipOptions>? configure = null,
        string? lockProfile = null
    )
    {
        LeadershipRole.Validate(role);
        if (lockProfile is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(lockProfile);
        }
        if (_registration.Roles.Contains(role, StringComparer.Ordinal))
        {
            throw new InvalidOperationException(
                $"A leadership role named '{role}' is already registered. Roles are unique within "
                    + "a service collection."
            );
        }

        if (
            LeadershipServiceCollectionExtensions.FindForeign(
                Services,
                typeof(ILeadership),
                role
            ) is
            { } foreign
        )
        {
            throw new InvalidOperationException(
                $"Leadership role '{role}' cannot be added: ILeadership keyed by '{role}' is "
                    + $"already registered as {LeadershipServiceCollectionExtensions.Describe(foreign)}. "
                    + "Remove that registration, or keep it and skip AddRole for that role."
            );
        }

        _registration.Roles.Add(role);
        _registration.LockProfiles.Add(role, lockProfile);
        var options = Services.AddOptions<LeadershipOptions>(role);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        Services.AddKeyedSingleton(
            role,
            (provider, key) =>
            {
                var role = (string)key!;
                var locks = lockProfile is null
                    ? provider.GetRequiredService<Locking.IDistributedLock>()
                    : provider.GetKeyedService<Locking.IDistributedLock>(lockProfile)
                        ?? throw new InvalidOperationException(
                            $"Leadership role '{role}' requires lock profile '{lockProfile}', but that profile is not registered."
                        );
                return new LeaderElector(
                    role,
                    locks,
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
