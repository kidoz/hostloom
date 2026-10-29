using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;

namespace HostLoom.NodeLeasing.DependencyInjection;

/// <summary>Registers pools for one service collection.</summary>
public sealed class NodeLeasingBuilder
{
    private readonly NodeLeasingRegistration _registration;

    internal NodeLeasingBuilder(IServiceCollection services, NodeLeasingRegistration registration)
    {
        Services = services;
        _registration = registration;
    }

    /// <summary>The service collection receiving node-leasing registrations.</summary>
    public IServiceCollection Services { get; }

    /// <summary>
    /// Registers an elector for <paramref name="pool"/>, resolved as <see cref="INodeLeasing"/>
    /// and <see cref="NodeIdElector"/> keyed by the pool, over the <c>IDistributedLock</c>
    /// registered by <c>AddHostLoomLocking</c>. The options are named by the pool and validated
    /// when the host starts, including <c>NodeLeasing:Lease</c> against the lock's
    /// <c>Locking:MaxLease</c>. When exactly one pool is registered, the unkeyed
    /// <see cref="INodeLeasing"/> resolves it as well.
    /// </summary>
    /// <param name="pool">The unique node-ID pool.</param>
    /// <param name="configure">Configures this pool's leasing options.</param>
    /// <param name="lockProfile">The keyed lock profile, or null for the default unkeyed lock.</param>
    /// <exception cref="InvalidOperationException">
    /// The pool was already registered, or <see cref="INodeLeasing"/> keyed by the pool was
    /// already registered by something else, such as a test double; the message names it. A
    /// double for a pool that is not added here needs no elector: register it keyed by the pool
    /// and skip <c>AddPool</c>.
    /// </exception>
    public NodeLeasingBuilder AddPool(
        string pool,
        Action<NodeLeasingOptions>? configure = null,
        string? lockProfile = null
    )
    {
        NodeLeasingPool.Validate(pool);
        if (lockProfile is not null)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(lockProfile);
        }
        if (_registration.Pools.Contains(pool, StringComparer.Ordinal))
        {
            throw new InvalidOperationException(
                $"A node-leasing pool named '{pool}' is already registered. Pools are unique within "
                    + "a service collection."
            );
        }

        if (
            NodeLeasingServiceCollectionExtensions.FindForeign(
                Services,
                typeof(INodeLeasing),
                pool
            ) is
            { } foreign
        )
        {
            throw new InvalidOperationException(
                $"Node-leasing pool '{pool}' cannot be added: INodeLeasing keyed by '{pool}' is "
                    + $"already registered as {NodeLeasingServiceCollectionExtensions.Describe(foreign)}. "
                    + "Remove that registration, or keep it and skip AddPool for that pool."
            );
        }

        _registration.Pools.Add(pool);
        _registration.LockProfiles.Add(pool, lockProfile);
        var options = Services.AddOptions<NodeLeasingOptions>(pool);
        if (configure is not null)
        {
            options.Configure(configure);
        }

        options.ValidateOnStart();
        Services.AddKeyedSingleton(
            pool,
            (provider, key) =>
            {
                var pool = (string)key!;
                var locks = lockProfile is null
                    ? provider.GetRequiredService<Locking.IDistributedLock>()
                    : provider.GetKeyedService<Locking.IDistributedLock>(lockProfile)
                        ?? throw new InvalidOperationException(
                            $"Node-leasing pool '{pool}' requires lock profile '{lockProfile}', but that profile is not registered."
                        );
                return new NodeIdElector(
                    pool,
                    locks,
                    provider.GetRequiredService<IOptionsMonitor<NodeLeasingOptions>>().Get(pool),
                    provider.GetRequiredService<TimeProvider>(),
                    provider.GetService<ILogger<NodeIdElector>>()
                );
            }
        );
        Services.AddKeyedSingleton<INodeLeasing>(
            pool,
            static (provider, key) => provider.GetRequiredKeyedService<NodeIdElector>(key)
        );
        return this;
    }
}
