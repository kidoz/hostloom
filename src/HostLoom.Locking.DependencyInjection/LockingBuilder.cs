using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace HostLoom.Locking.DependencyInjection;

/// <summary>Chooses the lock provider and optional readiness for one service collection.</summary>
public sealed class LockingBuilder
{
    private readonly LockingRegistration _registration;

    internal LockingBuilder(IServiceCollection services, LockingRegistration registration)
    {
        Services = services;
        _registration = registration;
    }

    /// <summary>The service collection receiving locking registrations.</summary>
    public IServiceCollection Services { get; }

    /// <summary>The keyed-service identity of this lock, or null for the default unkeyed lock.</summary>
    public string? Profile => _registration.Profile;

    /// <summary>
    /// Uses the per-process <see cref="InMemoryLockProvider"/>: real lease expiry, owner tokens,
    /// extension, and lost-lease detection, coordinating within this process only. It registers
    /// no readiness contributor because there is no infrastructure to reach.
    /// </summary>
    public LockingBuilder UseInMemory() => UseProvider<InMemoryLockProvider>("InMemory");

    /// <summary>
    /// Uses <typeparamref name="TProvider"/> as the one provider for this service collection, the
    /// analogue of <c>HostLoomBuilder.UseTransport</c>. Backend packages call this from their own
    /// <c>Use*</c> extension. When the provider also implements
    /// <see cref="ILockProviderHealthProbe"/> the same instance serves readiness.
    /// </summary>
    /// <param name="name">How the choice is reported by the exactly-one rule and the probe.</param>
    /// <exception cref="InvalidOperationException">A provider was already chosen; the message names it.</exception>
    public LockingBuilder UseProvider<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TProvider
    >(string name)
        where TProvider : class, ILockProvider
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (_registration.ProviderName is { } existing)
        {
            var conflict = string.Equals(existing, name, StringComparison.Ordinal)
                ? $"{name} locking was configured more than once."
                : $"HostLoom locking already uses the '{existing}' provider, so '{name}' cannot be chosen as well.";
            throw new InvalidOperationException(
                conflict
                    + " Repeated AddHostLoomLocking() calls are allowed, but repeated provider selection is not."
                    + " Configure exactly one lock provider per IServiceCollection and share that registration between consumers."
            );
        }

        _registration.ProviderName = name;
        if (Profile is null)
        {
            Services.TryAddSingleton<TProvider>();
            Services.TryAddSingleton<ILockProvider>(static provider =>
                provider.GetRequiredService<TProvider>()
            );
            if (typeof(ILockProviderHealthProbe).IsAssignableFrom(typeof(TProvider)))
            {
                Services.TryAddSingleton<ILockProviderHealthProbe>(static provider =>
                    (ILockProviderHealthProbe)provider.GetRequiredService<TProvider>()
                );
            }
        }
        else
        {
            Services.TryAddKeyedSingleton<TProvider>(Profile);
            Services.TryAddKeyedSingleton<ILockProvider>(
                Profile,
                static (provider, key) => provider.GetRequiredKeyedService<TProvider>(key)
            );
            if (typeof(ILockProviderHealthProbe).IsAssignableFrom(typeof(TProvider)))
            {
                Services.TryAddKeyedSingleton<ILockProviderHealthProbe>(
                    Profile,
                    static (provider, key) =>
                        (ILockProviderHealthProbe)provider.GetRequiredKeyedService<TProvider>(key)
                );
            }
        }

        return this;
    }

    /// <summary>
    /// Registers a readiness check tagged <c>ready</c> that asks the provider's
    /// <see cref="ILockProviderHealthProbe"/> whether the backend is reachable. A provider without
    /// a probe reports healthy with a description saying so, because "cannot tell" is not
    /// "broken". Liveness is never touched: a lock backend outage must not restart the process.
    /// </summary>
    public LockingBuilder AddHealthChecks(string readinessName = "hostloom-lock-ready")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(readinessName);
        if (_registration.HealthChecksAdded)
        {
            return this;
        }

        _registration.HealthChecksAdded = true;
        if (readinessName == "hostloom-lock-ready")
        {
            readinessName = LockingProfile.HealthName(readinessName, Profile);
        }
        Services
            .AddHealthChecks()
            .Add(
                new HealthCheckRegistration(
                    readinessName,
                    provider => new LockProviderReadinessCheck(Profile, provider),
                    HealthStatus.Unhealthy,
                    ["ready"]
                )
            );
        return this;
    }
}
