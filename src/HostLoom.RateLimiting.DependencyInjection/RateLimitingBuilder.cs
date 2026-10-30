using System.Diagnostics.CodeAnalysis;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;

namespace HostLoom.RateLimiting.DependencyInjection;

/// <summary>Chooses the store and optional readiness for one rate-limiter profile.</summary>
public sealed class RateLimitingBuilder
{
    private readonly RateLimitingRegistration _registration;

    internal RateLimitingBuilder(IServiceCollection services, RateLimitingRegistration registration)
    {
        Services = services;
        _registration = registration;
    }

    /// <summary>The service collection receiving rate-limiting registrations.</summary>
    public IServiceCollection Services { get; }

    /// <summary>The keyed-service identity, or null for the default limiter.</summary>
    public string? Profile => _registration.Profile;

    /// <summary>Uses process-local token buckets.</summary>
    public RateLimitingBuilder UseInMemory() => UseStore<InMemoryRateLimitStore>("InMemory");

    /// <summary>Selects exactly one store for this profile.</summary>
    public RateLimitingBuilder UseStore<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TStore
    >(string name)
        where TStore : class, IRateLimitStore
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (_registration.StoreName is { } existing)
        {
            var conflict = string.Equals(existing, name, StringComparison.Ordinal)
                ? $"{name} rate limiting was configured more than once."
                : $"HostLoom rate limiting already uses the '{existing}' store, so '{name}' cannot be chosen as well.";
            throw new InvalidOperationException(
                conflict + " Configure exactly one store per rate-limiter profile."
            );
        }

        _registration.StoreName = name;
        if (Profile is null)
        {
            Services.TryAddSingleton<TStore>();
            Services.TryAddSingleton<IRateLimitStore>(provider =>
                provider.GetRequiredService<TStore>()
            );
            if (typeof(IRateLimitStoreHealthProbe).IsAssignableFrom(typeof(TStore)))
            {
                Services.TryAddSingleton<IRateLimitStoreHealthProbe>(provider =>
                    (IRateLimitStoreHealthProbe)provider.GetRequiredService<TStore>()
                );
            }
        }
        else
        {
            Services.TryAddKeyedSingleton<TStore>(Profile);
            Services.TryAddKeyedSingleton<IRateLimitStore>(
                Profile,
                (provider, key) => provider.GetRequiredKeyedService<TStore>(key)
            );
            if (typeof(IRateLimitStoreHealthProbe).IsAssignableFrom(typeof(TStore)))
            {
                Services.TryAddKeyedSingleton<IRateLimitStoreHealthProbe>(
                    Profile,
                    (provider, key) =>
                        (IRateLimitStoreHealthProbe)provider.GetRequiredKeyedService<TStore>(key)
                );
            }
        }

        return this;
    }

    /// <summary>Adds a backend readiness check tagged <c>ready</c>; liveness is never changed.</summary>
    public RateLimitingBuilder AddHealthChecks(string readinessName = "hostloom-ratelimit-ready")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(readinessName);
        if (_registration.HealthChecksAdded)
        {
            return this;
        }

        _registration.HealthChecksAdded = true;
        if (readinessName == "hostloom-ratelimit-ready")
        {
            readinessName = RateLimitingProfile.HealthName(readinessName, Profile);
        }

        Services
            .AddHealthChecks()
            .Add(
                new HealthCheckRegistration(
                    readinessName,
                    provider => new RateLimitStoreReadinessCheck(Profile, provider),
                    HealthStatus.Unhealthy,
                    ["ready"]
                )
            );
        return this;
    }
}
