using System.Diagnostics.CodeAnalysis;
using System.Text.Json;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace HostLoom.Caching.DependencyInjection;

/// <summary>Chooses the store, the serializer, warmups, and health checks for the registered cache.</summary>
public sealed class CachingBuilder
{
    internal const string InMemoryStoreName = "InMemory";

    private readonly CachingRegistration _registration;

    internal CachingBuilder(IServiceCollection services, CachingRegistration registration)
    {
        Services = services;
        _registration = registration;
    }

    /// <summary>The service collection receiving cache registrations.</summary>
    public IServiceCollection Services { get; }

    /// <summary>The keyed-service identity of this cache, or null for the default unkeyed cache.</summary>
    public string? Profile => _registration.Profile;

    /// <summary>
    /// Composes the in-process tier as the only tier: no serializer, no cross-instance
    /// invalidation, staleness across instances bounded by expiry.
    /// </summary>
    /// <exception cref="InvalidOperationException">A store was already chosen.</exception>
    public CachingBuilder UseInMemory()
    {
        Choose(InMemoryStoreName);
        return this;
    }

    /// <summary>
    /// Composes <typeparamref name="TStore"/> as the distributed tier. This is the primitive
    /// backend packages call from their own <c>Use*</c> method; a store that also implements
    /// <see cref="ICacheInvalidationChannel"/> or <see cref="ICacheStoreHealthProbe"/> is
    /// registered for those contracts too.
    /// </summary>
    /// <param name="name">Short name reported by the probe and by the exactly-one rule.</param>
    /// <exception cref="InvalidOperationException">A store was already chosen.</exception>
    public CachingBuilder UseStore<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TStore
    >(string name)
        where TStore : class, IDistributedCacheStore
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(name);
        if (string.Equals(name, InMemoryStoreName, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                $"'{InMemoryStoreName}' names the in-process-only composition; call UseInMemory() for it.",
                nameof(name)
            );
        }

        Choose(name);
        if (Profile is null)
        {
            Services.TryAddSingleton<TStore>();
            Services.TryAddSingleton<IDistributedCacheStore>(static provider =>
                provider.GetRequiredService<TStore>()
            );
            if (typeof(ICacheInvalidationChannel).IsAssignableFrom(typeof(TStore)))
            {
                Services.TryAddSingleton(static provider =>
                    (ICacheInvalidationChannel)provider.GetRequiredService<TStore>()
                );
            }

            if (typeof(ICacheStoreHealthProbe).IsAssignableFrom(typeof(TStore)))
            {
                Services.TryAddSingleton(static provider =>
                    (ICacheStoreHealthProbe)provider.GetRequiredService<TStore>()
                );
            }
        }
        else
        {
            Services.TryAddKeyedSingleton<TStore>(Profile);
            Services.TryAddKeyedSingleton<IDistributedCacheStore>(
                Profile,
                static (provider, key) => provider.GetRequiredKeyedService<TStore>(key)
            );
            if (typeof(ICacheInvalidationChannel).IsAssignableFrom(typeof(TStore)))
            {
                Services.TryAddKeyedSingleton<ICacheInvalidationChannel>(
                    Profile,
                    static (provider, key) =>
                        (ICacheInvalidationChannel)provider.GetRequiredKeyedService<TStore>(key)
                );
            }

            if (typeof(ICacheStoreHealthProbe).IsAssignableFrom(typeof(TStore)))
            {
                Services.TryAddKeyedSingleton<ICacheStoreHealthProbe>(
                    Profile,
                    static (provider, key) =>
                        (ICacheStoreHealthProbe)provider.GetRequiredKeyedService<TStore>(key)
                );
            }
        }

        return this;
    }

    /// <summary>
    /// Serializes payloads with <c>System.Text.Json</c> over <paramref name="options"/>, whose
    /// <see cref="JsonSerializerOptions.TypeInfoResolver"/> must be set: a source-generated
    /// <c>JsonSerializerContext</c> for a trimmed or Native AOT publish. Replaces any serializer
    /// chosen earlier.
    /// </summary>
    /// <exception cref="ArgumentException">The options have no type-info resolver.</exception>
    public CachingBuilder UseSystemTextJson(JsonSerializerOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        var serializer = new SystemTextJsonCacheValueSerializer(options);
        return UseSerializer(serializer, nameof(SystemTextJsonCacheValueSerializer));
    }

    /// <summary>Serializes payloads with <typeparamref name="TSerializer"/>. Replaces any serializer chosen earlier.</summary>
    public CachingBuilder UseSerializer<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TSerializer
    >()
        where TSerializer : class, ICacheValueSerializer
    {
        RemoveSerializers();
        if (Profile is null)
        {
            Services.AddSingleton<ICacheValueSerializer, TSerializer>();
        }
        else
        {
            Services.AddKeyedSingleton<ICacheValueSerializer, TSerializer>(Profile);
        }

        _registration.SerializerName = typeof(TSerializer).Name;
        return this;
    }

    /// <summary>
    /// Serializes payloads with reflection-based <c>System.Text.Json</c> contracts. Explicitly not
    /// compatible with trimming or Native AOT; prefer <see cref="UseSystemTextJson"/> with a
    /// source-generated context.
    /// </summary>
    /// <param name="options">Settings to copy; the platform profile when null.</param>
    [RequiresUnreferencedCode(
        "Reflection-based JSON contracts are not compatible with trimming. Use UseSystemTextJson with a JsonSerializerContext."
    )]
    [RequiresDynamicCode(
        "Reflection-based JSON contracts are not compatible with Native AOT. Use UseSystemTextJson with a JsonSerializerContext."
    )]
    public CachingBuilder UseReflectionSerialization(JsonSerializerOptions? options = null) =>
        UseSerializer(
            SystemTextJsonCacheValueSerializer.CreateReflectionBased(options),
            nameof(SystemTextJsonCacheValueSerializer) + " (reflection)"
        );

    /// <summary>
    /// Registers <typeparamref name="TWarmup"/> to run once after the host starts, in the
    /// background, together with the readiness contributor that
    /// <see cref="CacheWarmupOptions.BlocksReadiness"/> controls.
    /// </summary>
    public CachingBuilder AddWarmup<
        [DynamicallyAccessedMembers(DynamicallyAccessedMemberTypes.PublicConstructors)] TWarmup
    >()
        where TWarmup : class, ICacheWarmup
    {
        if (Profile is null)
        {
            Services.TryAddEnumerable(ServiceDescriptor.Singleton<ICacheWarmup, TWarmup>());
        }
        else if (
            !Services.Any(descriptor =>
                descriptor.ServiceType == typeof(ICacheWarmup)
                && descriptor.IsKeyedService
                && Equals(descriptor.ServiceKey, Profile)
                && descriptor.KeyedImplementationType == typeof(TWarmup)
            )
        )
        {
            Services.AddKeyedSingleton<ICacheWarmup, TWarmup>(Profile);
        }

        if (!_registration.WarmupRegistered)
        {
            _registration.WarmupRegistered = true;
            if (Profile is null)
            {
                Services.TryAddSingleton<CacheWarmupState>();
            }
            else
            {
                Services.TryAddKeyedSingleton<CacheWarmupState>(Profile);
            }

            Services.AddSingleton<IHostedService>(provider => new CacheWarmupRunner(
                Profile,
                provider,
                provider.GetService<ILoggerFactory>()?.CreateLogger<CacheWarmupRunner>()
                    ?? NullLogger<CacheWarmupRunner>.Instance
            ));
            Services
                .AddHealthChecks()
                .Add(
                    new HealthCheckRegistration(
                        CachingProfile.HealthName("hostloom-cache-warmup", Profile),
                        provider => new CacheWarmupReadinessCheck(Profile, provider),
                        HealthStatus.Unhealthy,
                        ["ready"]
                    )
                );
        }

        return this;
    }

    /// <summary>
    /// Registers a readiness check tagged <c>ready</c> that asks the store's
    /// <see cref="ICacheStoreHealthProbe"/>. A store without a probe, including the in-process
    /// composition, reports healthy with an explanation. Liveness is never registered here:
    /// a store outage must not read as "restart me".
    /// </summary>
    public CachingBuilder AddHealthChecks(string readinessName = "hostloom-cache-ready")
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(readinessName);
        if (_registration.StoreReadinessRegistered)
        {
            return this;
        }

        _registration.StoreReadinessRegistered = true;
        if (readinessName == "hostloom-cache-ready")
        {
            readinessName = CachingProfile.HealthName(readinessName, Profile);
        }
        Services
            .AddHealthChecks()
            .Add(
                new HealthCheckRegistration(
                    readinessName,
                    provider => new CacheStoreReadinessCheck(Profile, provider),
                    HealthStatus.Unhealthy,
                    ["ready"]
                )
            );
        return this;
    }

    /// <summary>
    /// Registers <see cref="IDistributedCache"/> (and <see cref="IBufferDistributedCache"/>) over
    /// the chosen distributed store, so <c>HybridCache</c> and other asynchronous Microsoft
    /// consumers share the backend. Entries live under <c>{namespace}:cache:external:</c>, apart
    /// from the tiered cache's own. The synchronous members throw. Sliding expiration is an
    /// explicit opt-in and requires <see cref="CacheStoreCapabilities.SlidingExpiration"/> on the
    /// store; without the opt-in, sliding options throw <see cref="NotSupportedException"/>.
    /// Requires a distributed store: the in-process composition has nothing for the adapter to sit on.
    /// </summary>
    /// <param name="defaultExpiration">
    /// Time to live for entries whose options carry no expiration; thirty minutes when null.
    /// </param>
    /// <param name="enableSlidingExpiration">
    /// Whether to store versioned binary envelopes and renew sliding entries after reads and
    /// explicit refreshes. Opted-in writes use a separate external key domain.
    /// </param>
    public CachingBuilder AddDistributedCacheAdapter(
        TimeSpan? defaultExpiration = null,
        bool enableSlidingExpiration = false
    )
    {
        var expiration = defaultExpiration ?? TimeSpan.FromMinutes(30);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(expiration, TimeSpan.Zero);
        if (Profile is null)
        {
            Services.TryAddSingleton(provider =>
                CreateDistributedCache(provider, expiration, enableSlidingExpiration)
            );
            Services.TryAddSingleton<IDistributedCache>(static provider =>
                provider.GetRequiredService<HostLoomDistributedCache>()
            );
            Services.TryAddSingleton<IBufferDistributedCache>(static provider =>
                provider.GetRequiredService<HostLoomDistributedCache>()
            );
        }
        else
        {
            Services.TryAddKeyedSingleton<HostLoomDistributedCache>(
                Profile,
                (provider, _) =>
                    CreateDistributedCache(provider, expiration, enableSlidingExpiration)
            );
            Services.TryAddKeyedSingleton<IDistributedCache>(
                Profile,
                static (provider, key) =>
                    provider.GetRequiredKeyedService<HostLoomDistributedCache>(key)
            );
            Services.TryAddKeyedSingleton<IBufferDistributedCache>(
                Profile,
                static (provider, key) =>
                    provider.GetRequiredKeyedService<HostLoomDistributedCache>(key)
            );
        }

        return this;
    }

    private HostLoomDistributedCache CreateDistributedCache(
        IServiceProvider provider,
        TimeSpan expiration,
        bool enableSlidingExpiration
    )
    {
        var store =
            CachingProfile.Get<IDistributedCacheStore>(provider, Profile)
            ?? throw new InvalidOperationException(
                "AddDistributedCacheAdapter needs a distributed store: call UseStore<TStore>(name) "
                    + "or a backend's UseRedis() on the CachingBuilder. UseInMemory() composes no "
                    + "distributed tier for the adapter to sit on."
            );
        if (
            enableSlidingExpiration
            && (
                (store.Capabilities & CacheStoreCapabilities.SlidingExpiration) == 0
                || store is not ISlidingExpirationCacheStore
            )
        )
        {
            var profile = Profile is null ? "default cache profile" : $"cache profile '{Profile}'";
            throw new InvalidOperationException(
                $"The {profile} enabled sliding expiration, but its store does not provide "
                    + $"{nameof(CacheStoreCapabilities.SlidingExpiration)} and "
                    + $"{nameof(ISlidingExpirationCacheStore)}."
            );
        }

        return new HostLoomDistributedCache(
            store,
            provider
                .GetRequiredService<IOptionsMonitor<CachingOptions>>()
                .Get(_registration.OptionsName),
            expiration,
            provider.GetRequiredService<TimeProvider>(),
            provider.GetService<ILoggerFactory>()?.CreateLogger<HostLoomDistributedCache>()
                ?? NullLogger<HostLoomDistributedCache>.Instance,
            enableSlidingExpiration
        );
    }

    private CachingBuilder UseSerializer(ICacheValueSerializer serializer, string name)
    {
        RemoveSerializers();
        if (Profile is null)
        {
            Services.AddSingleton(serializer);
        }
        else
        {
            Services.AddKeyedSingleton(Profile, serializer);
        }

        _registration.SerializerName = name;
        return this;
    }

    private void RemoveSerializers()
    {
        for (var i = Services.Count - 1; i >= 0; i--)
        {
            var descriptor = Services[i];
            if (descriptor.ServiceType != typeof(ICacheValueSerializer))
            {
                continue;
            }

            var matches = Profile is null
                ? !descriptor.IsKeyedService
                : descriptor.IsKeyedService && Equals(descriptor.ServiceKey, Profile);
            if (matches)
            {
                Services.RemoveAt(i);
            }
        }
    }

    private void Choose(string storeName)
    {
        if (_registration.StoreName is { } existing)
        {
            throw new InvalidOperationException(
                $"HostLoom caching already uses the '{existing}' store; '{storeName}' cannot be "
                    + "added. Configure exactly one store per cache."
            );
        }

        _registration.StoreName = storeName;
    }
}
