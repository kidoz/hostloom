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
        Services.RemoveAll<ICacheValueSerializer>();
        Services.AddSingleton<ICacheValueSerializer, TSerializer>();
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
        Services.TryAddEnumerable(ServiceDescriptor.Singleton<ICacheWarmup, TWarmup>());
        if (!_registration.WarmupRegistered)
        {
            _registration.WarmupRegistered = true;
            Services.TryAddSingleton<CacheWarmupState>();
            Services.TryAddEnumerable(
                ServiceDescriptor.Singleton<IHostedService, CacheWarmupRunner>()
            );
            Services
                .AddHealthChecks()
                .AddCheck<CacheWarmupReadinessCheck>(
                    "hostloom-cache-warmup",
                    HealthStatus.Unhealthy,
                    ["ready"]
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
        Services
            .AddHealthChecks()
            .AddCheck<CacheStoreReadinessCheck>(readinessName, HealthStatus.Unhealthy, ["ready"]);
        return this;
    }

    /// <summary>
    /// Registers <see cref="IDistributedCache"/> (and <see cref="IBufferDistributedCache"/>) over
    /// the chosen distributed store, so <c>HybridCache</c> and other asynchronous Microsoft
    /// consumers share the backend. Entries live under <c>{namespace}:cache:external:</c>, apart
    /// from the tiered cache's own. The synchronous members throw; <c>RefreshAsync</c> is a no-op
    /// because the store has no touch operation. Requires a distributed store: the in-process
    /// composition has nothing for the adapter to sit on.
    /// </summary>
    /// <param name="defaultExpiration">
    /// Time to live for entries whose options carry no expiration; thirty minutes when null.
    /// </param>
    public CachingBuilder AddDistributedCacheAdapter(TimeSpan? defaultExpiration = null)
    {
        var expiration = defaultExpiration ?? TimeSpan.FromMinutes(30);
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(expiration, TimeSpan.Zero);
        Services.TryAddSingleton(provider =>
        {
            var store =
                provider.GetService<IDistributedCacheStore>()
                ?? throw new InvalidOperationException(
                    "AddDistributedCacheAdapter needs a distributed store: call UseStore<TStore>(name) "
                        + "or a backend's UseRedis() on the CachingBuilder. UseInMemory() composes no "
                        + "distributed tier for the adapter to sit on."
                );
            return new HostLoomDistributedCache(
                store,
                provider.GetRequiredService<IOptions<CachingOptions>>().Value,
                expiration,
                provider.GetRequiredService<TimeProvider>(),
                provider.GetService<ILoggerFactory>()?.CreateLogger<HostLoomDistributedCache>()
                    ?? NullLogger<HostLoomDistributedCache>.Instance
            );
        });
        Services.TryAddSingleton<IDistributedCache>(static provider =>
            provider.GetRequiredService<HostLoomDistributedCache>()
        );
        Services.TryAddSingleton<IBufferDistributedCache>(static provider =>
            provider.GetRequiredService<HostLoomDistributedCache>()
        );
        return this;
    }

    private CachingBuilder UseSerializer(ICacheValueSerializer serializer, string name)
    {
        Services.RemoveAll<ICacheValueSerializer>();
        Services.AddSingleton(serializer);
        _registration.SerializerName = name;
        return this;
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
