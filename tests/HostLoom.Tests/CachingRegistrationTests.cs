using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Conformance;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.Tests;

public sealed class CachingRegistrationTests
{
    private static JsonSerializerOptions Json() =>
        new() { TypeInfoResolver = new DefaultJsonTypeInfoResolver() };

    [Fact]
    public async Task AddHostLoomCaching_UseInMemory_ResolvesAWorkingCache()
    {
        var services = new ServiceCollection();
        services.AddHostLoomCaching(caching => caching.Namespace = "svc").UseInMemory();
        await using var provider = services.BuildServiceProvider();

        var cache = provider.GetRequiredService<ICache>();
        var value = await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(1),
            TimeSpan.FromMinutes(1),
            TestContext.Current.CancellationToken
        );

        Assert.Equal(1, value);
        Assert.Same(cache, provider.GetRequiredService<ICache>());
        Assert.Equal("InMemory", CachingProbe.Describe(cache).Store);
    }

    [Fact]
    public void UseStore_AfterUseInMemory_ThrowsNamingInMemory()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseInMemory();

        var exception = Assert.Throws<InvalidOperationException>(() =>
            builder.UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
        );

        Assert.Contains("InMemory", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void UseInMemory_Twice_Throws()
    {
        var services = new ServiceCollection();
        var builder = services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseInMemory();

        Assert.Throws<InvalidOperationException>(() => builder.UseInMemory());
    }

    [Fact]
    public void RepeatedAdd_ReturnsABuilderOverTheSameRegistration()
    {
        var services = new ServiceCollection();
        services.AddHostLoomCaching(caching => caching.Namespace = "svc").UseInMemory();

        Assert.Throws<InvalidOperationException>(() => services.AddHostLoomCaching().UseInMemory());
        Assert.Single(services, descriptor => descriptor.ServiceType == typeof(ICache));
    }

    [Fact]
    public async Task UseStore_WithoutSerializer_FailsValidationNamingUseSystemTextJson()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed");
        await using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<ICache>()
        );

        Assert.Contains("UseSystemTextJson", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InvalidOptions_FailAtStartupNamingTheOptionKey()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoomCaching(caching => caching.Namespace = "Bad Name")
            .UseInMemory();
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );

        Assert.Contains("Caching:Namespace", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task UseStore_RegistersTheStoreAsChannelAndTheCacheUsesIt()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
            .UseSystemTextJson(Json());
        await using var provider = services.BuildServiceProvider();

        var store = provider.GetRequiredService<IDistributedCacheStore>();
        var channel = provider.GetRequiredService<ICacheInvalidationChannel>();
        var description = CachingProbe.Describe(provider.GetRequiredService<ICache>());

        Assert.Same(store, channel);
        Assert.Equal(nameof(InMemoryDistributedCacheStore), description.Store);
        Assert.Equal(nameof(SystemTextJsonCacheValueSerializer), description.Serializer);
        Assert.StartsWith("channel", description.Invalidation, StringComparison.Ordinal);
    }

    [Fact]
    public void UseSystemTextJson_WithoutResolver_Throws()
    {
        var services = new ServiceCollection();
        var builder = services.AddHostLoomCaching(caching => caching.Namespace = "svc");

        Assert.Throws<ArgumentException>(() =>
            builder.UseSystemTextJson(new JsonSerializerOptions())
        );
    }

    [Fact]
    public async Task UseSerializer_ReplacesAnEarlierChoice()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
            .UseSystemTextJson(Json())
            .UseSerializer<CountingSerializer>();
        await using var provider = services.BuildServiceProvider();

        Assert.IsType<CountingSerializer>(provider.GetRequiredService<ICacheValueSerializer>());
        Assert.Single(
            services,
            descriptor => descriptor.ServiceType == typeof(ICacheValueSerializer)
        );
    }

    [Fact]
    public async Task AddWarmup_RunsAfterStartAndReadinessObeysBlocksReadiness()
    {
        var builder = Host.CreateApplicationBuilder();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        builder.Services.AddSingleton(gate);
        builder
            .Services.AddHostLoomCaching(caching =>
            {
                caching.Namespace = "svc";
                caching.Warmup.BlocksReadiness = true;
            })
            .UseInMemory()
            .AddWarmup<GatedWarmup>();
        using var host = builder.Build();
        var health = host.Services.GetRequiredService<HealthCheckService>();

        await host.StartAsync(TestContext.Current.CancellationToken);
        var whileRunning = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );
        gate.SetResult();
        var cache = host.Services.GetRequiredService<ICache>();
        await CacheConformance.WaitUntilAsync(async () =>
            (await cache.TryGetAsync<int>("warm")).Found
        );
        await CacheConformance.WaitUntilAsync(async () =>
            (await health.CheckHealthAsync(r => r.Tags.Contains("ready"))).Status
            == HealthStatus.Healthy
        );
        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, whileRunning.Status);
        Assert.Contains(
            "GatedWarmup",
            CachingProbe.Describe(cache, host.Services.GetServices<ICacheWarmup>()).Warmups
        );
    }

    [Fact]
    public async Task AddWarmup_WithoutBlockingReadiness_ReportsHealthyWhileRunning()
    {
        var builder = Host.CreateApplicationBuilder();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        builder.Services.AddSingleton(gate);
        builder
            .Services.AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseInMemory()
            .AddWarmup<GatedWarmup>();
        using var host = builder.Build();
        var health = host.Services.GetRequiredService<HealthCheckService>();

        await host.StartAsync(TestContext.Current.CancellationToken);
        var whileRunning = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );
        gate.SetResult();
        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Healthy, whileRunning.Status);
    }

    [Fact]
    public async Task AddHealthChecks_WithoutAProbe_ReportsHealthyAndExplains()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseInMemory()
            .AddHealthChecks();
        await using var provider = services.BuildServiceProvider();

        var report = await provider
            .GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(TestContext.Current.CancellationToken);

        var entry = Assert.Single(report.Entries);
        Assert.Equal("hostloom-cache-ready", entry.Key);
        Assert.Equal(HealthStatus.Healthy, entry.Value.Status);
        Assert.Contains("ready", entry.Value.Tags);
        Assert.Contains(
            "does not report health",
            entry.Value.Description,
            StringComparison.Ordinal
        );
    }

    [Fact]
    public async Task AddHealthChecks_WithAProbe_ReportsWhatTheProbeSays()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services
            .AddHostLoomCaching(caching => caching.Namespace = "svc")
            .UseStore<ProbingStore>("Probing")
            .UseSystemTextJson(Json())
            .AddHealthChecks();
        await using var provider = services.BuildServiceProvider();
        provider.GetRequiredService<ProbingStore>().Health = CacheStoreHealth.Unhealthy(
            "backend down"
        );

        var report = await provider
            .GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, report.Status);
        Assert.Equal("backend down", report.Entries["hostloom-cache-ready"].Description);
    }

    [Fact]
    public void NoStoreChosen_FailsValidationNamingTheBuilderMethods()
    {
        var services = new ServiceCollection();
        services.AddHostLoomCaching(caching => caching.Namespace = "svc");
        using var provider = services.BuildServiceProvider();

        var exception = Assert.Throws<OptionsValidationException>(() =>
            provider.GetRequiredService<ICache>()
        );

        Assert.Contains("UseInMemory()", exception.Message, StringComparison.Ordinal);
    }

    private sealed class GatedWarmup(TaskCompletionSource gate) : ICacheWarmup
    {
        public async ValueTask WarmupAsync(ICache cache, CancellationToken cancellationToken)
        {
            await gate.Task.WaitAsync(cancellationToken);
            await cache.SetAsync(
                "warm",
                1,
                new CacheEntryOptions(TimeSpan.FromMinutes(1)),
                cancellationToken
            );
        }
    }

    private sealed class CountingSerializer : ICacheValueSerializer
    {
        private readonly SystemTextJsonCacheValueSerializer _inner = new(Json());

        public void Serialize<T>(IBufferWriter<byte> destination, T value) =>
            _inner.Serialize(destination, value);

        public T? Deserialize<T>(ReadOnlySpan<byte> payload) => _inner.Deserialize<T>(payload);
    }

    private sealed class ProbingStore : IDistributedCacheStore, ICacheStoreHealthProbe
    {
        private readonly InMemoryDistributedCacheStore _inner = new();

        public CacheStoreHealth Health { get; set; } = CacheStoreHealth.Healthy();

        public CacheStoreCapabilities Capabilities => _inner.Capabilities;

        public ValueTask<CacheStoreHealth> CheckHealthAsync(CancellationToken cancellationToken) =>
            ValueTask.FromResult(Health);

        public ValueTask<CacheStoreEntry?> GetAsync(
            string key,
            CancellationToken cancellationToken = default
        ) => _inner.GetAsync(key, cancellationToken);

        public ValueTask SetAsync(
            string key,
            ReadOnlyMemory<byte> payload,
            TimeSpan timeToLive,
            IReadOnlyCollection<string>? tagKeys = null,
            CancellationToken cancellationToken = default
        ) => _inner.SetAsync(key, payload, timeToLive, tagKeys, cancellationToken);

        public ValueTask<bool> SetIfAbsentAsync(
            string key,
            ReadOnlyMemory<byte> payload,
            TimeSpan timeToLive,
            CancellationToken cancellationToken = default
        ) => _inner.SetIfAbsentAsync(key, payload, timeToLive, cancellationToken);

        public ValueTask RemoveAsync(
            IReadOnlyCollection<string> keys,
            CancellationToken cancellationToken = default
        ) => _inner.RemoveAsync(keys, cancellationToken);

        public ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
            IReadOnlyCollection<string> keys,
            CancellationToken cancellationToken = default
        ) => _inner.GetManyAsync(keys, cancellationToken);

        public ValueTask SetManyAsync(
            IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>> entries,
            TimeSpan timeToLive,
            CancellationToken cancellationToken = default
        ) => _inner.SetManyAsync(entries, timeToLive, cancellationToken);

        public ValueTask RemoveByTagAsync(
            string tagKey,
            CancellationToken cancellationToken = default
        ) => _inner.RemoveByTagAsync(tagKey, cancellationToken);
    }
}
