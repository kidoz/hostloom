using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Conformance;
using Microsoft.Extensions.Caching.Distributed;
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
    public async Task AddWarmup_NamedProfile_DeduplicatesTheSameWarmupType()
    {
        var services = new ServiceCollection();
        services.AddSingleton(
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously)
        );
        services
            .AddHostLoomCaching("catalog")
            .UseInMemory()
            .AddWarmup<GatedWarmup>()
            .AddWarmup<GatedWarmup>();
        await using var provider = services.BuildServiceProvider();

        Assert.Single(provider.GetKeyedServices<ICacheWarmup>("catalog"));
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

        var notStarted = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );
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
        var completed = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );
        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Healthy, notStarted.Status);
        Assert.Equal(
            "Cache warmup not started: 0 succeeded, 0 failed.",
            notStarted.Entries["hostloom-cache-warmup"].Description
        );
        Assert.Equal(HealthStatus.Unhealthy, whileRunning.Status);
        Assert.Equal(HealthStatus.Healthy, completed.Status);
        Assert.Equal(
            "Cache warmup succeeded: 1 succeeded, 0 failed.",
            completed.Entries["hostloom-cache-warmup"].Description
        );
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
    public async Task AddWarmup_PartialFailureIsFailOpenAndRetainsProgress()
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
            .AddWarmup<GatedWarmup>()
            .AddWarmup<FailingWarmup>();
        using var host = builder.Build();
        var health = host.Services.GetRequiredService<HealthCheckService>();

        await host.StartAsync(TestContext.Current.CancellationToken);
        gate.SetResult();
        await CacheConformance.WaitUntilAsync(async () =>
            (await health.CheckHealthAsync(r => r.Tags.Contains("ready"))).Status
            == HealthStatus.Healthy
        );
        var completed = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );
        var cache = host.Services.GetRequiredService<ICache>();
        var warm = await cache.TryGetAsync<int>("warm", TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Healthy, completed.Status);
        Assert.Equal(
            "Cache warmup failed: 1 succeeded, 1 failed.",
            completed.Entries["hostloom-cache-warmup"].Description
        );
        Assert.True(warm.Found);
    }

    [Fact]
    public async Task AddWarmup_CancellationIsTerminalFailOpenAndNotAFailure()
    {
        var builder = Host.CreateApplicationBuilder();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiting = new WarmupWait();
        builder.Services.AddSingleton(gate);
        builder.Services.AddSingleton(waiting);
        builder
            .Services.AddHostLoomCaching(caching =>
            {
                caching.Namespace = "svc";
                caching.Warmup.BlocksReadiness = true;
            })
            .UseInMemory()
            .AddWarmup<GatedWarmup>()
            .AddWarmup<WaitingWarmup>();
        using var host = builder.Build();
        var health = host.Services.GetRequiredService<HealthCheckService>();

        await host.StartAsync(TestContext.Current.CancellationToken);
        gate.SetResult();
        await waiting.Started.Task.WaitAsync(TestContext.Current.CancellationToken);
        await host.StopAsync(TestContext.Current.CancellationToken);
        var completed = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );
        var cache = host.Services.GetRequiredService<ICache>();
        var warm = await cache.TryGetAsync<int>("warm", TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Healthy, completed.Status);
        Assert.Equal(
            "Cache warmup cancelled: 1 succeeded, 0 failed.",
            completed.Entries["hostloom-cache-warmup"].Description
        );
        Assert.True(warm.Found);
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

    [Fact]
    public async Task NamedProfiles_IsolateOptionsStoresAndSerializers()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching("catalog", caching => caching.Namespace = "catalog")
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
            .UseSystemTextJson(Json());
        services
            .AddHostLoomCaching("inventory", caching => caching.Namespace = "inventory")
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
            .UseSerializer<CountingSerializer>();
        services.AddHostLoomCaching(caching => caching.Namespace = "default").UseInMemory();
        await using var provider = services.BuildServiceProvider();

        var catalog = provider.GetRequiredKeyedService<ICache>("catalog");
        var inventory = provider.GetRequiredKeyedService<ICache>("inventory");

        Assert.Equal("catalog", CachingProbe.Describe(catalog).Namespace);
        Assert.Equal("inventory", CachingProbe.Describe(inventory).Namespace);
        Assert.Equal(
            "default",
            CachingProbe.Describe(provider.GetRequiredService<ICache>()).Namespace
        );
        Assert.NotSame(catalog, inventory);
        Assert.Same(catalog, provider.GetRequiredKeyedService<ICache>("catalog"));
        Assert.NotSame(
            provider.GetRequiredKeyedService<IDistributedCacheStore>("catalog"),
            provider.GetRequiredKeyedService<IDistributedCacheStore>("inventory")
        );
        Assert.IsType<SystemTextJsonCacheValueSerializer>(
            provider.GetRequiredKeyedService<ICacheValueSerializer>("catalog")
        );
        Assert.IsType<CountingSerializer>(
            provider.GetRequiredKeyedService<ICacheValueSerializer>("inventory")
        );
        Assert.Null(provider.GetService<ICacheValueSerializer>());
    }

    [Fact]
    public async Task NamedDistributedCacheAdapter_RegistersKeyedServicesOnly()
    {
        var services = new ServiceCollection();
        services
            .AddHostLoomCaching("catalog", caching => caching.Namespace = "catalog")
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
            .UseSystemTextJson(Json())
            .AddDistributedCacheAdapter();
        await using var provider = services.BuildServiceProvider();

        Assert.Null(provider.GetService<IDistributedCache>());
        var cache = provider.GetRequiredKeyedService<IDistributedCache>("catalog");
        Assert.NotNull(cache);
        Assert.Same(cache, provider.GetRequiredKeyedService<IBufferDistributedCache>("catalog"));
    }

    [Fact]
    public async Task NamedWarmup_UsesItsBoundCacheAndHealthName()
    {
        var builder = Host.CreateApplicationBuilder();
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        builder.Services.AddSingleton(gate);
        builder
            .Services.AddHostLoomCaching(
                "catalog",
                caching =>
                {
                    caching.Namespace = "catalog";
                    caching.Warmup.BlocksReadiness = true;
                }
            )
            .UseInMemory()
            .AddWarmup<GatedWarmup>();
        using var host = builder.Build();

        await host.StartAsync(TestContext.Current.CancellationToken);
        var cache = host.Services.GetRequiredKeyedService<ICache>("catalog");
        var health = host.Services.GetRequiredService<HealthCheckService>();
        var running = await health.CheckHealthAsync(
            r => r.Tags.Contains("ready"),
            TestContext.Current.CancellationToken
        );
        gate.SetResult();
        await CacheConformance.WaitUntilAsync(async () =>
            (await cache.TryGetAsync<int>("warm")).Found
        );
        await host.StopAsync(TestContext.Current.CancellationToken);

        Assert.Equal(HealthStatus.Unhealthy, running.Status);
        Assert.Contains("hostloom-cache-warmup:catalog", running.Entries.Keys);
        Assert.True(
            (await cache.TryGetAsync<int>("warm", TestContext.Current.CancellationToken)).Found
        );
        Assert.Null(host.Services.GetService<ICache>());
    }

    [Fact]
    public async Task ProviderTeardown_DisposesDefaultAndNamedCaches()
    {
        var services = new ServiceCollection();
        services.AddHostLoomCaching(caching => caching.Namespace = "default").UseInMemory();
        services
            .AddHostLoomCaching("catalog", caching => caching.Namespace = "catalog")
            .UseInMemory();
        var provider = services.BuildServiceProvider();
        var defaultCache = provider.GetRequiredService<ICache>();
        var namedCache = provider.GetRequiredKeyedService<ICache>("catalog");

        await provider.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            defaultCache.TryGetAsync<int>("k", TestContext.Current.CancellationToken).AsTask()
        );
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            namedCache.TryGetAsync<int>("k", TestContext.Current.CancellationToken).AsTask()
        );
    }

    [Fact]
    public async Task NamedHealthChecks_IncludeTheProfile()
    {
        var services = new ServiceCollection();
        services.AddLogging();
        services
            .AddHostLoomCaching("catalog", caching => caching.Namespace = "catalog")
            .UseInMemory()
            .AddHealthChecks();
        services
            .AddHostLoomCaching("inventory", caching => caching.Namespace = "inventory")
            .UseInMemory()
            .AddHealthChecks();
        await using var provider = services.BuildServiceProvider();

        var report = await provider
            .GetRequiredService<HealthCheckService>()
            .CheckHealthAsync(TestContext.Current.CancellationToken);

        Assert.Contains("hostloom-cache-ready:catalog", report.Entries.Keys);
        Assert.Contains("hostloom-cache-ready:inventory", report.Entries.Keys);
    }

    [Fact]
    public async Task DuplicateHealthNamesAcrossProfiles_FailStartup()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoomCaching("catalog", caching => caching.Namespace = "catalog")
            .UseInMemory()
            .AddHealthChecks("shared");
        builder
            .Services.AddHostLoomCaching("inventory", caching => caching.Namespace = "inventory")
            .UseInMemory()
            .AddHealthChecks("shared");
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );

        Assert.Contains("shared", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public async Task InvalidNamedOptions_IdentifyTheProfile()
    {
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoomCaching("catalog", caching => caching.Namespace = "Bad Name")
            .UseInMemory();
        using var host = builder.Build();

        var exception = await Assert.ThrowsAsync<OptionsValidationException>(() =>
            host.StartAsync(TestContext.Current.CancellationToken)
        );

        Assert.Contains("catalog", exception.Message, StringComparison.Ordinal);
        Assert.Contains("Caching:Namespace", exception.Message, StringComparison.Ordinal);
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

    private sealed class FailingWarmup : ICacheWarmup
    {
        public ValueTask WarmupAsync(ICache cache, CancellationToken cancellationToken) =>
            throw new InvalidOperationException("Warmup failed.");
    }

    private sealed class WarmupWait
    {
        public TaskCompletionSource Started { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
    }

    private sealed class WaitingWarmup(WarmupWait waiting) : ICacheWarmup
    {
        public async ValueTask WarmupAsync(ICache cache, CancellationToken cancellationToken)
        {
            waiting.Started.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
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
            IReadOnlyCollection<string>? tagKeys = null,
            CancellationToken cancellationToken = default
        ) => _inner.SetIfAbsentAsync(key, payload, timeToLive, tagKeys, cancellationToken);

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
