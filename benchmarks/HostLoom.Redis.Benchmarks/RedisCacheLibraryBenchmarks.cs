using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using BenchmarkDotNet.Attributes;
using HostLoom.Caching;
using HostLoom.Redis;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using ZiggyCreatures.Caching.Fusion;

// CA1707: underscored benchmark names keep implementation and scenario visible in result tables.
#pragma warning disable CA1707

namespace HostLoom.Redis.Benchmarks;

/// <summary>
/// Compares a warmed distributed-cache hit through Redis. Each implementation has its local tier
/// disabled, uses System.Text.Json, and talks to the endpoint in HOSTLOOM_BENCHMARK_REDIS.
/// </summary>
[MemoryDiagnoser]
public class RedisCacheLibraryBenchmarks
{
    private static readonly RedisCatalog Hit = new("eu", 42);
    private static readonly CacheEntryOptions HostLoomOptions = new(TimeSpan.FromMinutes(10));
    private static readonly HybridCacheEntryOptions HybridOptions = new()
    {
        Expiration = TimeSpan.FromMinutes(10),
        Flags = HybridCacheEntryFlags.DisableLocalCache,
    };
    private static readonly FusionCacheEntryOptions FusionOptions = new(TimeSpan.FromMinutes(10))
    {
        SkipMemoryCacheRead = true,
        SkipMemoryCacheWrite = true,
    };

    private StackExchange.Redis.ConnectionMultiplexer _hostLoomMultiplexer = null!;
    private RedisCacheStore _hostLoomStore = null!;
    private TieredCache _hostLoom = null!;
    private ServiceProvider _hybridServices = null!;
    private HybridCache _hybrid = null!;
    private ServiceProvider _fusionServices = null!;
    private IFusionCache _fusion = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        var configuration = RedisBenchmarkConfiguration.Value;
        var serializerOptions = new JsonSerializerOptions
        {
            TypeInfoResolver = new DefaultJsonTypeInfoResolver(),
        };

        _hostLoomMultiplexer = await StackExchange
            .Redis.ConnectionMultiplexer.ConnectAsync(configuration)
            .ConfigureAwait(false);
        await RedisBenchmarkConfiguration.VerifyAsync(_hostLoomMultiplexer).ConfigureAwait(false);

        _hostLoomStore = new RedisCacheStore(_hostLoomMultiplexer);
        var hostLoomOptions = new CachingOptions { Namespace = "bench-hostloom-redis" };
        hostLoomOptions.L1.Enabled = false;
        _hostLoom = new TieredCache(
            hostLoomOptions,
            _hostLoomStore,
            new SystemTextJsonCacheValueSerializer(serializerOptions)
        );
        await _hostLoom.SetAsync("hit", Hit, HostLoomOptions).ConfigureAwait(false);

        var hybridServices = new ServiceCollection();
        hybridServices.AddStackExchangeRedisCache(options =>
        {
            options.Configuration = configuration;
            options.InstanceName = "bench:hybrid:";
        });
        hybridServices.AddHybridCache();
        _hybridServices = hybridServices.BuildServiceProvider();
        _hybrid = _hybridServices.GetRequiredService<HybridCache>();
        await _hybrid.SetAsync("hit", Hit, HybridOptions).ConfigureAwait(false);

        var fusionServices = new ServiceCollection();
        fusionServices.AddStackExchangeRedisCache(options =>
        {
            options.Configuration = configuration;
            options.InstanceName = "bench:fusion:";
        });
        fusionServices.AddFusionCacheSystemTextJsonSerializer(serializerOptions);
        fusionServices.AddFusionCache().WithRegisteredDistributedCache();
        _fusionServices = fusionServices.BuildServiceProvider();
        _fusion = _fusionServices.GetRequiredService<IFusionCache>();
        await _fusion.SetAsync("hit", Hit, FusionOptions).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _hostLoom.RemoveAsync("hit").ConfigureAwait(false);
        await _hybrid.RemoveAsync("hit").ConfigureAwait(false);
        await _fusion.RemoveAsync("hit").ConfigureAwait(false);
        await _hostLoom.DisposeAsync().ConfigureAwait(false);
        await _hostLoomStore.DisposeAsync().ConfigureAwait(false);
        await _hybridServices.DisposeAsync().ConfigureAwait(false);
        await _fusionServices.DisposeAsync().ConfigureAwait(false);
        await _hostLoomMultiplexer.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark(Baseline = true)]
    public ValueTask<RedisCatalog?> HostLoom_Redis_L2_hit() =>
        _hostLoom.GetOrCreateAsync(
            "hit",
            0,
            static (_, _) => ValueTask.FromResult(Hit),
            HostLoomOptions
        );

    [Benchmark]
    public ValueTask<RedisCatalog> HybridCache_Redis_L2_hit() =>
        _hybrid.GetOrCreateAsync(
            "hit",
            0,
            static (_, _) => ValueTask.FromResult(Hit),
            HybridOptions
        );

    [Benchmark]
    public ValueTask<RedisCatalog> FusionCache_Redis_L2_hit() =>
        _fusion.GetOrSetAsync("hit", static _ => Task.FromResult(Hit), FusionOptions);
}

public sealed record RedisCatalog(string Region, int Items);
