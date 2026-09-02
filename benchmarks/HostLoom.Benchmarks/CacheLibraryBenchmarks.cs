using BenchmarkDotNet.Attributes;
using BenchmarkDotNet.Configs;
using HostLoom.Caching;
using Microsoft.Extensions.Caching.Hybrid;
using Microsoft.Extensions.DependencyInjection;
using ZiggyCreatures.Caching.Fusion;

// CA1707: underscored benchmark names keep implementation and scenario visible in result tables.
#pragma warning disable CA1707

namespace HostLoom.Benchmarks;

/// <summary>
/// Compares equivalent process-local cache-aside paths. Redis-backed comparisons live in the
/// separate HostLoom.Redis.Benchmarks project so a missing server can never produce fake results.
/// </summary>
[MemoryDiagnoser]
[CategoriesColumn]
[GroupBenchmarksBy(BenchmarkLogicalGroupRule.ByCategory)]
public class CacheLibraryBenchmarks
{
    private const string L1 = "L1 hit";
    private const string Contention = "100-way miss";
    private static readonly Catalog Hit = new("eu", 42);
    private static readonly CacheEntryOptions HostLoomOptions = new(TimeSpan.FromMinutes(10));
    private static readonly HybridCacheEntryOptions HybridOptions = new()
    {
        Expiration = TimeSpan.FromMinutes(10),
    };
    private static readonly FusionCacheEntryOptions FusionOptions = new(TimeSpan.FromMinutes(10));

    private TieredCache _hostLoom = null!;
    private ServiceProvider _hybridServices = null!;
    private HybridCache _hybrid = null!;
    private ServiceProvider _fusionServices = null!;
    private IFusionCache _fusion = null!;
    private long _missSequence;

    [GlobalSetup]
    public async Task Setup()
    {
        _hostLoom = new TieredCache(new CachingOptions { Namespace = "compare-hostloom" });
        await _hostLoom.SetAsync("hit", Hit, HostLoomOptions).ConfigureAwait(false);

        var hybridServices = new ServiceCollection();
        hybridServices.AddHybridCache();
        _hybridServices = hybridServices.BuildServiceProvider();
        _hybrid = _hybridServices.GetRequiredService<HybridCache>();
        await _hybrid.SetAsync("hit", Hit, HybridOptions).ConfigureAwait(false);

        var fusionServices = new ServiceCollection();
        fusionServices.AddFusionCache();
        _fusionServices = fusionServices.BuildServiceProvider();
        _fusion = _fusionServices.GetRequiredService<IFusionCache>();
        await _fusion.SetAsync("hit", Hit, FusionOptions).ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _hostLoom.DisposeAsync().ConfigureAwait(false);
        await _hybridServices.DisposeAsync().ConfigureAwait(false);
        await _fusionServices.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark(Baseline = true), BenchmarkCategory(L1)]
    public ValueTask<Catalog?> HostLoom_L1_hit() =>
        _hostLoom.GetOrCreateAsync(
            "hit",
            0,
            static (_, _) => ValueTask.FromResult(Hit),
            HostLoomOptions
        );

    [Benchmark, BenchmarkCategory(L1)]
    public ValueTask<Catalog> HybridCache_L1_hit() =>
        _hybrid.GetOrCreateAsync(
            "hit",
            0,
            static (_, _) => ValueTask.FromResult(Hit),
            HybridOptions
        );

    [Benchmark, BenchmarkCategory(L1)]
    public ValueTask<Catalog> FusionCache_L1_hit() =>
        _fusion.GetOrSetAsync("hit", static _ => Task.FromResult(Hit), FusionOptions);

    [Benchmark(Baseline = true), BenchmarkCategory(Contention)]
    public Task HostLoom_miss_under_100_way_contention() =>
        RunHostLoomContentionAsync(NextKey("hostloom"));

    [Benchmark, BenchmarkCategory(Contention)]
    public Task HybridCache_miss_under_100_way_contention() =>
        RunHybridContentionAsync(NextKey("hybrid"));

    [Benchmark, BenchmarkCategory(Contention)]
    public Task FusionCache_miss_under_100_way_contention() =>
        RunFusionContentionAsync(NextKey("fusion"));

    private async Task RunHostLoomContentionAsync(string key)
    {
        var gate = NewGate();
        var callers = new Task<Catalog?>[100];
        for (var i = 0; i < callers.Length; i++)
        {
            callers[i] = _hostLoom
                .GetOrCreateAsync(
                    key,
                    gate,
                    static (state, _) => new ValueTask<Catalog>(state.Task),
                    HostLoomOptions
                )
                .AsTask();
        }

        gate.SetResult(Hit);
        await Task.WhenAll(callers).ConfigureAwait(false);
    }

    private async Task RunHybridContentionAsync(string key)
    {
        var gate = NewGate();
        var callers = new Task<Catalog>[100];
        for (var i = 0; i < callers.Length; i++)
        {
            callers[i] = _hybrid
                .GetOrCreateAsync(
                    key,
                    gate,
                    static (state, _) => new ValueTask<Catalog>(state.Task),
                    HybridOptions
                )
                .AsTask();
        }

        gate.SetResult(Hit);
        await Task.WhenAll(callers).ConfigureAwait(false);
    }

    private async Task RunFusionContentionAsync(string key)
    {
        var gate = NewGate();
        Func<CancellationToken, Task<Catalog>> factory = _ => gate.Task;
        var callers = new Task<Catalog>[100];
        for (var i = 0; i < callers.Length; i++)
        {
            callers[i] = FusionCacheExtMethods
                .GetOrSetAsync(_fusion, key, factory, FusionOptions)
                .AsTask();
        }

        gate.SetResult(Hit);
        await Task.WhenAll(callers).ConfigureAwait(false);
    }

    private string NextKey(string cache) =>
        $"{cache}-miss-{Interlocked.Increment(ref _missSequence)}";

    private static TaskCompletionSource<Catalog> NewGate() =>
        new(TaskCreationOptions.RunContinuationsAsynchronously);
}
