using BenchmarkDotNet.Attributes;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using HostLoom.Locking;
using HostLoom.Locking.DependencyInjection;
using Microsoft.Extensions.DependencyInjection;

#pragma warning disable CA1707

namespace HostLoom.Benchmarks;

/// <summary>Warm default versus named service resolution; provider construction is outside timing.</summary>
[MemoryDiagnoser]
public class ProfileResolutionBenchmarks
{
    private ServiceProvider _services = null!;

    [GlobalSetup]
    public void Setup()
    {
        var services = new ServiceCollection();
        services.AddHostLoomCaching(options => options.Namespace = "bench-default").UseInMemory();
        services
            .AddHostLoomCaching("catalog", options => options.Namespace = "bench-named")
            .UseInMemory();
        services.AddHostLoomLocking(options => options.Namespace = "bench-default").UseInMemory();
        services
            .AddHostLoomLocking("catalog-jobs", options => options.Namespace = "bench-named")
            .UseInMemory();
        _services = services.BuildServiceProvider();
        _ = _services.GetRequiredService<ICache>();
        _ = _services.GetRequiredKeyedService<ICache>("catalog");
        _ = _services.GetRequiredService<IDistributedLock>();
        _ = _services.GetRequiredKeyedService<IDistributedLock>("catalog-jobs");
    }

    [GlobalCleanup]
    public async Task Cleanup() => await _services.DisposeAsync().ConfigureAwait(false);

    [Benchmark(Baseline = true)]
    public ICache Default_cache_resolution() => _services.GetRequiredService<ICache>();

    [Benchmark]
    public ICache Named_cache_resolution() => _services.GetRequiredKeyedService<ICache>("catalog");

    [Benchmark]
    public IDistributedLock Default_lock_resolution() =>
        _services.GetRequiredService<IDistributedLock>();

    [Benchmark]
    public IDistributedLock Named_lock_resolution() =>
        _services.GetRequiredKeyedService<IDistributedLock>("catalog-jobs");
}
