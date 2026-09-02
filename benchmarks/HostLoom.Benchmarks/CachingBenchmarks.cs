using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using BenchmarkDotNet.Attributes;
using HostLoom.Caching;
using HostLoom.Locking;

// CA1707: underscored benchmark names are how the results table stays readable.
#pragma warning disable CA1707

namespace HostLoom.Benchmarks;

/// <summary>
/// The cache's tracked scenarios on the in-process backends: an in-process hit through the
/// state-carrying overload (the path that must allocate nothing), a distributed hit through the
/// serializer, a miss under 100-way contention, and a bulk read of 100 keys.
/// </summary>
[MemoryDiagnoser]
public class CachingBenchmarks
{
    private static readonly CacheEntryOptions Options = new(TimeSpan.FromMinutes(10));
    private static readonly string[] ManyKeys = Enumerable
        .Range(0, 100)
        .Select(i => $"many-{i}")
        .ToArray();

    private TieredCache _local = null!;
    private TieredCache _distributedOnly = null!;
    private TieredCache _tiered = null!;
    private int _missCounter;

    [GlobalSetup]
    public async Task Setup()
    {
        var serializer = new SystemTextJsonCacheValueSerializer(
            new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }
        );
        var store = new InMemoryDistributedCacheStore();
        _local = new TieredCache(new CachingOptions { Namespace = "bench-l1" });
        var noLocal = new CachingOptions { Namespace = "bench-l2" };
        noLocal.L1.Enabled = false;
        _distributedOnly = new TieredCache(noLocal, store, serializer);
        _tiered = new TieredCache(
            new CachingOptions { Namespace = "bench-tiered" },
            store,
            serializer
        );

        await _local.SetAsync("hit", new Catalog("eu", 42), Options).ConfigureAwait(false);
        await _distributedOnly
            .SetAsync("hit", new Catalog("eu", 42), Options)
            .ConfigureAwait(false);
        foreach (var key in ManyKeys)
        {
            await _tiered.SetAsync(key, new Catalog(key, 1), Options).ConfigureAwait(false);
        }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _local.DisposeAsync().ConfigureAwait(false);
        await _distributedOnly.DisposeAsync().ConfigureAwait(false);
        await _tiered.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark(Baseline = true)]
    public ValueTask<Catalog?> L1_hit_state_overload() =>
        _local.GetOrCreateAsync(
            "hit",
            0,
            static (_, _) => ValueTask.FromResult(new Catalog("miss", 0)),
            Options
        );

    [Benchmark]
    public ValueTask<Catalog?> L2_hit_through_serializer() =>
        _distributedOnly.GetOrCreateAsync(
            "hit",
            0,
            static (_, _) => ValueTask.FromResult(new Catalog("miss", 0)),
            Options
        );

    [Benchmark]
    public async Task Miss_under_100_way_contention()
    {
        var key = "miss-" + Interlocked.Increment(ref _missCounter);
        var callers = new Task<Catalog?>[100];
        for (var i = 0; i < callers.Length; i++)
        {
            callers[i] = _tiered
                .GetOrCreateAsync(
                    key,
                    0,
                    static (_, _) => ValueTask.FromResult(new Catalog("computed", 1)),
                    Options
                )
                .AsTask();
        }

        await Task.WhenAll(callers).ConfigureAwait(false);
    }

    [Benchmark]
    public ValueTask<IReadOnlyDictionary<string, Catalog>> GetMany_100_keys() =>
        _tiered.GetManyAsync<Catalog>(ManyKeys);
}

/// <summary>The value the caching benchmarks store.</summary>
public sealed record Catalog(string Region, int Items);

/// <summary>Lock acquire and release, and execute-with-lock, on the in-process provider.</summary>
[MemoryDiagnoser]
public class LockingBenchmarks
{
    private DistributedLock _lock = null!;

    [GlobalSetup]
    public void Setup() =>
        _lock = new DistributedLock(
            new LockingOptions { Namespace = "bench" },
            new InMemoryLockProvider()
        );

    [GlobalCleanup]
    public async Task Cleanup() => await _lock.DisposeAsync().ConfigureAwait(false);

    [Benchmark(Baseline = true)]
    public async Task Acquire_and_release()
    {
        var handle = await _lock.TryAcquireAsync("job").ConfigureAwait(false);
        await handle!.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark]
    public ValueTask<int> Execute_with_lock() =>
        _lock.ExecuteWithLockAsync("job", static _ => ValueTask.FromResult(1));
}
