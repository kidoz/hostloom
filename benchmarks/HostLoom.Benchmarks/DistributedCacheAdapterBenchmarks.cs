using BenchmarkDotNet.Attributes;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;

#pragma warning disable CA1707

namespace HostLoom.Benchmarks;

/// <summary>Absolute versus opted-in sliding adapter operations over the in-memory store.</summary>
[MemoryDiagnoser]
public class DistributedCacheAdapterBenchmarks
{
    private static readonly byte[] Payload = new byte[64];
    private static readonly DistributedCacheEntryOptions AbsoluteOptions = new()
    {
        AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10),
    };
    private static readonly DistributedCacheEntryOptions SlidingOptions = new()
    {
        AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(10),
        SlidingExpiration = TimeSpan.FromMinutes(5),
    };

    private ServiceProvider _services = null!;
    private CountingStore _absoluteStore = null!;
    private CountingStore _slidingStore = null!;
    private IDistributedCache _absolute = null!;
    private IDistributedCache _sliding = null!;
    private string _absoluteReadOperations = "";
    private string _absoluteWriteOperations = "";
    private string _slidingReadOperations = "";
    private string _slidingWriteOperations = "";

    [GlobalSetup]
    public async Task Setup()
    {
        _absoluteStore = new CountingStore(new InMemoryDistributedCacheStore());
        _slidingStore = new CountingStore(new InMemoryDistributedCacheStore());
        var services = new ServiceCollection();
        services.AddSingleton(_absoluteStore);
        services.AddKeyedSingleton("sliding", _slidingStore);
        services
            .AddHostLoomCaching(options => options.Namespace = "bench-absolute")
            .UseStore<CountingStore>("InMemoryDistributed")
            .UseReflectionSerialization()
            .AddDistributedCacheAdapter();
        services
            .AddHostLoomCaching("sliding", options => options.Namespace = "bench-sliding")
            .UseStore<CountingStore>("InMemoryDistributed")
            .UseReflectionSerialization()
            .AddDistributedCacheAdapter(enableSlidingExpiration: true);
        _services = services.BuildServiceProvider();
        _absolute = _services.GetRequiredService<IDistributedCache>();
        _sliding = _services.GetRequiredKeyedService<IDistributedCache>("sliding");

        await _absolute.SetAsync("hit", Payload, AbsoluteOptions).ConfigureAwait(false);
        await _sliding.SetAsync("hit", Payload, SlidingOptions).ConfigureAwait(false);
        _absoluteReadOperations = await CountAsync(
                _absoluteStore,
                async () => _ = await _absolute.GetAsync("hit").ConfigureAwait(false)
            )
            .ConfigureAwait(false);
        _absoluteWriteOperations = await CountAsync(
                _absoluteStore,
                () => _absolute.SetAsync("write", Payload, AbsoluteOptions)
            )
            .ConfigureAwait(false);
        _slidingReadOperations = await CountAsync(
                _slidingStore,
                async () => _ = await _sliding.GetAsync("hit").ConfigureAwait(false)
            )
            .ConfigureAwait(false);
        _slidingWriteOperations = await CountAsync(
                _slidingStore,
                () => _sliding.SetAsync("write", Payload, SlidingOptions)
            )
            .ConfigureAwait(false);
        AssertOperations(_absoluteReadOperations, "get=1, set=0, refresh=0, remove=0");
        AssertOperations(_absoluteWriteOperations, "get=0, set=1, refresh=0, remove=0");
        AssertOperations(_slidingReadOperations, "get=1, set=0, refresh=1, remove=0");
        AssertOperations(_slidingWriteOperations, "get=0, set=1, refresh=0, remove=1");
        _absoluteStore.CountEnabled = false;
        _slidingStore.CountEnabled = false;
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        Console.WriteLine($"Absolute read store operations: {_absoluteReadOperations}");
        Console.WriteLine($"Absolute write store operations: {_absoluteWriteOperations}");
        Console.WriteLine($"Sliding read store operations: {_slidingReadOperations}");
        Console.WriteLine($"Sliding write store operations: {_slidingWriteOperations}");
        await _services.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark(Baseline = true)]
    public Task<byte[]?> Absolute_read() => _absolute.GetAsync("hit");

    [Benchmark]
    public Task<byte[]?> Sliding_read() => _sliding.GetAsync("hit");

    [Benchmark]
    public Task Absolute_write() => _absolute.SetAsync("write", Payload, AbsoluteOptions);

    [Benchmark]
    public Task Sliding_write() => _sliding.SetAsync("write", Payload, SlidingOptions);

    private static async Task<string> CountAsync(CountingStore store, Func<Task> operation)
    {
        store.Reset();
        await operation().ConfigureAwait(false);
        return store.Snapshot();
    }

    private static void AssertOperations(string actual, string expected)
    {
        if (actual != expected)
        {
            throw new InvalidOperationException(
                $"Expected store operations {expected}, got {actual}."
            );
        }
    }
}

public sealed class CountingStore(IDistributedCacheStore inner)
    : IDistributedCacheStore,
        ISlidingExpirationCacheStore
{
    private int _get;
    private int _set;
    private int _refresh;
    private int _remove;

    public bool CountEnabled { get; set; } = true;

    public CacheStoreCapabilities Capabilities => inner.Capabilities;

    public ValueTask<CacheStoreEntry?> GetAsync(
        string key,
        CancellationToken cancellationToken = default
    )
    {
        if (CountEnabled)
            _get++;
        return inner.GetAsync(key, cancellationToken);
    }

    public ValueTask SetAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    )
    {
        if (CountEnabled)
            _set++;
        return inner.SetAsync(key, payload, timeToLive, tagKeys, cancellationToken);
    }

    public ValueTask<CacheRefreshOutcome> RefreshAsync(
        string key,
        ReadOnlyMemory<byte> expectedPayload,
        TimeSpan slidingWindow,
        DateTimeOffset? absoluteExpiration,
        CancellationToken cancellationToken = default
    )
    {
        if (CountEnabled)
            _refresh++;
        return ((ISlidingExpirationCacheStore)inner).RefreshAsync(
            key,
            expectedPayload,
            slidingWindow,
            absoluteExpiration,
            cancellationToken
        );
    }

    public ValueTask<bool> SetIfAbsentAsync(
        string key,
        ReadOnlyMemory<byte> payload,
        TimeSpan timeToLive,
        IReadOnlyCollection<string>? tagKeys = null,
        CancellationToken cancellationToken = default
    ) => inner.SetIfAbsentAsync(key, payload, timeToLive, tagKeys, cancellationToken);

    public ValueTask RemoveAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    )
    {
        if (CountEnabled)
            _remove++;
        return inner.RemoveAsync(keys, cancellationToken);
    }

    public ValueTask<IReadOnlyDictionary<string, CacheStoreEntry>> GetManyAsync(
        IReadOnlyCollection<string> keys,
        CancellationToken cancellationToken = default
    ) => inner.GetManyAsync(keys, cancellationToken);

    public ValueTask SetManyAsync(
        IReadOnlyCollection<KeyValuePair<string, ReadOnlyMemory<byte>>> entries,
        TimeSpan timeToLive,
        CancellationToken cancellationToken = default
    ) => inner.SetManyAsync(entries, timeToLive, cancellationToken);

    public ValueTask RemoveByTagAsync(
        string tagKey,
        CancellationToken cancellationToken = default
    ) => inner.RemoveByTagAsync(tagKey, cancellationToken);

    public void Reset() => (_get, _set, _refresh, _remove) = (0, 0, 0, 0);

    public string Snapshot() => $"get={_get}, set={_set}, refresh={_refresh}, remove={_remove}";
}
