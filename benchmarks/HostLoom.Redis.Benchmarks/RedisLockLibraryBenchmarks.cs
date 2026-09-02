using BenchmarkDotNet.Attributes;
using HostLoom.Locking;
using HostLoom.Redis;
using Medallion.Threading.Redis;
using StackExchange.Redis;

// CA1707: underscored benchmark names keep implementation and scenario visible in result tables.
#pragma warning disable CA1707

namespace HostLoom.Redis.Benchmarks;

/// <summary>
/// Compares uncontended acquire/release using one Redis multiplexer. HostLoom automatic extension
/// is disabled; Medallion retains its production acquisition and lease-loss behavior.
/// </summary>
[MemoryDiagnoser]
public class RedisLockLibraryBenchmarks
{
    private ConnectionMultiplexer _multiplexer = null!;
    private RedisLockProvider _provider = null!;
    private HostLoom.Locking.DistributedLock _hostLoom = null!;
    private RedisDistributedLock _medallion = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _multiplexer = await ConnectionMultiplexer
            .ConnectAsync(RedisBenchmarkConfiguration.Value)
            .ConfigureAwait(false);
        await RedisBenchmarkConfiguration.VerifyAsync(_multiplexer).ConfigureAwait(false);

        _provider = new RedisLockProvider(_multiplexer);
        _hostLoom = new HostLoom.Locking.DistributedLock(
            new LockingOptions
            {
                Namespace = "bench-hostloom-redis",
                AutoExtend = false,
                Retry = LockRetryPolicy.Immediate(0),
            },
            _provider
        );
        _medallion = new RedisDistributedLock(
            "bench:medallion:lock:job",
            _multiplexer.GetDatabase()
        );

        await _multiplexer
            .GetDatabase()
            .KeyDeleteAsync(["bench-hostloom-redis:lock:job", "bench:medallion:lock:job"])
            .ConfigureAwait(false);
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _multiplexer
            .GetDatabase()
            .KeyDeleteAsync(["bench-hostloom-redis:lock:job", "bench:medallion:lock:job"])
            .ConfigureAwait(false);
        await _hostLoom.DisposeAsync().ConfigureAwait(false);
        await _provider.DisposeAsync().ConfigureAwait(false);
        await _multiplexer.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark(Baseline = true)]
    public async Task HostLoom_acquire_and_release()
    {
        var handle = await _hostLoom.TryAcquireAsync("job").ConfigureAwait(false);
        await handle!.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark]
    public async Task Medallion_acquire_and_release()
    {
        var handle = await _medallion.AcquireAsync(TimeSpan.Zero).ConfigureAwait(false);
        await handle.DisposeAsync().ConfigureAwait(false);
    }
}
