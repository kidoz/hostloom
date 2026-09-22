using BenchmarkDotNet.Attributes;
using HostLoom.Locking;

#pragma warning disable CA1707

namespace HostLoom.Benchmarks;

/// <summary>Separates renewal of an existing handle from a complete lease lifecycle.</summary>
[MemoryDiagnoser]
public class LockRenewalBenchmarks
{
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(30);
    private DistributedLock _locks = null!;
    private ILockHandle _held = null!;

    [GlobalSetup]
    public async Task Setup()
    {
        _locks = new DistributedLock(
            new LockingOptions { Namespace = "bench-renewal", AutoExtend = false },
            new InMemoryLockProvider()
        );
        _held =
            await _locks.TryAcquireAsync("held").ConfigureAwait(false)
            ?? throw new InvalidOperationException("The benchmark lease was not acquired.");
        if (!await _held.ExtendAsync(Lease).ConfigureAwait(false))
        {
            throw new InvalidOperationException("The benchmark lease could not be renewed.");
        }
    }

    [GlobalCleanup]
    public async Task Cleanup()
    {
        await _held.DisposeAsync().ConfigureAwait(false);
        await _locks.DisposeAsync().ConfigureAwait(false);
    }

    [Benchmark]
    public ValueTask<bool> Renew_held_lease() => _held.ExtendAsync(Lease);

    [Benchmark]
    public async Task Acquire_renew_and_release()
    {
        var handle =
            await _locks.TryAcquireAsync("catalog").ConfigureAwait(false)
            ?? throw new InvalidOperationException("The benchmark lease was not acquired.");
        await using (handle.ConfigureAwait(false))
        {
            if (!await handle.ExtendAsync(Lease).ConfigureAwait(false))
            {
                throw new InvalidOperationException("The benchmark lease could not be renewed.");
            }
        }
    }
}
