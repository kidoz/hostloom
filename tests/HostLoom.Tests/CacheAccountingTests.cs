using HostLoom.Caching;
using Xunit;

namespace HostLoom.Tests;

public sealed class CacheAccountingTests
{
    [Theory]
    [InlineData("replace")]
    [InlineData("clear")]
    [InlineData("mixed")]
    public async Task Concurrent_mutations_preserve_quiescent_byte_accounting(string operation)
    {
        using var cache = new LocalCacheStore(new() { MaxEntries = 1000 }, new TestClock());
        var start = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var workers = Enumerable
            .Range(0, 8)
            .Select(worker =>
                Task.Run(
                    async () =>
                    {
                        await start.Task;
                        for (var i = 0; i < 10000; i++)
                        {
                            var key = "catalog:" + (i % 4);
                            var size = (worker * 31 + i) % 997 + 1;
                            if (operation == "clear" && i % 7 == 0)
                                cache.Clear();
                            else if (operation == "mixed" && i % 5 == 0)
                                cache.Remove(key);
                            else if (operation == "mixed" && i % 3 == 0)
                                cache.SetIfAbsent(key, size, TimeSpan.FromHours(1), size: size);
                            else
                                cache.Set(key, size, TimeSpan.FromHours(1), size: size);
                        }
                    },
                    TestContext.Current.CancellationToken
                )
            )
            .ToArray();
        start.SetResult();
        await Task.WhenAll(workers)
            .WaitAsync(TimeSpan.FromSeconds(15), TestContext.Current.CancellationToken);
        long expected = 0;
        var expectedCount = 0;
        for (var key = 0; key < 4; key++)
            if (cache.TryGet<int>("catalog:" + key, out var size))
            {
                expected += size;
                expectedCount++;
            }
        Assert.Equal(expected, cache.ApproximateBytes);
        Assert.Equal(expectedCount, cache.Count);
        cache.Clear();
        Assert.Equal(0, cache.Count);
        Assert.Equal(0, cache.ApproximateBytes);
    }

    [Fact]
    public void Known_sizes_control_eviction_after_replacement_and_expiry()
    {
        var clock = new TestClock();
        using var cache = new LocalCacheStore(new() { MaxEntries = 100, MaxBytes = 100 }, clock);
        cache.Set("catalog", 80, TimeSpan.FromSeconds(1), ["books"], size: 80);
        cache.Set("catalog", 20, TimeSpan.FromSeconds(1), ["books"], size: 20);
        Assert.Equal(20, cache.ApproximateBytes);
        clock.Advance(TimeSpan.FromSeconds(2));
        Assert.True(cache.SetIfAbsent("catalog", 90, TimeSpan.FromSeconds(10), size: 90));
        Assert.Equal(90, cache.ApproximateBytes);
        cache.Set("inventory", 30, TimeSpan.FromSeconds(10), size: 30);
        Assert.InRange(cache.ApproximateBytes, 0, 100);
        cache.Remove(["catalog", "inventory"]);
        Assert.Equal(0, cache.ApproximateBytes);
    }
}
