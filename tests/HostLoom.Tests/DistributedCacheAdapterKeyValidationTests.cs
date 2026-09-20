using System.Buffers;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.DependencyInjection;
using Microsoft.Extensions.Caching.Distributed;
using Microsoft.Extensions.DependencyInjection;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The <c>IDistributedCache</c> adapter applies the kernel's key rules, so a consumer cannot
/// reach the store with a key the tiered cache itself would refuse.
/// </summary>
public sealed class DistributedCacheAdapterKeyValidationTests
{
    private static CancellationToken Token => TestContext.Current.CancellationToken;

    [Theory]
    [InlineData("")]
    [InlineData("catalog eu")]
    [InlineData("catalog\teu")]
    [InlineData("catalog:eu:longer-than-sixteen")]
    public async Task AsyncMembers_RejectAnInvalidKeyBeforeTouchingTheStore(string key)
    {
        var clock = new TestClock();
        var store = new InMemoryDistributedCacheStore(clock);
        var services = new ServiceCollection();
        services.AddSingleton(store);
        services.AddSingleton<TimeProvider>(clock);
        services
            .AddHostLoomCaching(caching =>
            {
                caching.Namespace = "svc";
                caching.MaxKeyLength = 16;
            })
            .UseStore<InMemoryDistributedCacheStore>("InMemoryDistributed")
            .UseSystemTextJson(
                new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }
            )
            .AddDistributedCacheAdapter(TimeSpan.FromMinutes(5));
        await using var provider = services.BuildServiceProvider();
        var cache = provider.GetRequiredService<IDistributedCache>();
        var buffered = (IBufferDistributedCache)cache;
        var options = new DistributedCacheEntryOptions
        {
            AbsoluteExpirationRelativeToNow = TimeSpan.FromMinutes(1),
        };

        await Assert.ThrowsAsync<ArgumentException>(() => cache.GetAsync(key, Token));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.SetAsync(key, [1], options, Token));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.RefreshAsync(key, Token));
        await Assert.ThrowsAsync<ArgumentException>(() => cache.RemoveAsync(key, Token));
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await buffered.TryGetAsync(key, new ArrayBufferWriter<byte>(), Token)
        );
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await buffered.SetAsync(key, new ReadOnlySequence<byte>([1]), options, Token)
        );

        // A key within the rules still round-trips, so the bound is the only thing that changed.
        await cache.SetAsync("catalog:eu", [1], options, Token);
        Assert.Equal([1], await cache.GetAsync("catalog:eu", Token));
    }
}
