using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Caching.Testing;
using NSubstitute;
using Xunit;

namespace HostLoom.Tests;

public sealed class RecordingCacheStoreTests
{
    [Fact]
    public async Task OverAChannelLessStore_TheCacheRunsTtlOnlyAndCallsAreRecorded()
    {
        // A store whose channel is a separate class, as the Redis and Valkey stores are.
        var inner = Substitute.For<IDistributedCacheStore>();
        var recording = new RecordingCacheStore(inner);
        await using var cache = new TieredCache(
            new CachingOptions { Namespace = "rec" },
            recording,
            new SystemTextJsonCacheValueSerializer(
                new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() }
            )
        );
        var token = TestContext.Current.CancellationToken;

        var value = await cache.GetOrCreateAsync(
            "k",
            _ => ValueTask.FromResult(1),
            new CacheEntryOptions(TimeSpan.FromMinutes(1)),
            token
        );
        await cache.RemoveAsync("k", token);
        using var subscription = recording.Subscribe(_ => { });

        Assert.Equal(1, value);
        Assert.Null(recording.Channel);
        Assert.Equal(1, recording.Count("set"));
        Assert.Equal(1, recording.Count("remove"));
        Assert.Equal(1, recording.Count("publish"));
    }
}
