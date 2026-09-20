using HostLoom.Caching;
using HostLoom.Redis;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

public sealed class RedisInvalidationLifecycleTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Throwing_subscribers_do_not_block_later_invalidation_observers(
        bool trackingFlush
    )
    {
        await using var connection = new RedisConnection(Substitute.For<IConnectionMultiplexer>());
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "catalog" }
        );
        using var failing = channel.Subscribe(_ =>
            throw new InvalidOperationException("observer failed")
        );
        var delivered = new List<CacheInvalidation>();
        using var healthy = channel.Subscribe(delivered.Add);
        if (trackingFlush)
        {
            channel.HandleTrackingMessage(RedisValue.Null);
        }
        else
        {
            channel.HandleExplicitMessage("v1\nkcatalog:eu");
        }
        var invalidation = Assert.Single(delivered);
        Assert.Equal(trackingFlush, invalidation.FlushAll);
        if (!trackingFlush)
        {
            Assert.Equal(["catalog:eu"], invalidation.Keys);
        }
    }

    [Fact]
    public async Task Disposed_channel_rejects_dispatch_and_new_subscribers()
    {
        await using var connection = new RedisConnection(Substitute.For<IConnectionMultiplexer>());
        var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "catalog" }
        );
        var delivered = new List<CacheInvalidation>();
        using var subscription = channel.Subscribe(delivered.Add);
        var disposing = channel.DisposeAsync().AsTask();
        Assert.Same(disposing, channel.DisposeAsync().AsTask());
        await disposing;
        channel.HandleExplicitMessage("v1\nkcatalog:eu");
        channel.HandleTrackingMessage(RedisValue.Null);
        Assert.Empty(delivered);
        Assert.Throws<ObjectDisposedException>(() => channel.Subscribe(_ => { }));
    }
}
