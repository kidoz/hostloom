using System.Text;
using HostLoom.Caching;
using HostLoom.Redis;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The explicit invalidation channel is a plain pub/sub channel: anything with PUBLISH rights can
/// write to it, so what arrives is bounded and validated before it reaches the in-process tier.
/// </summary>
public sealed class RedisInvalidationBoundsTests
{
    [Fact]
    public void Decode_DropsAMessageWithMoreItemsThanTheBound()
    {
        var overLimit = new StringBuilder("v1");
        for (var i = 0; i <= RedisCacheInvalidationChannel.MaxItems; i++)
        {
            overLimit.Append("\nkcatalog:").Append(i);
        }

        Assert.Null(RedisCacheInvalidationChannel.Decode(overLimit.ToString()));

        // Keys and tags count together; exactly the bound is still accepted.
        var atLimit = new StringBuilder("v1");
        for (var i = 0; i < RedisCacheInvalidationChannel.MaxItems - 1; i++)
        {
            atLimit.Append("\nkcatalog:").Append(i);
        }

        atLimit.Append("\ntcatalog");
        var decoded = RedisCacheInvalidationChannel.Decode(atLimit.ToString());
        Assert.NotNull(decoded);
        Assert.Equal(RedisCacheInvalidationChannel.MaxItems - 1, decoded.Keys.Count);
        Assert.Equal(["catalog"], decoded.Tags);
    }

    [Theory]
    [InlineData("v1\nkcatalog eu")]
    [InlineData("v1\nkcatalog:eu\ntcatalog\teu")]
    [InlineData("v1\nkcatalog:eu\ntcatalog\r")]
    [InlineData("v1\nkcatalog:eu\nt ")]
    public void Decode_DropsTheWholeMessageWhenAnyItemIsInvalid(string message) =>
        Assert.Null(RedisCacheInvalidationChannel.Decode(message));

    [Fact]
    public void Decode_HonoursTheConfiguredKeyLength()
    {
        var key = new string('k', 513);
        Assert.Null(RedisCacheInvalidationChannel.Decode("v1\nk" + key));
        Assert.NotNull(RedisCacheInvalidationChannel.Decode("v1\nk" + key, maxKeyLength: 1024));
        Assert.Null(RedisCacheInvalidationChannel.Decode("v1\nkcatalog:eu", maxKeyLength: 5));
    }

    [Fact]
    public void Decode_DropsAnOversizedMessageBeforeSplittingIt()
    {
        var message = "v1\nk" + new string('a', RedisCacheInvalidationChannel.MaxPayloadBytes);
        Assert.Null(RedisCacheInvalidationChannel.Decode(message));
    }

    [Fact]
    public void Decode_KeepsFlushAndUnknownLinesForwardCompatible()
    {
        var decoded = RedisCacheInvalidationChannel.Decode("v1\n*\nxfuture\n\nkcatalog:eu");
        Assert.NotNull(decoded);
        Assert.True(decoded.FlushAll);
        Assert.Equal(["catalog:eu"], decoded.Keys);
        Assert.Empty(decoded.Tags);
    }

    [Fact]
    public void Encode_RejectsWhatEverySubscriberWouldDrop()
    {
        Assert.Throws<ArgumentException>(() =>
            RedisCacheInvalidationChannel.Encode(
                new CacheInvalidation(new string[RedisCacheInvalidationChannel.MaxItems + 1], [])
            )
        );
        var oversized = new CacheInvalidation(
            [.. Enumerable.Range(0, 2_000).Select(static _ => new string('a', 600))],
            []
        );
        Assert.Throws<ArgumentException>(() => RedisCacheInvalidationChannel.Encode(oversized));
    }

    [Fact]
    public async Task ExplicitMessages_MalformedOnesAreCountedAndNeverDispatched()
    {
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.ClientName.Returns("hostloom-buildhost-4242");
        await using var connection = new RedisConnection(mux);
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "svc", MaxKeyLength = 64 }
        );
        var received = new List<CacheInvalidation>();
        using var subscription = channel.Subscribe(received.Add);

        channel.HandleExplicitMessage("v1\nkcatalog eu");
        channel.HandleExplicitMessage("v1\nk" + new string('k', 65));
        channel.HandleExplicitMessage(
            new string('a', RedisCacheInvalidationChannel.MaxPayloadBytes + 1)
        );
        channel.HandleExplicitMessage(RedisValue.Null);

        Assert.Equal(4, channel.MalformedMessages);
        Assert.Empty(received);

        channel.HandleExplicitMessage("v1\nkcatalog:eu\ntcatalog");

        Assert.Equal(4, channel.MalformedMessages);
        var invalidation = Assert.Single(received);
        Assert.Equal(["catalog:eu"], invalidation.Keys);
        Assert.Equal(["catalog"], invalidation.Tags);
    }
}
