using HostLoom.Caching;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The invalidation transports' pure parts: turning server-side keys and notifications back into
/// consumer keys, choosing a transport, and finding the subscriber connection. None of it needs
/// a Redis.
/// </summary>
public sealed class RedisInvalidationTests
{
    [Theory]
    [InlineData("svc", false, null, "svc:cache:data:k", true, "k")]
    [InlineData("svc", false, null, "svc:cache:data:a:b:c", true, "a:b:c")]
    [InlineData("svc", true, null, "{svc}:cache:data:k", true, "k")]
    [InlineData("svc", true, null, "svc:cache:data:k", false, "")]
    [InlineData("svc", false, "2", "svc:cache:data:k:2", true, "k")]
    [InlineData("svc", false, "2", "svc:cache:data:k", false, "")]
    [InlineData("svc", false, null, "svc:cache:lease:k", false, "")]
    [InlineData("svc", false, null, "svc:cache:tag:t", false, "")]
    [InlineData("svc", false, null, "svc:lock:k", false, "")]
    [InlineData("svc", false, null, "other:cache:data:k", false, "")]
    [InlineData("svc", false, null, "svc:cache:data:", false, "")]
    public void KeyLayout_RecoversTheConsumerKeyOnlyFromEntryKeys(
        string ns,
        bool hashTags,
        string? version,
        string redisKey,
        bool expected,
        string consumerKey
    )
    {
        var layout = new RedisKeyLayout(ns, hashTags, version);

        var parsed = layout.TryParseDataKey(redisKey, out var key);

        Assert.Equal(expected, parsed);
        Assert.Equal(consumerKey, key);
    }

    [Theory]
    [InlineData(CacheInvalidationMode.Auto, "7.4.0", RedisInvalidationTransport.Tracking)]
    [InlineData(CacheInvalidationMode.Auto, "6.0.0", RedisInvalidationTransport.Tracking)]
    [InlineData(CacheInvalidationMode.Auto, "5.0.14", RedisInvalidationTransport.Broadcast)]
    [InlineData(CacheInvalidationMode.Auto, null, RedisInvalidationTransport.Broadcast)]
    [InlineData(CacheInvalidationMode.Tracking, "5.0.14", RedisInvalidationTransport.Tracking)]
    [InlineData(CacheInvalidationMode.Broadcast, "7.4.0", RedisInvalidationTransport.Broadcast)]
    public void Resolve_PicksTrackingOnRedis6AndLaterUnlessToldOtherwise(
        CacheInvalidationMode mode,
        string? version,
        RedisInvalidationTransport expected
    ) =>
        Assert.Equal(
            expected,
            RedisInvalidationDecoder.Resolve(mode, version is null ? null : Version.Parse(version))
        );

    [Fact]
    public void KeyspaceEvent_IsAcceptedOnlyForEntryKeysAndInvalidatingEvents()
    {
        var layout = new RedisKeyLayout("svc", false, null);

        Assert.True(
            RedisInvalidationDecoder.TryParseKeyspaceEvent(
                layout,
                "__keyspace@0__:svc:cache:data:k",
                "expired",
                out var key
            )
        );
        Assert.Equal("k", key);
        Assert.True(
            RedisInvalidationDecoder.TryParseKeyspaceEvent(
                layout,
                "__keyspace@3__:svc:cache:data:k",
                "evicted",
                out _
            )
        );
        Assert.False(
            RedisInvalidationDecoder.TryParseKeyspaceEvent(
                layout,
                "__keyspace@0__:svc:cache:data:k",
                "incrby",
                out _
            )
        );
        Assert.False(
            RedisInvalidationDecoder.TryParseKeyspaceEvent(
                layout,
                "__keyspace@0__:svc:cache:lease:k",
                "expired",
                out _
            )
        );
        Assert.False(
            RedisInvalidationDecoder.TryParseKeyspaceEvent(
                layout,
                "__keyevent@0__:expired",
                "svc:cache:data:k",
                out _
            )
        );
        Assert.False(
            RedisInvalidationDecoder.TryParseKeyspaceEvent(layout, null, "expired", out _)
        );
    }

    [Fact]
    public void TrackingKey_MapsBackToTheConsumerKey()
    {
        var layout = new RedisKeyLayout("svc", true, "7");

        Assert.True(
            RedisInvalidationDecoder.TryParseTrackingKey(
                layout,
                "{svc}:cache:data:k:7",
                out var key
            )
        );
        Assert.Equal("k", key);
        Assert.False(RedisInvalidationDecoder.TryParseTrackingKey(layout, "{svc}:lock:k", out _));
        Assert.False(RedisInvalidationDecoder.TryParseTrackingKey(layout, null, out _));
    }

    [Fact]
    public void KeyspacePatterns_DefaultToTheNamespaceEntriesAndHonourFilters()
    {
        var layout = new RedisKeyLayout("svc", false, null);

        Assert.Equal(
            ["__keyspace@0__:svc:cache:data:*"],
            RedisInvalidationDecoder.KeyspacePatterns(layout, 0, [])
        );
        Assert.Equal(
            ["__keyspace@2__:svc:cache:data:catalog:*", "__keyspace@2__:svc:cache:data:rates:*"],
            RedisInvalidationDecoder.KeyspacePatterns(
                layout,
                2,
                ["svc:cache:data:catalog:", "svc:cache:data:rates:"]
            )
        );
    }

    [Fact]
    public void FindSubscriberClientId_MatchesNameAndPubSubFlag()
    {
        const string list =
            "id=3 addr=127.0.0.1:1 laddr=127.0.0.1:6379 fd=8 name=hostloom-a-1 age=1 idle=0 flags=N db=0 cmd=client|list\n"
            + "id=4 addr=127.0.0.1:2 laddr=127.0.0.1:6379 fd=9 name=hostloom-a-1 age=1 idle=0 flags=P db=0 cmd=subscribe\n"
            + "id=5 addr=127.0.0.1:3 laddr=127.0.0.1:6379 fd=10 name=hostloom-a-2 age=1 idle=0 flags=P db=0 cmd=subscribe\n";

        Assert.Equal(4, RedisInvalidationDecoder.FindSubscriberClientId(list, "hostloom-a-1"));
        Assert.Equal(
            4,
            RedisInvalidationDecoder.FindSubscriberClientId("txt:" + list, "hostloom-a-1")
        );
        Assert.Equal(5, RedisInvalidationDecoder.FindSubscriberClientId(list, "hostloom-a-2"));
        Assert.Null(RedisInvalidationDecoder.FindSubscriberClientId(list, "hostloom-b-1"));
        Assert.Null(RedisInvalidationDecoder.FindSubscriberClientId(null, "hostloom-a-1"));
    }

    [Fact]
    public async Task Connection_SuffixesTheClientNamePerConnection()
    {
        var options = new RedisOptions { Configuration = "localhost:1", ClientName = "svc" };
        await using var first = new RedisConnection(options);
        await using var second = new RedisConnection(options);

        Assert.StartsWith("svc-", first.ClientName, StringComparison.Ordinal);
        Assert.NotEqual(first.ClientName, second.ClientName);
        Assert.Contains(first.ClientName, first.Describe(), StringComparison.Ordinal);
    }

    [Fact]
    public void Options_ValidateTheClientCommandRetrySettings()
    {
        var options = new RedisOptions
        {
            Configuration = "localhost:6379",
            MaxClientCommandRetries = -1,
            InitialRetryDelay = TimeSpan.Zero,
        };

        var problems = options.Validate();

        Assert.Contains(
            problems,
            p => p.StartsWith("Redis:MaxClientCommandRetries", StringComparison.Ordinal)
        );
        Assert.Contains(
            problems,
            p => p.StartsWith("Redis:InitialRetryDelay", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Channel_ReportsPendingUntilSubscribed()
    {
        await using var connection = new RedisConnection(
            new RedisOptions { Configuration = "localhost:1" }
        );
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "svc" }
        );

        Assert.Equal(RedisInvalidationTransport.Pending, channel.Transport);
        Assert.Contains("pending", channel.ToString(), StringComparison.Ordinal);
        Assert.Equal("svc:cache:invalidate", channel.ChannelName);
    }
}
