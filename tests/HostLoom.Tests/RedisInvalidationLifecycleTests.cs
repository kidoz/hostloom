using System.Net;
using HostLoom.Caching;
using HostLoom.Conformance;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
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

    [Fact]
    public async Task A_failed_subscribe_detaches_its_own_handler_before_the_next_attempt()
    {
        const string Explicit = "catalog:cache:invalidate";
        var sdk = new SdkSubscriptions((channel, attempt) => channel == Explicit && attempt <= 3);
        await using var connection = sdk.Connection();
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions
            {
                Namespace = "catalog",
                Invalidation = { Mode = CacheInvalidationMode.Broadcast },
            }
        );
        var received = new TaskCompletionSource<CacheInvalidation>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        using var observer = channel.Subscribe(i => received.TrySetResult(i));
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport == RedisInvalidationTransport.Broadcast)
        );

        Assert.Equal(4, sdk.Attempts(Explicit));
        Assert.Equal(1, sdk.Attached(Explicit));
        Assert.Equal(1, sdk.Attached("__keyspace@0__:catalog:cache:data:*"));

        // What the SDK hands the handler reaches the observers through the channel's reader.
        sdk.Deliver(Explicit, "v1\nkcatalog:eu");
        var invalidation = await received.Task.WaitAsync(
            TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken
        );
        Assert.Equal(["catalog:eu"], invalidation.Keys);

        await channel.DisposeAsync();
        Assert.Equal(0, sdk.AttachedTotal);
        Assert.Equal(0, sdk.BlanketUnsubscribes);
    }

    [Fact]
    public async Task Tracking_subscription_is_attached_once_across_registration_retries()
    {
        const string Tracking = "__redis__:invalidate";
        var sdk = new SdkSubscriptions((channel, attempt) => channel == Tracking && attempt <= 2);
        await using var connection = sdk.Connection();
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions
            {
                Namespace = "catalog",
                Invalidation = { Mode = CacheInvalidationMode.Tracking },
            }
        );
        using var observer = channel.Subscribe(_ => { });

        // Two refused subscriptions, then registration fails for want of a connected server on
        // every remaining attempt, leaving the explicit channel as the only fan-out.
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport == RedisInvalidationTransport.ExplicitOnly)
        );
        Assert.Equal(3, sdk.Attempts(Tracking));
        Assert.Equal(1, sdk.Attached(Tracking));
        Assert.Equal(1, sdk.Attached("catalog:cache:invalidate"));

        await channel.DisposeAsync();
        Assert.Equal(0, sdk.AttachedTotal);
        Assert.Equal(0, sdk.BlanketUnsubscribes);
    }

    [Fact]
    public async Task Broadcast_retry_subscribes_only_the_patterns_still_missing()
    {
        const string Catalog = "__keyspace@0__:catalog:cache:data:catalog:*";
        const string Rates = "__keyspace@0__:catalog:cache:data:rates:*";
        var sdk = new SdkSubscriptions((channel, attempt) => channel == Rates && attempt == 1);
        await using var connection = sdk.Connection();
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions
            {
                Namespace = "catalog",
                Invalidation =
                {
                    Mode = CacheInvalidationMode.Broadcast,
                    KeyPrefixFilters =
                    {
                        "catalog:cache:data:catalog:",
                        "catalog:cache:data:rates:",
                    },
                },
            }
        );
        using var observer = channel.Subscribe(_ => { });
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport == RedisInvalidationTransport.ExplicitOnly)
        );
        Assert.Equal(1, sdk.Attached(Catalog));
        Assert.Equal(0, sdk.Attached(Rates));

        // A topology refresh retries broadcast; the pattern already attached is not added again.
        sdk.Multiplexer.ConfigurationChanged += Raise.EventWith(
            new EndPointEventArgs(sdk.Multiplexer, new IPEndPoint(IPAddress.Loopback, 6379))
        );
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(channel.Transport == RedisInvalidationTransport.Broadcast)
        );
        Assert.Equal(1, sdk.Attempts(Catalog));
        Assert.Equal(2, sdk.Attempts(Rates));
        Assert.Equal(1, sdk.Attached(Catalog));
        Assert.Equal(1, sdk.Attached(Rates));
    }

    /// <summary>
    /// Stands in for the StackExchange.Redis subscription table: a handler is attached before the
    /// server answers, stays attached when the subscription fails, and is removed by delegate.
    /// </summary>
    private sealed class SdkSubscriptions
    {
        private readonly Lock _gate = new();
        private readonly List<(
            string Channel,
            Action<RedisChannel, RedisValue> Handler
        )> _attached = [];
        private readonly Dictionary<string, int> _attempts = new(StringComparer.Ordinal);
        private int _blanketUnsubscribes;

        public SdkSubscriptions(Func<string, int, bool> refuses)
        {
            var subscriber = Substitute.For<ISubscriber>();
            subscriber
                .SubscribeAsync(
                    Arg.Any<RedisChannel>(),
                    Arg.Any<Action<RedisChannel, RedisValue>>(),
                    Arg.Any<CommandFlags>()
                )
                .Returns(call =>
                {
                    var channel = call.ArgAt<RedisChannel>(0).ToString();
                    int attempt;
                    lock (_gate)
                    {
                        _attached.Add((channel, call.ArgAt<Action<RedisChannel, RedisValue>>(1)));
                        attempt = _attempts[channel] = _attempts.GetValueOrDefault(channel) + 1;
                    }

                    return refuses(channel, attempt)
                        ? Task.FromException(
                            new RedisConnectionException(
                                ConnectionFailureType.UnableToConnect,
                                CommandFlags.None,
                                "refused"
                            )
                        )
                        : Task.CompletedTask;
                });
            subscriber
                .UnsubscribeAsync(
                    Arg.Any<RedisChannel>(),
                    Arg.Any<Action<RedisChannel, RedisValue>?>(),
                    Arg.Any<CommandFlags>()
                )
                .Returns(call =>
                {
                    var channel = call.ArgAt<RedisChannel>(0).ToString();
                    var handler = call.ArgAt<Action<RedisChannel, RedisValue>?>(1);
                    lock (_gate)
                    {
                        if (handler is null)
                        {
                            _blanketUnsubscribes++;
                        }
                        else
                        {
                            _attached.Remove((channel, handler));
                        }
                    }

                    return Task.CompletedTask;
                });
            Multiplexer = Substitute.For<IConnectionMultiplexer>();
            Multiplexer.GetSubscriber(Arg.Any<object>()).Returns(subscriber);
        }

        public IConnectionMultiplexer Multiplexer { get; }

        public int AttachedTotal
        {
            get
            {
                lock (_gate)
                {
                    return _attached.Count;
                }
            }
        }

        public int BlanketUnsubscribes
        {
            get
            {
                lock (_gate)
                {
                    return _blanketUnsubscribes;
                }
            }
        }

        public RedisConnection Connection() =>
            new(
                Multiplexer,
                new RedisOptions
                {
                    Configuration = "external",
                    InitialRetryDelay = TimeSpan.FromMilliseconds(1),
                }
            );

        public int Attempts(string channel)
        {
            lock (_gate)
            {
                return _attempts.GetValueOrDefault(channel);
            }
        }

        public int Attached(string channel)
        {
            lock (_gate)
            {
                return _attached.Count(entry => entry.Channel == channel);
            }
        }

        public void Deliver(string channel, RedisValue message)
        {
            Action<RedisChannel, RedisValue>[] handlers;
            lock (_gate)
            {
                handlers =
                [
                    .. _attached
                        .Where(entry => entry.Channel == channel)
                        .Select(entry => entry.Handler),
                ];
            }

            foreach (var handler in handlers)
            {
                handler(RedisChannel.Literal(channel), message);
            }
        }
    }
}
