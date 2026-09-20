using System.Diagnostics;
using System.Net;
using System.Threading.Channels;
using HostLoom.Caching;
using HostLoom.Conformance;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Opt-in tests own a loopback-only Redis container. Database flushes never target the shared
/// developer Redis. Run with HOSTLOOM_REDIS_INVALIDATION_TESTS=1 and Docker available.
/// </summary>
[Collection(nameof(RedisInvalidationRegressionTests))]
[CollectionDefinition(nameof(RedisInvalidationRegressionTests), DisableParallelization = true)]
public sealed class RedisInvalidationRegressionTests : IAsyncLifetime
{
    private const string Skip =
        "Set HOSTLOOM_REDIS_INVALIDATION_TESTS=1 to run against an owned Redis container.";
    private readonly string _container = "hostloom-invalidation-" + Guid.NewGuid().ToString("N");
    private string _configuration = "";
    private bool _started;
    public static bool Enabled =>
        Environment.GetEnvironmentVariable("HOSTLOOM_REDIS_INVALIDATION_TESTS") == "1";

    public async ValueTask InitializeAsync()
    {
        if (!Enabled)
        {
            return;
        }
        _started = true;
        try
        {
            await DockerAsync(
                "run",
                "--detach",
                "--rm",
                "--name",
                _container,
                "--publish",
                "127.0.0.1::6379",
                "redis:7.4",
                "redis-server",
                "--notify-keyspace-events",
                "Kg$xe",
                "--save",
                "",
                "--appendonly",
                "no"
            );
            var port = await DockerAsync("port", _container, "6379/tcp");
            _configuration = port.Trim() + ",allowAdmin=true";
        }
        catch
        {
            await DisposeAsync();
            throw;
        }
    }

    public async ValueTask DisposeAsync()
    {
        if (_started)
        {
            _started = false;
            await DockerAsync("rm", "--force", _container);
        }
    }

    [Theory(Timeout = 40_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Tracking_flush_evicts_L1_for_database_and_server_flushes(bool allDatabases)
    {
        await using var connection = Connection();
        var options = new CachingOptions { Namespace = "catalog" }.WithMode(
            CacheInvalidationMode.Tracking
        );
        await using var channel = new RedisCacheInvalidationChannel(connection, options);
        await using var store = new RedisCacheStore(connection);
        await using var cache = new TieredCache(
            options,
            store,
            SystemTextJsonCacheValueSerializer.CreateReflectionBased(),
            channel
        );
        var flush = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var observer = channel.Subscribe(i =>
        {
            if (i.FlushAll)
            {
                flush.TrySetResult();
            }
        });
        await ReadyAsync(channel, RedisInvalidationTransport.Tracking);
        await cache.SetAsync(
            "catalog:eu",
            "old",
            new CacheEntryOptions(TimeSpan.FromMinutes(1)),
            TestContext.Current.CancellationToken
        );
        Assert.Equal(
            CacheTier.L1,
            (
                await cache.TryGetAsync<string>("catalog:eu", TestContext.Current.CancellationToken)
            ).Tier
        );
        using var admin = await ConnectionMultiplexer.ConnectAsync(_configuration);
        var server = admin.GetServer(admin.GetEndPoints()[0]);
        if (allDatabases)
        {
            await server.FlushAllDatabasesAsync();
        }
        else
        {
            await server.FlushDatabaseAsync();
        }
        await flush.Task.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        await CacheConformance.WaitUntilAsync(async () =>
            !(
                await cache.TryGetAsync<string>("catalog:eu", TestContext.Current.CancellationToken)
            ).Found
        );
        Assert.False(await admin.GetDatabase().KeyExistsAsync("catalog:cache:data:catalog:eu"));
    }

    [Fact(Timeout = 40_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task Broadcast_receives_set_delete_unlink_and_expiry_notifications()
    {
        await using var connection = Connection();
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "catalog" }.WithMode(CacheInvalidationMode.Broadcast)
        );
        var received = Channel.CreateUnbounded<string>();
        using var observer = channel.Subscribe(i =>
        {
            foreach (var key in i.Keys)
            {
                received.Writer.TryWrite(key);
            }
        });
        await ReadyAsync(channel, RedisInvalidationTransport.Broadcast);
        using var writer = await ConnectionMultiplexer.ConnectAsync(_configuration);
        var db = writer.GetDatabase();
        const string key = "catalog:cache:data:catalog:eu";
        await db.StringSetAsync(key, "old");
        await NextAsync();
        await db.StringSetAsync(key, "new");
        await NextAsync();
        await db.KeyDeleteAsync(key);
        await NextAsync();
        await db.StringSetAsync(key, "old");
        await NextAsync();
        await db.ExecuteAsync("UNLINK", key);
        await NextAsync();
        await db.StringSetAsync(key, "old", TimeSpan.FromMilliseconds(100));
        await NextAsync();
        await NextAsync();
        async Task NextAsync() =>
            Assert.Equal(
                "catalog:eu",
                await received
                    .Reader.ReadAsync(TestContext.Current.CancellationToken)
                    .AsTask()
                    .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken)
            );
    }

    [Theory(Timeout = 40_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Disconnected_disposal_detaches_current_and_late_SDK_subscriptions(
        bool pendingSubscribe
    )
    {
        using var real = await ConnectionMultiplexer.ConnectAsync(_configuration);
        var subscriber = real.GetSubscriber();
        var wrapper = Substitute.For<IConnectionMultiplexer>();
        wrapper.ClientName.Returns(real.ClientName);
        wrapper.IsConnected.Returns(true);
        wrapper.GetSubscriber(Arg.Any<object>()).Returns(subscriber);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        if (pendingSubscribe)
        {
            var delayed = Substitute.For<ISubscriber>();
            delayed
                .SubscribeAsync(Arg.Any<RedisChannel>(), Arg.Any<CommandFlags>())
                .Returns(async call =>
                {
                    var queue = await subscriber.SubscribeAsync(call.Arg<RedisChannel>());
                    entered.TrySetResult();
                    await release.Task.WaitAsync(
                        TimeSpan.FromSeconds(10),
                        TestContext.Current.CancellationToken
                    );
                    return queue;
                });
            wrapper.GetSubscriber(Arg.Any<object>()).Returns(delayed);
        }
        await using var connection = new RedisConnection(wrapper);
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "catalog" }.WithMode(CacheInvalidationMode.Broadcast)
        );
        var delivered = 0;
        using var observer = channel.Subscribe(_ => Interlocked.Increment(ref delivered));
        if (pendingSubscribe)
        {
            await entered.Task.WaitAsync(
                TimeSpan.FromSeconds(10),
                TestContext.Current.CancellationToken
            );
        }
        else
        {
            await ReadyAsync(channel, RedisInvalidationTransport.Broadcast);
        }
        wrapper.IsConnected.Returns(false);
        var disposing = channel.DisposeAsync().AsTask();
        if (pendingSubscribe)
        {
            Assert.False(disposing.IsCompleted);
            release.SetResult();
        }
        await disposing.WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        // Redis reports zero live subscribers once the SDK has detached our only queue.
        Assert.Equal(
            0,
            await subscriber.PublishAsync(
                RedisChannel.Literal(channel.ChannelName),
                "v1\nkcatalog:eu"
            )
        );
        Assert.Equal(0, Volatile.Read(ref delivered));
        Assert.Null(subscriber.SubscribedEndpoint(RedisChannel.Literal(channel.ChannelName)));
    }

    [Fact(Timeout = 40_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task Reconnect_flush_reaches_healthy_observers_after_an_observer_throws()
    {
        using var real = await ConnectionMultiplexer.ConnectAsync(_configuration);
        var wrapper = Substitute.For<IConnectionMultiplexer>();
        wrapper.ClientName.Returns(real.ClientName);
        wrapper.IsConnected.Returns(true);
        wrapper.GetSubscriber(Arg.Any<object>()).Returns(real.GetSubscriber());
        await using var connection = new RedisConnection(wrapper);
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "catalog" }.WithMode(CacheInvalidationMode.Broadcast)
        );
        using var failing = channel.Subscribe(_ =>
            throw new InvalidOperationException("observer failed")
        );
        var delivered = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        using var healthy = channel.Subscribe(i =>
        {
            if (i.FlushAll)
            {
                delivered.TrySetResult();
            }
        });
        await ReadyAsync(channel, RedisInvalidationTransport.Broadcast);
        wrapper.ConnectionRestored += Raise.EventWith(
            new ConnectionFailedEventArgs(
                wrapper,
                new IPEndPoint(IPAddress.Loopback, 6379),
                ConnectionType.Subscription,
                ConnectionFailureType.SocketFailure,
                null!,
                null!
            )
        );
        await delivered.Task.WaitAsync(
            TimeSpan.FromSeconds(10),
            TestContext.Current.CancellationToken
        );
    }

    private RedisConnection Connection() =>
        new(new RedisOptions { Configuration = _configuration });

    private static Task ReadyAsync(
        RedisCacheInvalidationChannel channel,
        RedisInvalidationTransport mode
    ) => CacheConformance.WaitUntilAsync(() => Task.FromResult(channel.Transport == mode));

    private static async Task<string> DockerAsync(params string[] arguments)
    {
        var start = new ProcessStartInfo("docker")
        {
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            UseShellExecute = false,
        };
        foreach (var argument in arguments)
        {
            start.ArgumentList.Add(argument);
        }
        using var process =
            Process.Start(start) ?? throw new InvalidOperationException("Could not start Docker.");
        var output = process.StandardOutput.ReadToEndAsync(CancellationToken.None);
        var error = process.StandardError.ReadToEndAsync(CancellationToken.None);
        using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(60));
        try
        {
            await process.WaitForExitAsync(deadline.Token);
        }
        catch
        {
            process.Kill(entireProcessTree: true);
            throw;
        }
        var stderr = await error;
        if (process.ExitCode != 0)
        {
            throw new InvalidOperationException(stderr);
        }
        return await output;
    }
}
