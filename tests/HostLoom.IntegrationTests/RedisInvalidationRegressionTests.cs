using System.Collections;
using System.Diagnostics;
using System.Globalization;
using System.Net;
using System.Reflection;
using System.Threading.Channels;
using HostLoom.Caching;
using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Redis;
using HostLoom.Redis.Internal;
using Microsoft.Extensions.Logging;
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
    private int _port;
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
                // The image declares /data a volume; tmpfs keeps each case from leaving one behind.
                "--tmpfs",
                "/data",
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
            var endpoint = (await DockerAsync("port", _container, "6379/tcp")).Trim();
            _configuration = endpoint + ",allowAdmin=true";
            _port = int.Parse(
                endpoint[(endpoint.LastIndexOf(':') + 1)..],
                CultureInfo.InvariantCulture
            );
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
            // A forced removal bypasses --rm's own cleanup, so anonymous volumes go explicitly.
            await DockerAsync("rm", "--force", "--volumes", _container);
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
                .SubscribeAsync(
                    Arg.Any<RedisChannel>(),
                    Arg.Any<Action<RedisChannel, RedisValue>>(),
                    Arg.Any<CommandFlags>()
                )
                .Returns(async call =>
                {
                    await subscriber.SubscribeAsync(
                        call.Arg<RedisChannel>(),
                        call.Arg<Action<RedisChannel, RedisValue>>(),
                        call.Arg<CommandFlags>()
                    );
                    entered.TrySetResult();
                    await release.Task.WaitAsync(
                        TimeSpan.FromSeconds(10),
                        TestContext.Current.CancellationToken
                    );
                });
            delayed
                .UnsubscribeAsync(
                    Arg.Any<RedisChannel>(),
                    Arg.Any<Action<RedisChannel, RedisValue>?>(),
                    Arg.Any<CommandFlags>()
                )
                .Returns(call =>
                    subscriber.UnsubscribeAsync(
                        call.Arg<RedisChannel>(),
                        call.Arg<Action<RedisChannel, RedisValue>?>(),
                        call.Arg<CommandFlags>()
                    )
                );
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
        Assert.Empty(Attachments(real));
    }

    [Fact(Timeout = 60_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task Failed_subscribe_attempts_leave_one_attachment_per_channel_after_recovery()
    {
        var token = TestContext.Current.CancellationToken;
        await using var proxy = new TcpFaultProxy(TcpFaultProxy.Host, _port);
        proxy.SetEnabled(false);
        var log = new SubscribeFailureLog();
        await using var connection = new RedisConnection(
            new RedisOptions
            {
                Configuration = proxy.Configuration,
                ConnectTimeout = TimeSpan.FromSeconds(1),
                CommandTimeout = TimeSpan.FromSeconds(1),
                InitialRetryDelay = TimeSpan.FromMilliseconds(50),
            }
        );
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "catalog" }.WithMode(CacheInvalidationMode.Tracking),
            log
        );
        var received = Channel.CreateUnbounded<CacheInvalidation>();
        using var observer = channel.Subscribe(i => received.Writer.TryWrite(i));

        // The SDK attaches a subscriber before SUBSCRIBE fails. At most the attempt in flight may
        // be attached while Redis is unreachable, never one per failed attempt.
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(log.Failures >= 4), 30);
        var multiplexer = await connection.GetMultiplexerAsync(token);
        var whileDown = Attachments(multiplexer);
        Assert.True(whileDown.Values.All(count => count.Total <= 1), Describe(whileDown));

        proxy.SetEnabled(true);
        await CacheConformance.WaitUntilAsync(
            () => Task.FromResult(channel.Transport == RedisInvalidationTransport.Tracking),
            30
        );
        Assert.True(log.Failures >= 4);
        Assert.Equal(
            $"__redis__:invalidate=1; {channel.ChannelName}=1",
            Totals(Attachments(multiplexer))
        );

        using var publisher = await ConnectionMultiplexer.ConnectAsync(_configuration);
        var keys = Enumerable.Range(0, 20).Select(i => "catalog:eu" + i).ToArray();
        foreach (var key in keys)
        {
            Assert.Equal(
                1,
                await publisher
                    .GetSubscriber()
                    .PublishAsync(RedisChannel.Literal(channel.ChannelName), "v1\nk" + key)
            );
        }

        var delivered = new List<string>();
        while (delivered.Count < keys.Length)
        {
            var invalidation = await received
                .Reader.ReadAsync(token)
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(10), token);
            delivered.AddRange(invalidation.Keys);
        }

        Assert.Equal(keys.Order(StringComparer.Ordinal), delivered.Order(StringComparer.Ordinal));
        Assert.Equal(
            $"__redis__:invalidate=1; {channel.ChannelName}=1",
            Totals(Attachments(multiplexer))
        );
    }

    [Theory(Timeout = 60_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(CacheInvalidationMode.Tracking)]
    [InlineData(CacheInvalidationMode.Broadcast)]
    public async Task Disposal_detaches_only_what_the_channel_attached_to_an_external_multiplexer(
        CacheInvalidationMode mode
    )
    {
        await using var proxy = new TcpFaultProxy(TcpFaultProxy.Host, _port);
        var configuration = ConfigurationOptions.Parse(proxy.Configuration);
        configuration.AbortOnConnectFail = false;
        configuration.AllowAdmin = true;
        configuration.ClientName = "hostloom-external-" + Guid.NewGuid().ToString("N");
        configuration.ConnectTimeout = 1_000;
        configuration.AsyncTimeout = 1_000;
        configuration.SyncTimeout = 1_000;
        configuration.Protocol = RedisProtocol.Resp2;
        await using var external = await ConnectionMultiplexer.ConnectAsync(configuration);

        // Another component on the same multiplexer and channel name keeps its subscription.
        var shared = RedisChannel.Literal("catalog:cache:invalidate");
        var neighbour = Channel.CreateUnbounded<string>();
        await external
            .GetSubscriber()
            .SubscribeAsync(shared, (_, message) => neighbour.Writer.TryWrite(message.ToString()));
        var before = Describe(Attachments(external));

        // Start while Redis is unreachable, so several subscribe attempts fail first.
        proxy.SetEnabled(false);
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(!external.IsConnected), 20);
        var log = new SubscribeFailureLog();
        await using var connection = new RedisConnection(
            external,
            new RedisOptions
            {
                Configuration = "external",
                InitialRetryDelay = TimeSpan.FromMilliseconds(50),
            }
        );
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "catalog" }.WithMode(mode),
            log
        );
        var delivered = 0;
        using var observer = channel.Subscribe(i =>
        {
            if (!i.FlushAll)
            {
                Interlocked.Increment(ref delivered);
            }
        });
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(log.Failures >= 3), 30);
        proxy.SetEnabled(true);
        await CacheConformance.WaitUntilAsync(
            () =>
                Task.FromResult(
                    channel.Transport
                        == (
                            mode == CacheInvalidationMode.Tracking
                                ? RedisInvalidationTransport.Tracking
                                : RedisInvalidationTransport.Broadcast
                        )
                ),
            30
        );
        using var publisher = await ConnectionMultiplexer.ConnectAsync(_configuration);
        await publisher.GetSubscriber().PublishAsync(shared, "v1\nkcatalog:eu");
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(Volatile.Read(ref delivered) == 1)
        );
        Assert.Equal("v1\nkcatalog:eu", await ReceiveAsync(neighbour));

        // Dispose while the multiplexer is disconnected: the case in which a failed attempt's
        // queue used to stay behind on the externally owned multiplexer.
        proxy.SetEnabled(false);
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(!external.IsConnected), 20);
        await channel
            .DisposeAsync()
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        Assert.Equal(before, Describe(Attachments(external)));

        proxy.SetEnabled(true);
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(external.IsConnected), 20);
        // The SDK restores the neighbour's subscription after the reconnect; publish until it is back.
        await CacheConformance.WaitUntilAsync(
            async () => await publisher.GetSubscriber().PublishAsync(shared, "v1\nkcatalog:us") > 0,
            20
        );
        Assert.Equal("v1\nkcatalog:us", await ReceiveAsync(neighbour));
        Assert.Equal(before, Describe(Attachments(external)));
        Assert.Equal(1, Volatile.Read(ref delivered));
    }

    [Fact(Timeout = 60_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task Refused_tracking_subscriptions_leave_one_attachment_once_permitted()
    {
        var token = TestContext.Current.CancellationToken;
        using var admin = await ConnectionMultiplexer.ConnectAsync(_configuration);
        var server = admin.GetServer(admin.GetEndPoints()[0]);
        // Everything except the tracking redirect channel, so each SUBSCRIBE to it fails with
        // NOPERM on a healthy connection.
        await server.ExecuteAsync(
            "ACL",
            "SETUSER",
            "catalog-svc",
            "on",
            ">catalog-secret",
            "~*",
            "resetchannels",
            "&catalog:cache:invalidate",
            "&__Booksleeve_MasterChanged",
            "+@all"
        );
        await using var proxy = new TcpFaultProxy(TcpFaultProxy.Host, _port);
        var log = new SubscribeFailureLog();
        await using var connection = new RedisConnection(
            new RedisOptions
            {
                Configuration = proxy.Configuration + ",user=catalog-svc,password=catalog-secret",
                ConnectTimeout = TimeSpan.FromSeconds(1),
                CommandTimeout = TimeSpan.FromSeconds(1),
                InitialRetryDelay = TimeSpan.FromMilliseconds(50),
                MaxClientCommandRetries = 3,
            }
        );
        await using var channel = new RedisCacheInvalidationChannel(
            connection,
            new CachingOptions { Namespace = "catalog" }.WithMode(CacheInvalidationMode.Tracking),
            log
        );
        var keys = Channel.CreateUnbounded<string>();
        using var observer = channel.Subscribe(i =>
        {
            foreach (var key in i.Keys)
            {
                keys.Writer.TryWrite(key);
            }
        });
        await CacheConformance.WaitUntilAsync(
            () => Task.FromResult(channel.Transport == RedisInvalidationTransport.ExplicitOnly),
            30
        );
        Assert.Equal(1, log.TrackingUnavailable);
        var multiplexer = await connection.GetMultiplexerAsync(token);
        Assert.Equal($"{channel.ChannelName}=1", Totals(Attachments(multiplexer)));

        // Grant the channel and reconnect, which retries tracking registration.
        await server.ExecuteAsync("ACL", "SETUSER", "catalog-svc", "&__redis__:invalidate");
        var registrations = channel.TrackingInitialisations;
        proxy.SetEnabled(false);
        proxy.SetEnabled(true);
        await CacheConformance.WaitUntilAsync(
            () =>
                Task.FromResult(
                    channel.Transport == RedisInvalidationTransport.Tracking
                        && channel.TrackingInitialisations > registrations
                ),
            30
        );
        Assert.Equal(
            $"__redis__:invalidate=1; {channel.ChannelName}=1",
            Totals(Attachments(multiplexer))
        );

        await admin.GetDatabase().StringSetAsync("catalog:cache:data:catalog:eu", "new");
        Assert.Equal("catalog:eu", await ReceiveAsync(keys));
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

    /// <summary>
    /// Handlers and queues attached per channel, read from the multiplexer's own subscription
    /// table. These are StackExchange.Redis internals, pinned by the central package version.
    /// </summary>
    private static SortedDictionary<string, Attached> Attachments(
        IConnectionMultiplexer multiplexer
    )
    {
        const BindingFlags Internal = BindingFlags.Instance | BindingFlags.NonPublic;
        var table =
            typeof(ConnectionMultiplexer)
                .GetMethod("GetSubscriptions", Internal)
                ?.Invoke(multiplexer, null) as IDictionary
            ?? throw new InvalidOperationException("The SDK subscription table is not readable.");
        var counts =
            typeof(ConnectionMultiplexer).GetMethod("GetSubscriberCounts", Internal)
            ?? throw new InvalidOperationException("The SDK subscriber counts are not readable.");
        var attachments = new SortedDictionary<string, Attached>(StringComparer.Ordinal);
        foreach (DictionaryEntry entry in table)
        {
            object?[] arguments = [entry.Key, 0, 0];
            counts.Invoke(multiplexer, arguments);
            attachments[entry.Key.ToString()!] = new Attached(
                (int)arguments[1]!,
                (int)arguments[2]!
            );
        }

        return attachments;
    }

    private static string Describe(SortedDictionary<string, Attached> attachments) =>
        string.Join(
            "; ",
            attachments.Select(pair =>
                $"{pair.Key}: {pair.Value.Handlers} handler(s), {pair.Value.Queues} queue(s)"
            )
        );

    private static string Totals(SortedDictionary<string, Attached> attachments) =>
        string.Join("; ", attachments.Select(pair => $"{pair.Key}={pair.Value.Total}"));

    private static Task<string> ReceiveAsync(Channel<string> received) =>
        received
            .Reader.ReadAsync(TestContext.Current.CancellationToken)
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);

    private readonly record struct Attached(int Handlers, int Queues)
    {
        public int Total => Handlers + Queues;
    }

    /// <summary>Counts failed subscribe attempts and tracking fallbacks by event ID.</summary>
    private sealed class SubscribeFailureLog : ILogger<RedisCacheInvalidationChannel>
    {
        private int _failures;
        private int _trackingUnavailable;

        public int Failures => Volatile.Read(ref _failures);

        public int TrackingUnavailable => Volatile.Read(ref _trackingUnavailable);

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        )
        {
            if (eventId.Id == 1312)
            {
                Interlocked.Increment(ref _failures);
            }
            else if (eventId.Id == 1315)
            {
                Interlocked.Increment(ref _trackingUnavailable);
            }
        }
    }

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
