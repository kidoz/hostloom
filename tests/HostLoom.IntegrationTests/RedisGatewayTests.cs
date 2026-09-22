using System.Net;
using System.Net.Sockets;
using HostLoom.Caching;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Locking;
using HostLoom.Redis;
using StackExchange.Redis;
using StackExchange.Redis.Configuration;
using Xunit;

namespace HostLoom.IntegrationTests;

[Collection(nameof(RedisGatewayTests))]
[CollectionDefinition(nameof(RedisGatewayTests), DisableParallelization = true)]
public sealed class RedisGatewayTests
{
    public static bool Enabled =>
        Environment.GetEnvironmentVariable("HOSTLOOM_REDIS_GATEWAY") == "1"
        && !string.IsNullOrEmpty(Environment.GetEnvironmentVariable(RedisClusterFixture.Variable));
    private const string Skip =
        "Run scripts/test-redis-cluster.py --haproxy for an owned gateway and cluster.";

    [Fact(Timeout = 120_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task SingleEndpointOnly_RejectsUnreachableShardAndReplicaWritesEvenAfterRecreation()
    {
        var token = TestContext.Current.CancellationToken;
        var fixture = new RedisClusterFixture();
        // This separate control connection is never used for application cache/lock traffic.
        await using var control = new RedisConnection(fixture.Options());
        var (ns, primary, replica) = await fixture.SelectShardAsync(control, 2, token);
        var gatewayPort = fixture.GatewayPort!.Value;
        var tunnel = new GatewayOnlyTunnel(gatewayPort);
        var options = fixture.GatewayOptions();
        options.ConfigurationOptions = ConfigurationOptions.Parse(options.Configuration!);
        options.ConfigurationOptions.Tunnel = tunnel;

        await using var connection = new RedisConnection(options);
        var mux = await connection.GetMultiplexerAsync(token);
        var gateway = mux.GetServer("127.0.0.1", gatewayPort);
        await gateway.PingAsync().WaitAsync(token);
        await AssertShardUnavailableAsync(connection);
        Assert.True(tunnel.RejectedConnections > 0);

        try
        {
            await fixture.PromoteAsync(replica, token);
            await AssertShardUnavailableAsync(connection);
            await using var afterDemotion = new RedisConnection(options);
            await AssertShardUnavailableAsync(afterDemotion);
        }
        finally
        {
            using var recovery = new CancellationTokenSource(TimeSpan.FromSeconds(40));
            await fixture.PromoteAsync(primary, recovery.Token);
        }

        try
        {
            // Deliberately override the owned proxy's primary health check and close its
            // sessions, representing a misrouted write endpoint. Restart restores the config.
            await fixture.RouteGatewayToReplicaAsync(replica, token);
            await RedisClusterFixture.PollAsync(
                async () =>
                {
                    try
                    {
                        var id = (string?)
                            await gateway.ExecuteAsync("CLUSTER", "MYID").WaitAsync(token);
                        var info = (string?)
                            await gateway.ExecuteAsync("INFO", "replication").WaitAsync(token);
                        return id == replica.NodeId
                            && info?.Contains("role:slave", StringComparison.Ordinal) == true;
                    }
                    catch (RedisException)
                    {
                        return false;
                    }
                },
                token
            );
            var rejection = await Assert.ThrowsAsync<RedisServerException>(async () =>
                await gateway
                    .ExecuteAsync(
                        "EVAL",
                        "return redis.call('SET', ARGV[1], 'rejected')",
                        "0",
                        "{" + ns + "}:cache:data:gateway-readonly"
                    )
                    .WaitAsync(token)
            );
            Assert.Contains("READONLY", rejection.Message, StringComparison.Ordinal);
            TestContext.Current.TestOutputHelper!.WriteLine(
                "Write through HAProxy to the forced replica: " + rejection.Message
            );
            await AssertShardUnavailableAsync(connection);

            await using var recreated = new RedisConnection(options);
            Assert.NotSame(mux, await recreated.GetMultiplexerAsync(token));
            await AssertShardUnavailableAsync(recreated);
        }
        finally
        {
            using var recovery = new CancellationTokenSource(TimeSpan.FromSeconds(40));
            await fixture.RestartGatewayAsync(recovery.Token);
            await RedisClusterFixture.PollAsync(
                async () =>
                {
                    try
                    {
                        var info = (string?)
                            await gateway
                                .ExecuteAsync("INFO", "replication")
                                .WaitAsync(recovery.Token);
                        return info?.Contains("role:master", StringComparison.Ordinal) == true;
                    }
                    catch (RedisException)
                    {
                        return false;
                    }
                },
                recovery.Token
            );
        }

        await using var freshAfterRecovery = new RedisConnection(options);
        await AssertShardUnavailableAsync(freshAfterRecovery);
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Single endpoint limitation confirmed: {tunnel.RejectedConnections} direct-node connection attempts blocked; recreating the connection did not provide shard routing."
        );

        async Task AssertShardUnavailableAsync(RedisConnection client)
        {
            await using var store = new RedisCacheStore(client);
            await Assert.ThrowsAsync<CacheStoreException>(() =>
                store
                    .SetAsync(
                        ns + ":cache:data:catalog",
                        new byte[] { 42 },
                        TimeSpan.FromSeconds(30),
                        cancellationToken: token
                    )
                    .AsTask()
            );
            await using var provider = new RedisLockProvider(client);
            await using var mutex = new DistributedLock(
                new LockingOptions { Namespace = ns },
                provider
            );
            var ran = false;
            await Assert.ThrowsAsync<LockProviderUnavailableException>(() =>
                mutex
                    .ExecuteWithLockAsync(
                        "inventory",
                        _ =>
                        {
                            ran = true;
                            return ValueTask.CompletedTask;
                        },
                        cancellationToken: token
                    )
                    .AsTask()
            );
            Assert.False(ran);
        }
    }

    private sealed class GatewayOnlyTunnel(int port) : Tunnel
    {
        private int _rejected;
        public int RejectedConnections => Volatile.Read(ref _rejected);

        public override ValueTask BeforeSocketConnectAsync(
            EndPoint endpoint,
            ConnectionType connectionType,
            Socket? socket,
            CancellationToken cancellationToken = default
        )
        {
            var allowed = endpoint switch
            {
                IPEndPoint ip => IPAddress.IsLoopback(ip.Address) && ip.Port == port,
                DnsEndPoint dns => dns.Host == "127.0.0.1" && dns.Port == port,
                _ => false,
            };
            if (!allowed)
            {
                Interlocked.Increment(ref _rejected);
                throw new SocketException((int)SocketError.AccessDenied);
            }
            return ValueTask.CompletedTask;
        }
    }

    [Fact(Timeout = 120_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task GatewayBootstrap_DiscoversShardsAndReconnectsAfterGatewayRestart()
    {
        var token = TestContext.Current.CancellationToken;
        var fixture = new RedisClusterFixture();
        await using var connection = new RedisConnection(fixture.GatewayOptions());
        var mux = await connection.GetMultiplexerAsync(token);
        await using var store = new RedisCacheStore(connection);
        var keys = new List<string>();
        for (var shard = 0; shard < 3; shard++)
        {
            var (ns, primary, _) = await fixture.SelectShardAsync(connection, shard, token);
            var key = ns + ":cache:data:catalog";
            keys.Add(key);
            await store.SetAsync(
                key,
                new byte[] { 42 },
                TimeSpan.FromMinutes(2),
                cancellationToken: token
            );
            Assert.Equal(
                new byte[] { 42 },
                (await store.GetAsync(key, token))!.Value.Payload.ToArray()
            );
            var route = await mux.GetDatabase()
                .IdentifyEndpointAsync("{" + ns + "}:cache:data:catalog");
            Assert.Equal(primary.EndPoint, route);
            Assert.NotEqual(mux.GetServer("127.0.0.1", fixture.GatewayPort!.Value).EndPoint, route);
            TestContext.Current.TestOutputHelper!.WriteLine(
                $"Gateway bootstrap discovered direct shard route {route}."
            );
        }

        var gateway = mux.GetServer("127.0.0.1", fixture.GatewayPort!.Value);
        await gateway.PingAsync().WaitAsync(token);
        var reconnects = connection.Reconnects;
        await fixture.RestartGatewayAsync(token);
        await RedisClusterFixture.PollAsync(
            async () =>
            {
                try
                {
                    await gateway.PingAsync().WaitAsync(token);
                    return connection.Reconnects > reconnects;
                }
                catch (RedisException)
                {
                    return false;
                }
            },
            token
        );
        Assert.Same(mux, await connection.GetMultiplexerAsync(token));
        foreach (var key in keys)
            Assert.Equal(
                new byte[] { 42 },
                (await store.GetAsync(key, token))!.Value.Payload.ToArray()
            );

        await using var recreated = new RedisConnection(fixture.GatewayOptions());
        Assert.NotSame(mux, await recreated.GetMultiplexerAsync(token));
        await using var recreatedStore = new RedisCacheStore(recreated);
        foreach (var key in keys)
            Assert.Equal(
                new byte[] { 42 },
                (await recreatedStore.GetAsync(key, token))!.Value.Payload.ToArray()
            );
        await using var provider = new RedisLockProvider(recreated);
        var lockKey = "gateway-" + Guid.NewGuid().ToString("N") + ":lock:inventory";
        Assert.True(
            await provider.TryAcquireAsync(lockKey, "owner", TimeSpan.FromSeconds(5), token)
        );
        Assert.False(await provider.ReleaseAsync(lockKey, "stale-owner", token));
        Assert.True(await provider.ReleaseAsync(lockKey, "owner", token));
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Existing multiplexer restored; reconnect events: {connection.Reconnects - reconnects}. Fresh connection also read all shards and acquired/released a lock."
        );
    }

    [Theory(Timeout = 120_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    public async Task DemotedPrimary_RejectsWritesAndExistingAndFreshConnectionsRecover(int shard)
    {
        var token = TestContext.Current.CancellationToken;
        var fixture = new RedisClusterFixture();
        await using var connection = new RedisConnection(fixture.GatewayOptions());
        await using var store = new RedisCacheStore(connection);
        await using var provider = new RedisLockProvider(connection);
        var (ns, primary, replica) = await fixture.SelectShardAsync(connection, shard, token);
        await using var mutex = new DistributedLock(
            new LockingOptions { Namespace = ns },
            provider
        );
        await using var held = await mutex.TryAcquireAsync(
            "inventory",
            new LockOptions { Lease = TimeSpan.FromSeconds(60), AutoExtend = false },
            token
        );
        Assert.NotNull(held);
        var mux = await connection.GetMultiplexerAsync(token);
        var db = mux.GetDatabase();
        var lockKey = "{" + ns + "}:lock:inventory";
        var owner = await db.StringGetAsync(lockKey).WaitAsync(token);
        await RedisClusterFixture.WaitReplicatedAsync(db, lockKey, owner, token);
        var key = ns + ":cache:data:catalog";
        await store.SetAsync(
            key,
            new byte[] { 1 },
            TimeSpan.FromMinutes(2),
            cancellationToken: token
        );
        await RedisClusterFixture.WaitReplicatedAsync(
            db,
            "{" + ns + "}:cache:data:catalog",
            new byte[] { 1 },
            token
        );
        try
        {
            await fixture.PromoteAsync(replica, token);
            await RedisClusterFixture.WaitForClientRoutingAsync(
                connection,
                lockKey,
                replica,
                token
            );
            var formerPrimary = mux.GetServer(primary.EndPoint!);
            await RedisClusterFixture.PollAsync(
                async () =>
                {
                    var info = (string?)
                        await formerPrimary.ExecuteAsync("INFO", "replication").WaitAsync(token);
                    return info?.Contains("role:slave", StringComparison.Ordinal) == true
                        && info.Contains("master_link_status:up", StringComparison.Ordinal);
                },
                token
            );
            // A keyless write script is sent to this exact server, so neither the SDK nor a
            // MOVED response routes this probe to the new primary. It must reject writes.
            var readOnly = await Assert.ThrowsAsync<RedisServerException>(async () =>
                await formerPrimary
                    .ExecuteAsync(
                        "EVAL",
                        "return redis.call('SET', ARGV[1], 'rejected')",
                        "0",
                        "{" + ns + "}:cache:data:readonly-probe"
                    )
                    .WaitAsync(token)
            );
            Assert.Contains("READONLY", readOnly.Message, StringComparison.Ordinal);
            TestContext.Current.TestOutputHelper!.WriteLine(
                $"Former primary {primary.EndPoint}: {readOnly.Message}"
            );
            Assert.False(
                await db.KeyExistsAsync("{" + ns + "}:cache:data:readonly-probe").WaitAsync(token)
            );

            await RedisClusterFixture.PollAsync(
                async () =>
                {
                    try
                    {
                        return await held.ExtendAsync(TimeSpan.FromSeconds(60), token);
                    }
                    catch (LockProviderUnavailableException)
                    {
                        return false;
                    }
                },
                token
            );
            Assert.True(held.IsHeld);
            Assert.Equal(owner, await db.StringGetAsync(lockKey).WaitAsync(token));
            await store.SetAsync(
                key,
                new byte[] { 2 },
                TimeSpan.FromMinutes(2),
                cancellationToken: token
            );
            Assert.Equal(
                new byte[] { 2 },
                (await store.GetAsync(key, token))!.Value.Payload.ToArray()
            );
            Assert.Same(mux, await connection.GetMultiplexerAsync(token));

            await using var recreated = new RedisConnection(fixture.GatewayOptions());
            await using var freshStore = new RedisCacheStore(recreated);
            await using var freshProvider = new RedisLockProvider(recreated);
            Assert.NotSame(mux, await recreated.GetMultiplexerAsync(token));
            Assert.Equal(
                new byte[] { 2 },
                (await freshStore.GetAsync(key, token))!.Value.Payload.ToArray()
            );
            Assert.False(
                await freshProvider.TryAcquireAsync(
                    ns + ":lock:inventory",
                    "successor",
                    TimeSpan.FromSeconds(30),
                    token
                )
            );
            Assert.False(
                await freshProvider.ReleaseAsync(ns + ":lock:inventory", "stale-owner", token)
            );
            await held.DisposeAsync();
            Assert.True(
                await freshProvider.TryAcquireAsync(
                    ns + ":lock:inventory",
                    "successor",
                    TimeSpan.FromSeconds(30),
                    token
                )
            );
            Assert.False(
                await provider.ReleaseAsync(ns + ":lock:inventory", owner.ToString(), token)
            );
            Assert.False(
                await provider.ExtendAsync(
                    ns + ":lock:inventory",
                    owner.ToString(),
                    TimeSpan.FromSeconds(60),
                    token
                )
            );
            Assert.True(
                await freshProvider.ReleaseAsync(ns + ":lock:inventory", "successor", token)
            );
        }
        finally
        {
            using var recovery = new CancellationTokenSource(TimeSpan.FromSeconds(40));
            await fixture.PromoteAsync(primary, recovery.Token);
            await RedisClusterFixture.WaitForClientRoutingAsync(
                connection,
                lockKey,
                primary,
                recovery.Token
            );
        }
    }
}
