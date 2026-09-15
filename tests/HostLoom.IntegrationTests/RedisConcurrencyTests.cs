using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Conformance;
using HostLoom.Locking;
using HostLoom.Redis;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

[Collection(nameof(RedisConcurrencyTests))]
[CollectionDefinition(nameof(RedisConcurrencyTests), DisableParallelization = true)]
public sealed class RedisConcurrencyTests
{
    public static bool Available => RedisAvailability.Redis;

    [Theory(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ContendedLocks_ProtectReadModifyWriteAcrossEightConnections(bool hashTags)
    {
        var token = TestContext.Current.CancellationToken;
        await using var clients = new Clients(hashTags, 8);
        var db = await clients.Connections[0].GetDatabaseAsync(token);
        var counterKey = clients.Namespace + ":counter";
        await db.StringSetAsync(counterKey, 0, TimeSpan.FromMinutes(1));
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var active = 0;
        var completed = 0;
        var workers = clients
            .Locks.Select(async mutex =>
            {
                await gate.Task.WaitAsync(token);
                for (var iteration = 0; iteration < 16; iteration++)
                {
                    await mutex.ExecuteWithLockAsync(
                        "inventory",
                        async cancellationToken =>
                        {
                            Assert.Equal(1, Interlocked.Increment(ref active));
                            try
                            {
                                // Deliberately non-atomic: losing exclusion loses an increment.
                                var before = (int)
                                    await db.StringGetAsync(counterKey)
                                        .WaitAsync(cancellationToken);
                                await Task.Yield();
                                await db.StringSetAsync(
                                        counterKey,
                                        before + 1,
                                        TimeSpan.FromMinutes(1)
                                    )
                                    .WaitAsync(cancellationToken);
                                Interlocked.Increment(ref completed);
                            }
                            finally
                            {
                                Interlocked.Decrement(ref active);
                            }
                        },
                        new LockOptions
                        {
                            Lease = TimeSpan.FromSeconds(15),
                            MaxWait = TimeSpan.FromSeconds(20),
                            Retry = LockRetryPolicy.Linear(200, TimeSpan.FromMilliseconds(1)),
                        },
                        token
                    );
                }
            })
            .ToArray();
        gate.SetResult();
        await Task.WhenAll(workers).WaitAsync(token);

        Assert.Equal(128, completed);
        Assert.Equal(128, (int)await db.StringGetAsync(counterKey));
        Assert.Equal(0, active);
        Assert.False(await db.KeyExistsAsync(clients.Key("lock:inventory")));
    }

    [Theory(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SetIfAbsent_HasOneWinnerAndOnlyIndexesItsTags(bool hashTags)
    {
        var token = TestContext.Current.CancellationToken;
        await using var clients = new Clients(hashTags, 8);
        var key = clients.Namespace + ":cache:data:catalog";
        var gate = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var attempts = Enumerable
            .Range(0, 128)
            .Select(async index =>
            {
                await gate.Task.WaitAsync(token);
                return await clients
                    .Stores[index % 8]
                    .SetIfAbsentAsync(
                        key,
                        BitConverter.GetBytes(index),
                        TimeSpan.FromSeconds(30),
                        [clients.Namespace + ":cache:tag:catalog-" + index],
                        token
                    );
            })
            .ToArray();
        gate.SetResult();
        var outcomes = await Task.WhenAll(attempts).WaitAsync(token);
        var winner = Assert.Single(Enumerable.Range(0, outcomes.Length), i => outcomes[i]);
        var stored = await clients.Stores[0].GetAsync(key, token);
        Assert.NotNull(stored);
        Assert.Equal(winner, BitConverter.ToInt32(stored.Value.Payload.Span));
        Assert.InRange(
            stored.Value.RemainingTimeToLive!.Value,
            TimeSpan.Zero,
            TimeSpan.FromSeconds(30)
        );

        var db = await clients.Connections[0].GetDatabaseAsync(token);
        for (var index = 0; index < outcomes.Length; index++)
        {
            var members = await db.SetMembersAsync(clients.Key("cache:tag:catalog-" + index));
            if (index == winner)
                Assert.Equal(clients.Key("cache:data:catalog"), (string?)Assert.Single(members));
            else
                Assert.Empty(members);
        }
    }

    [Theory(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ExpiredOwner_CannotExtendOrReleaseSuccessor(bool hashTags)
    {
        var token = TestContext.Current.CancellationToken;
        await using var clients = new Clients(hashTags, 2);
        var key = clients.Namespace + ":lock:inventory";
        var db = await clients.Connections[0].GetDatabaseAsync(token);
        Assert.True(
            await clients
                .Providers[0]
                .TryAcquireAsync(key, "previous", TimeSpan.FromMilliseconds(200), token)
        );
        await CacheConformance.WaitUntilAsync(async () =>
            !await db.KeyExistsAsync(clients.Key("lock:inventory"))
        );
        Assert.True(
            await clients
                .Providers[1]
                .TryAcquireAsync(key, "current", TimeSpan.FromSeconds(30), token)
        );

        var staleOperations = Enumerable
            .Range(0, 64)
            .Select(async _ =>
            {
                Assert.False(
                    await clients
                        .Providers[0]
                        .ExtendAsync(key, "previous", TimeSpan.FromMinutes(1), token)
                );
                Assert.False(await clients.Providers[0].ReleaseAsync(key, "previous", token));
            });
        await Task.WhenAll(staleOperations).WaitAsync(token);

        Assert.Equal("current", (string?)await db.StringGetAsync(clients.Key("lock:inventory")));
        Assert.InRange(
            (await db.KeyTimeToLiveAsync(clients.Key("lock:inventory")))!.Value,
            TimeSpan.FromSeconds(10),
            TimeSpan.FromSeconds(30)
        );
        Assert.True(
            await clients.Providers[1].ExtendAsync(key, "current", TimeSpan.FromSeconds(45), token)
        );
        Assert.True(await clients.Providers[1].ReleaseAsync(key, "current", token));
        Assert.False(await db.KeyExistsAsync(clients.Key("lock:inventory")));
    }

    [Theory(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task AutomaticExtension_RetainsOwnershipBeyondOriginalLease(bool hashTags)
    {
        var token = TestContext.Current.CancellationToken;
        await using var clients = new Clients(hashTags, 2);
        await using var held = await clients
            .Locks[0]
            .TryAcquireAsync(
                "inventory",
                new LockOptions { Lease = TimeSpan.FromSeconds(2), AutoExtend = true },
                token
            );
        Assert.NotNull(held);
        var originalEnd = held.LeaseEnd;
        await CacheConformance.WaitUntilAsync(() =>
            Task.FromResult(
                held.LeaseEnd > originalEnd + TimeSpan.FromSeconds(2)
                    && DateTimeOffset.UtcNow > originalEnd
            )
        );

        Assert.True(held.IsHeld);
        Assert.False(held.LostToken.IsCancellationRequested);
        Assert.Null(await clients.Locks[1].TryAcquireAsync("inventory", cancellationToken: token));
        await held.DisposeAsync();
        await using var successor = await clients
            .Locks[1]
            .TryAcquireAsync("inventory", cancellationToken: token);
        Assert.NotNull(successor);
    }

    [Fact(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    public async Task CancelledWaiters_DoNotRunActionsOrReleaseTheHolder()
    {
        var token = TestContext.Current.CancellationToken;
        await using var clients = new Clients(false, 2);
        await using var held = await clients
            .Locks[0]
            .TryAcquireAsync("inventory", cancellationToken: token);
        Assert.NotNull(held);
        using var cancellation = CancellationTokenSource.CreateLinkedTokenSource(token);
        var actions = 0;
        var attempts = Enumerable
            .Range(0, 32)
            .Select(_ =>
                clients
                    .Locks[1]
                    .ExecuteWithLockAsync(
                        "inventory",
                        _ => ValueTask.FromResult(Interlocked.Increment(ref actions)),
                        new LockOptions
                        {
                            MaxWait = TimeSpan.FromSeconds(20),
                            Retry = LockRetryPolicy.Linear(100, TimeSpan.FromMilliseconds(10)),
                        },
                        cancellation.Token
                    )
                    .AsTask()
            )
            .ToArray();
        await cancellation.CancelAsync();
        foreach (var attempt in attempts)
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => attempt);

        Assert.Equal(0, actions);
        Assert.True(held.IsHeld);
        Assert.Null(await clients.Locks[1].TryAcquireAsync("inventory", cancellationToken: token));
        await held.DisposeAsync();
        await using var next = await clients
            .Locks[1]
            .TryAcquireAsync("inventory", cancellationToken: token);
        Assert.NotNull(next);
    }

    [Theory(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task CacheAndLock_WithIdenticalConsumerKeysRemainIndependent(bool hashTags)
    {
        var token = TestContext.Current.CancellationToken;
        await using var clients = new Clients(hashTags, 2);
        await using var cache = new TieredCache(
            new CachingOptions { Namespace = clients.Namespace },
            clients.Stores[0],
            Serializer()
        );
        await using var held = await clients
            .Locks[1]
            .TryAcquireAsync("catalog", cancellationToken: token);
        Assert.NotNull(held);
        await cache.SetAsync(
            "catalog",
            42,
            new CacheEntryOptions(TimeSpan.FromSeconds(30)) { Tags = ["catalog"] },
            token
        );
        await cache.RemoveByTagAsync("catalog", token);
        Assert.False((await cache.TryGetAsync<int>("catalog", token)).Found);
        Assert.Null(await clients.Locks[0].TryAcquireAsync("catalog", cancellationToken: token));
        Assert.True(held.IsHeld);
        await cache.SetAsync("catalog", 73, new CacheEntryOptions(TimeSpan.FromSeconds(30)), token);
        await held.DisposeAsync();
        await using var reader = new TieredCache(
            new CachingOptions { Namespace = clients.Namespace },
            clients.Stores[1],
            Serializer()
        );
        var result = await reader.TryGetAsync<int>("catalog", token);
        Assert.Equal(CacheTier.L2, result.Tier);
        Assert.Equal(73, result.Value);
    }

    [Theory(Timeout = 60_000, Skip = RedisAvailability.Skip, SkipUnless = nameof(Available))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task MalformedL2Payload_IsRefilledAndReadableByAnotherConnection(bool empty)
    {
        var token = TestContext.Current.CancellationToken;
        await using var clients = new Clients(false, 2);
        await clients
            .Stores[0]
            .SetAsync(
                clients.Namespace + ":cache:data:catalog",
                empty ? Array.Empty<byte>() : new byte[] { 255, 0, 1, 2 },
                TimeSpan.FromSeconds(30),
                cancellationToken: token
            );
        await using var cache = new TieredCache(
            new CachingOptions { Namespace = clients.Namespace },
            clients.Stores[0],
            Serializer()
        );
        var factories = 0;
        Assert.Equal(
            42,
            await cache.GetOrCreateAsync(
                "catalog",
                _ =>
                {
                    Interlocked.Increment(ref factories);
                    return ValueTask.FromResult(42);
                },
                TimeSpan.FromSeconds(30),
                token
            )
        );
        Assert.Equal(1, factories);
        await using var reader = new TieredCache(
            new CachingOptions { Namespace = clients.Namespace },
            clients.Stores[1],
            Serializer()
        );
        var result = await reader.TryGetAsync<int>("catalog", token);
        Assert.Equal(CacheTier.L2, result.Tier);
        Assert.Equal(42, result.Value);
    }

    private static SystemTextJsonCacheValueSerializer Serializer() =>
        new(new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() });

    private sealed class Clients : IAsyncDisposable
    {
        private readonly bool _hashTags;

        public Clients(bool hashTags, int count)
        {
            _hashTags = hashTags;
            Connections = Enumerable
                .Range(0, count)
                .Select(_ => new RedisConnection(
                    new RedisOptions
                    {
                        Configuration = RedisAvailability.Configuration,
                        UseHashTags = hashTags,
                    }
                ))
                .ToArray();
            Stores = Connections.Select(connection => new RedisCacheStore(connection)).ToArray();
            Providers = Connections
                .Select(connection => new RedisLockProvider(connection))
                .ToArray();
            Locks = Providers
                .Select(provider => new DistributedLock(
                    new LockingOptions { Namespace = Namespace },
                    provider
                ))
                .ToArray();
        }

        public string Namespace { get; } = "deep-" + Guid.NewGuid().ToString("N");
        public RedisConnection[] Connections { get; }
        public RedisCacheStore[] Stores { get; }
        public RedisLockProvider[] Providers { get; }
        public DistributedLock[] Locks { get; }

        public string Key(string suffix) =>
            (_hashTags ? "{" + Namespace + "}" : Namespace) + ":" + suffix;

        public async ValueTask DisposeAsync()
        {
            try
            {
                using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
                var mux = await Connections[0].GetMultiplexerAsync(cleanup.Token);
                var server = mux.GetServer(mux.GetEndPoints()[0]);
                // Only this test's GUID namespace; never flush the shared fixture database.
                await foreach (
                    var key in server.KeysAsync(pattern: Key("*")).WithCancellation(cleanup.Token)
                )
                    await mux.GetDatabase().KeyDeleteAsync(key).WaitAsync(cleanup.Token);
                await mux.GetDatabase()
                    .KeyDeleteAsync(Namespace + ":counter")
                    .WaitAsync(cleanup.Token);
            }
            finally
            {
                foreach (var mutex in Locks)
                    await mutex.DisposeAsync();
                foreach (var store in Stores)
                    await store.DisposeAsync();
                foreach (var provider in Providers)
                    await provider.DisposeAsync();
                foreach (var connection in Connections)
                    await connection.DisposeAsync();
            }
        }
    }
}
