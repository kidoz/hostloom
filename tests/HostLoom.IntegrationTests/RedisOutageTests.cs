using System.Diagnostics.Metrics;
using System.Text.Json;
using System.Text.Json.Serialization.Metadata;
using HostLoom.Caching;
using HostLoom.Conformance;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Locking;
using HostLoom.Redis;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Opt-in local network experiments: HOSTLOOM_REDIS_CHAOS=1, a real loopback Redis, and
/// a private proxy per test. A 60-second deadline aborts the experiment and disposes the proxy.
/// </summary>
[Collection(nameof(RedisOutageTests))]
[CollectionDefinition(nameof(RedisOutageTests), DisableParallelization = true)]
public sealed class RedisOutageTests
{
    private const string Skip =
        "Set HOSTLOOM_REDIS_CHAOS=1 and start the local Redis fixture to run isolated network faults.";
    public static bool Enabled =>
        Environment.GetEnvironmentVariable("HOSTLOOM_REDIS_CHAOS") == "1"
        && RedisAvailability.Redis;

    [Fact(Timeout = 60_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task DisconnectedCache_FailsOpenAndRecoversOnTheSameConnection()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "outage-" + Guid.NewGuid().ToString("N");
        await using var proxy = new RedisFaultProxy();
        await using var connection = Connection(proxy);
        await using var store = new RedisCacheStore(connection);
        await using var cache = new TieredCache(
            new CachingOptions { Namespace = ns },
            store,
            Serializer()
        );
        var entry = new CacheEntryOptions(TimeSpan.FromSeconds(30));
        await cache.SetAsync("catalog", 7, entry, token);
        Assert.True((await store.CheckHealthAsync(token)).IsHealthy);
        Assert.NotNull(await store.GetAsync(ns + ":cache:data:catalog", token));
        // Written just before the outage with a short life and a long grace: its in-process copy
        // expires during the outage, where the grace is meant to serve it.
        var graced = new CacheEntryOptions(TimeSpan.FromSeconds(1))
        {
            StaleGrace = TimeSpan.FromMinutes(1),
        };
        await cache.SetAsync("rates", 3, graced, token);

        proxy.SetEnabled(false);
        try
        {
            await Task.Delay(TimeSpan.FromMilliseconds(1300), token);
            var entered = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var release = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var refresher = cache
                .GetOrCreateAsync(
                    "rates",
                    async _ =>
                    {
                        entered.SetResult();
                        await release.Task;
                        return 4;
                    },
                    graced,
                    token
                )
                .AsTask();
            await entered.Task.WaitAsync(TimeSpan.FromSeconds(30), token);
            // The refresher holds the guard; with Redis unreachable the expired copy inside its
            // grace is what a second caller receives, at once.
            var stale = await cache
                .GetOrCreateAsync("rates", _ => ValueTask.FromResult(99), graced, token)
                .AsTask()
                .WaitAsync(TimeSpan.FromSeconds(30), token);
            Assert.Equal(3, stale);
            release.SetResult();
            Assert.Equal(4, await refresher);
            Assert.False((await store.CheckHealthAsync(token)).IsHealthy);
            var failure = await Assert.ThrowsAsync<CacheStoreException>(() =>
                store.GetAsync(ns + ":cache:data:catalog", token).AsTask()
            );
            Assert.Contains(
                failure.Kind,
                new[] { CacheFailureKind.Unavailable, CacheFailureKind.Timeout }
            );
            var local = await cache.TryGetAsync<int>("catalog", token);
            Assert.Equal(CacheTier.L1, local.Tier);
            Assert.Equal(7, local.Value);
            Assert.Equal(
                9,
                await cache.GetOrCreateAsync(
                    "inventory",
                    _ => ValueTask.FromResult(9),
                    entry.Expiration,
                    token
                )
            );
            Assert.Equal(CacheTier.L1, (await cache.TryGetAsync<int>("inventory", token)).Tier);
            Assert.False(await cache.SetIfAbsentAsync("deny", 1, entry, token));
            await Assert.ThrowsAsync<CacheUnavailableException>(() =>
                cache
                    .SetIfAbsentAsync(
                        "throw",
                        1,
                        new CacheEntryOptions(entry.Expiration)
                        {
                            OnUnavailable = UnavailableBehavior.Throw,
                        },
                        token
                    )
                    .AsTask()
            );
        }
        finally
        {
            proxy.SetEnabled(true);
        }

        await CacheConformance.WaitUntilAsync(
            async () => (await store.CheckHealthAsync(token)).IsHealthy,
            20
        );
        await cache.SetAsync("recovered", 11, entry, token);
        await using var direct = new RedisConnection(
            new RedisOptions { Configuration = RedisAvailability.Configuration }
        );
        await using var readerStore = new RedisCacheStore(direct);
        await using var reader = new TieredCache(
            new CachingOptions { Namespace = ns },
            readerStore,
            Serializer()
        );
        var recovered = await reader.TryGetAsync<int>("recovered", token);
        Assert.Equal(CacheTier.L2, recovered.Tier);
        Assert.Equal(11, recovered.Value);
        await cache.RemoveAsync(
            ["catalog", "inventory", "deny", "throw", "recovered", "rates"],
            token
        );
    }

    [Fact(Timeout = 60_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task DisconnectedLock_ReportsUnavailableLosesLeaseAndRecoversSafely()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "outage-" + Guid.NewGuid().ToString("N");
        await using var proxy = new RedisFaultProxy();
        await using var connection = Connection(proxy);
        await using var provider = new RedisLockProvider(connection);
        await using var mutex = new DistributedLock(
            new LockingOptions { Namespace = ns },
            provider
        );
        await using var held = await mutex.TryAcquireAsync(
            "inventory",
            new LockOptions { Lease = TimeSpan.FromSeconds(15) },
            token
        );
        Assert.NotNull(held);
        var leaseEnd = held.LeaseEnd;
        Assert.True((await provider.CheckHealthAsync(token)).IsHealthy);
        var actions = 0;

        proxy.SetEnabled(false);
        try
        {
            Assert.False((await provider.CheckHealthAsync(token)).IsHealthy);
            var failure = await Assert.ThrowsAsync<LockProviderUnavailableException>(() =>
                mutex
                    .ExecuteWithLockAsync(
                        "catalog",
                        _ => ValueTask.FromResult(Interlocked.Increment(ref actions)),
                        cancellationToken: token
                    )
                    .AsTask()
            );
            Assert.Contains(
                failure.Kind,
                new[] { LockFailureKind.Unavailable, LockFailureKind.Timeout }
            );
            Assert.Equal(0, actions);
            Assert.True(held.IsHeld);
            Assert.False(await held.ExtendAsync(TimeSpan.FromSeconds(15), token));
            // A transport failure preserves the previous lease deadline. Only expiry or
            // a confirmed owner mismatch proves it lost; a failed renewal never moves it.
            Assert.Equal(leaseEnd, held.LeaseEnd);
            await CacheConformance.WaitUntilAsync(
                () => Task.FromResult(held.LostToken.IsCancellationRequested),
                20
            );
            Assert.False(held.IsHeld);
            Assert.True(held.LostToken.IsCancellationRequested);
        }
        finally
        {
            proxy.SetEnabled(true);
        }

        await CacheConformance.WaitUntilAsync(
            async () => (await provider.CheckHealthAsync(token)).IsHealthy,
            20
        );
        await using var direct = new RedisConnection(
            new RedisOptions { Configuration = RedisAvailability.Configuration }
        );
        await using var successorProvider = new RedisLockProvider(direct);
        await using var successorLock = new DistributedLock(
            new LockingOptions { Namespace = ns },
            successorProvider
        );
        await using var successor = await successorLock.TryAcquireAsync(
            "inventory",
            new LockOptions
            {
                MaxWait = TimeSpan.FromSeconds(20),
                Retry = LockRetryPolicy.Linear(100, TimeSpan.FromMilliseconds(10)),
            },
            token
        );
        Assert.NotNull(successor);
        await held.DisposeAsync();
        Assert.Null(await mutex.TryAcquireAsync("inventory", cancellationToken: token));
        await successor.DisposeAsync();
        Assert.Equal(
            1,
            await mutex.ExecuteWithLockAsync(
                "inventory",
                _ => ValueTask.FromResult(1),
                cancellationToken: token
            )
        );
    }

    [Fact(Timeout = 60_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task AutoExtend_ResumesAfterAHeartbeatFailedDuringAnOutage()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "outage-" + Guid.NewGuid().ToString("N");
        await using var proxy = new RedisFaultProxy();
        await using var connection = Connection(proxy);
        await using var provider = new RedisLockProvider(connection);
        await using var mutex = new DistributedLock(
            new LockingOptions { Namespace = ns },
            provider
        );
        var lease = TimeSpan.FromSeconds(12);
        await using var held = await mutex.TryAcquireAsync(
            "inventory",
            new LockOptions { Lease = lease, AutoExtend = true },
            token
        );
        Assert.NotNull(held);
        var leaseEnd = held.LeaseEnd;

        // The first heartbeat is due halfway through the lease; cut the connection across it.
        await Task.Delay(TimeSpan.FromSeconds(5), token);
        proxy.SetEnabled(false);
        await Task.Delay(TimeSpan.FromSeconds(2), token);
        Assert.Equal(leaseEnd, held.LeaseEnd);
        Assert.True(held.IsHeld);
        proxy.SetEnabled(true);

        // One failed heartbeat must not end automatic extension: a retry lands once the
        // connection is back, before the original lease end.
        await CacheConformance.WaitUntilAsync(() => Task.FromResult(held.LeaseEnd > leaseEnd), 10);
        Assert.True(held.IsHeld);
        Assert.False(held.LostToken.IsCancellationRequested);
        var pastTheOriginalEnd = leaseEnd + TimeSpan.FromSeconds(1) - DateTimeOffset.UtcNow;
        if (pastTheOriginalEnd > TimeSpan.Zero)
        {
            await Task.Delay(pastTheOriginalEnd, token);
        }

        Assert.True(held.IsHeld);
    }

    [Fact(Timeout = 60_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task CancelledAcquire_ReleasesTheGrantThatLandsAfterTheCallerGaveUp()
    {
        var token = TestContext.Current.CancellationToken;
        var ns = "outage-" + Guid.NewGuid().ToString("N");
        await using var proxy = new RedisFaultProxy();
        await using var connection = Connection(proxy);
        await using var provider = new RedisLockProvider(connection);
        await using var mutex = new DistributedLock(
            new LockingOptions { Namespace = ns },
            provider
        );
        var released = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        using var listener = OrphanReleaseListener(released);

        // Connect first: a held handshake reply would fail the connection, not the acquisition.
        Assert.True((await provider.CheckHealthAsync(token)).IsHealthy);

        // The SET reaches the server at once; only its reply is held until the caller gave up.
        proxy.HoldReplies();
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(token);
        var acquire = mutex
            .TryAcquireAsync(
                "inventory",
                new LockOptions { Lease = TimeSpan.FromSeconds(20) },
                caller.Token
            )
            .AsTask();
        await Task.Delay(TimeSpan.FromMilliseconds(200), token);
        Assert.False(acquire.IsCompleted);
        await caller.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => acquire);
        proxy.ReleaseReplies();

        // The late grant is released by the abandoned owner, long before its 20-second lease
        // would expire, so a successor on another connection takes the key.
        await released.Task.WaitAsync(TimeSpan.FromSeconds(10), token);
        await using var direct = new RedisConnection(
            new RedisOptions { Configuration = RedisAvailability.Configuration }
        );
        await using var successorProvider = new RedisLockProvider(direct);
        await using var successorLock = new DistributedLock(
            new LockingOptions { Namespace = ns },
            successorProvider
        );
        await using var successor = await successorLock.TryAcquireAsync(
            "inventory",
            new LockOptions { Lease = TimeSpan.FromSeconds(5) },
            token
        );
        Assert.NotNull(successor);
    }

    private static MeterListener OrphanReleaseListener(TaskCompletionSource released)
    {
        var listener = new MeterListener
        {
            InstrumentPublished = (instrument, l) =>
            {
                if (
                    instrument.Meter.Name == LockingDiagnostics.MeterName
                    && instrument.Name == "hostloom.lock.orphan_releases"
                )
                    l.EnableMeasurementEvents(instrument);
            },
        };
        listener.SetMeasurementEventCallback<long>(
            (_, _, tags, _) =>
            {
                foreach (var tag in tags)
                    if (
                        tag.Key == LockingDiagnostics.OutcomeTag
                        && string.Equals(tag.Value as string, "released", StringComparison.Ordinal)
                    )
                        released.TrySetResult();
            }
        );
        listener.Start();
        return listener;
    }

    private static RedisConnection Connection(RedisFaultProxy proxy) =>
        new(
            new RedisOptions
            {
                Configuration = proxy.Configuration,
                ConnectTimeout = TimeSpan.FromSeconds(1),
                CommandTimeout = TimeSpan.FromSeconds(1),
                HealthTimeout = TimeSpan.FromMilliseconds(300),
            }
        );

    private static SystemTextJsonCacheValueSerializer Serializer() =>
        new(new JsonSerializerOptions { TypeInfoResolver = new DefaultJsonTypeInfoResolver() });
}
