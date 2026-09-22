using HostLoom.Caching;
using HostLoom.Locking;
using HostLoom.Redis;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

public sealed class RedisReadOnlyTests
{
    [Fact]
    public async Task ReadOnlyWrite_IsTypedAndTheSameCacheStoreCanWriteAfterRecovery()
    {
        var db = Substitute.For<IDatabase>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);
        var readOnly = true;
        db.StringSetAsync(
                Arg.Any<RedisKey>(),
                Arg.Any<RedisValue>(),
                Arg.Any<Expiration>(),
                Arg.Any<ValueCondition>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(_ => readOnly ? Task.FromException<bool>(ReadOnly()) : Task.FromResult(true));
        await using var connection = new RedisConnection(mux);
        await using var store = new RedisCacheStore(connection);
        var token = TestContext.Current.CancellationToken;
        var failure = await Assert.ThrowsAsync<CacheStoreException>(() =>
            store
                .SetAsync(
                    "catalog:cache:data:books",
                    new byte[] { 1 },
                    TimeSpan.FromSeconds(30),
                    cancellationToken: token
                )
                .AsTask()
        );
        Assert.Equal(CacheFailureKind.Other, failure.Kind);
        Assert.IsType<RedisServerException>(failure.InnerException);
        readOnly = false;
        await store.SetAsync(
            "catalog:cache:data:books",
            new byte[] { 2 },
            TimeSpan.FromSeconds(30),
            cancellationToken: token
        );
        Assert.Same(mux, await connection.GetMultiplexerAsync(token));
    }

    [Fact]
    public async Task ReadOnlyAcquire_DoesNotRunProtectedWorkAndRecoversWithoutReplacingTheConnection()
    {
        var db = Substitute.For<IDatabase>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);
        var readOnly = true;
        db.StringSetAsync(
                Arg.Any<RedisKey>(),
                Arg.Any<RedisValue>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<When>()
            )
            .Returns(_ => readOnly ? Task.FromException<bool>(ReadOnly()) : Task.FromResult(true));
        db.ScriptEvaluateAsync(
                Arg.Any<string>(),
                Arg.Any<RedisKey[]>(),
                Arg.Any<RedisValue[]>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(Task.FromResult(RedisResult.Create(1L)));
        await using var connection = new RedisConnection(mux);
        await using var provider = new RedisLockProvider(connection);
        await using var mutex = new DistributedLock(
            new LockingOptions { Namespace = "catalog" },
            provider
        );
        var calls = 0;
        ValueTask<int> Action(CancellationToken _) => ValueTask.FromResult(++calls);
        var failure = await Assert.ThrowsAsync<LockProviderUnavailableException>(() =>
            mutex
                .ExecuteWithLockAsync(
                    "inventory",
                    Action,
                    cancellationToken: TestContext.Current.CancellationToken
                )
                .AsTask()
        );
        Assert.Equal(LockFailureKind.Other, failure.Kind);
        Assert.Equal(0, calls);
        readOnly = false;
        Assert.Equal(
            1,
            await mutex.ExecuteWithLockAsync(
                "inventory",
                Action,
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        Assert.Same(
            mux,
            await connection.GetMultiplexerAsync(TestContext.Current.CancellationToken)
        );
    }

    private static RedisServerException ReadOnly() =>
        new(
            RedisErrorKind.ReadOnly,
            CommandFlags.None,
            "READONLY You can't write against a read only replica."
        );
}
