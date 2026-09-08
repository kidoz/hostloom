using HostLoom.Redis;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

public sealed class RedisCacheStoreTests
{
    [Theory]
    [InlineData("set")]
    [InlineData("tagged")]
    [InlineData("absent")]
    [InlineData("many")]
    public async Task CancelledWrite_RetainsOwnedBytesUntilTheSdkFinishes(string operation)
    {
        var db = Substitute.For<IDatabase>();
        var batch = Substitute.For<IBatch>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);
        db.CreateBatch(Arg.Any<object>()).Returns(batch);
        var pending = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var queued = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        RedisValue retained = default;
        foreach (var target in new IDatabaseAsync[] { db, batch })
        {
            target
                .StringSetAsync(
                    Arg.Any<RedisKey>(),
                    Arg.Any<RedisValue>(),
                    Arg.Any<Expiration>(),
                    Arg.Any<ValueCondition>(),
                    Arg.Any<CommandFlags>()
                )
                .Returns(call =>
                {
                    retained = call.ArgAt<RedisValue>(1);
                    queued.TrySetResult();
                    return pending.Task;
                });
            target
                .StringSetAsync(
                    Arg.Any<RedisKey>(),
                    Arg.Any<RedisValue>(),
                    Arg.Any<TimeSpan?>(),
                    Arg.Any<When>()
                )
                .Returns(call =>
                {
                    retained = call.ArgAt<RedisValue>(1);
                    queued.TrySetResult();
                    return pending.Task;
                });
        }

        await using var store = new RedisCacheStore(mux);
        using var cancellation = new CancellationTokenSource();
        var payload = Enumerable.Repeat((byte)1, 128).ToArray();
        var ttl = TimeSpan.FromMinutes(1);
        const string key = "svc:cache:data:catalog";
        var write = operation switch
        {
            "absent" => store
                .SetIfAbsentAsync(key, payload, ttl, cancellationToken: cancellation.Token)
                .AsTask(),
            "many" => store.SetManyAsync([new(key, payload)], ttl, cancellation.Token).AsTask(),
            _ => store
                .SetAsync(
                    key,
                    payload,
                    ttl,
                    operation == "tagged" ? ["svc:cache:tag:catalog"] : null,
                    cancellation.Token
                )
                .AsTask(),
        };

        try
        {
            await queued.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            );
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() => write);
            Assert.False(pending.Task.IsCompleted);
            Array.Fill(payload, (byte)9);

            Assert.Equal(
                Enumerable.Repeat((byte)1, 128),
                ((ReadOnlyMemory<byte>)retained).ToArray()
            );
        }
        finally
        {
            pending.TrySetResult(true);
        }
    }

    [Fact]
    public async Task BulkRead_DoesNotResurrectAValueThatExpiredBeforeItsTtlWasRead()
    {
        var db = Substitute.For<IDatabase>();
        var batch = Substitute.For<IBatch>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);
        db.CreateBatch(Arg.Any<object>()).Returns(batch);
        // The old two-command path observes bytes before expiry and no TTL afterwards.
        db.StringGetAsync(Arg.Any<RedisKey[]>(), Arg.Any<CommandFlags>())
            .Returns(Task.FromResult(new RedisValue[] { new byte[] { 7 } }));
        batch
            .KeyTimeToLiveAsync(Arg.Any<RedisKey>(), Arg.Any<CommandFlags>())
            .Returns(Task.FromResult<TimeSpan?>(null));
        // An atomic read observes either the old value with its TTL, or this miss.
        batch
            .ScriptEvaluateAsync(
                Arg.Any<string>(),
                Arg.Any<RedisKey[]>(),
                Arg.Any<RedisValue[]>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(Task.FromResult(RedisResult.Create(RedisValue.Null)));
        await using var store = new RedisCacheStore(mux);

        var found = await store.GetManyAsync(
            ["svc:cache:data:catalog"],
            TestContext.Current.CancellationToken
        );

        Assert.Empty(found);
    }
}
