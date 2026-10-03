using HostLoom.Redis;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

public sealed class RedisLockProviderTests
{
    [Fact(Timeout = 10_000)]
    public async Task Acquire_honours_the_callers_token_while_the_reply_is_outstanding()
    {
        var db = Substitute.For<IDatabase>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);
        var reply = new TaskCompletionSource<bool>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        db.StringSetAsync(
                Arg.Any<RedisKey>(),
                Arg.Any<RedisValue>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<When>()
            )
            .Returns(reply.Task);
        db.StringSetAsync(
                Arg.Any<RedisKey>(),
                Arg.Any<RedisValue>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<When>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(reply.Task);
        db.StringSetAsync(
                Arg.Any<RedisKey>(),
                Arg.Any<RedisValue>(),
                Arg.Any<Expiration>(),
                Arg.Any<ValueCondition>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(reply.Task);
        await using var provider = new RedisLockProvider(mux);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );

        var acquire = provider
            .TryAcquireAsync(
                "orders:lock:inventory",
                "owner",
                TimeSpan.FromSeconds(5),
                caller.Token
            )
            .AsTask();
        Assert.False(acquire.IsCompleted);
        await caller.CancelAsync();

        // Standard cancellation: the caller's token ends the wait for a reply that never came,
        // and a cancellation is not reported as a backend failure.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => acquire);
        reply.SetResult(true);
    }

    [Theory]
    [InlineData(1L, 1L)]
    [InlineData(5_000L, 1L)]
    [InlineData(10_000L, 1L)]
    [InlineData(15_000L, 2L)]
    [InlineData(15_000_000L, 1_500L)]
    public async Task Leases_are_rounded_up_to_milliseconds_on_acquire_and_extend(
        long leaseTicks,
        long expectedMilliseconds
    )
    {
        var db = Substitute.For<IDatabase>();
        var mux = Substitute.For<IConnectionMultiplexer>();
        mux.GetDatabase(Arg.Any<int>(), Arg.Any<object>()).Returns(db);
        db.StringSetAsync(
                Arg.Any<RedisKey>(),
                Arg.Any<RedisValue>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<When>()
            )
            .Returns(call =>
            {
                Assert.Equal(
                    TimeSpan.FromMilliseconds(expectedMilliseconds),
                    call.ArgAt<TimeSpan?>(2)
                );
                return Task.FromResult(true);
            });
        db.StringSetAsync(
                Arg.Any<RedisKey>(),
                Arg.Any<RedisValue>(),
                Arg.Any<TimeSpan?>(),
                Arg.Any<When>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(call =>
            {
                Assert.Equal(
                    TimeSpan.FromMilliseconds(expectedMilliseconds),
                    call.ArgAt<TimeSpan?>(2)
                );
                return Task.FromResult(true);
            });
        db.ScriptEvaluateAsync(
                Arg.Any<string>(),
                Arg.Any<RedisKey[]>(),
                Arg.Any<RedisValue[]>(),
                Arg.Any<CommandFlags>()
            )
            .Returns(call =>
            {
                var arguments = call.ArgAt<RedisValue[]>(2);
                Assert.Equal(expectedMilliseconds, (long)arguments[1]);
                return Task.FromResult(RedisResult.Create(1L));
            });
        await using var provider = new RedisLockProvider(mux);
        var lease = TimeSpan.FromTicks(leaseTicks);

        // A positive sub-millisecond lease must never reach the server as PX 0, which would
        // delete the key while the acquire or extension reports success.
        Assert.True(
            await provider.TryAcquireAsync(
                "orders:lock:inventory",
                "owner",
                lease,
                TestContext.Current.CancellationToken
            )
        );
        Assert.True(
            await provider.ExtendAsync(
                "orders:lock:inventory",
                "owner",
                lease,
                TestContext.Current.CancellationToken
            )
        );
    }
}
