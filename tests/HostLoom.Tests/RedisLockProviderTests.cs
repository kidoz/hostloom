using HostLoom.Redis;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

public sealed class RedisLockProviderTests
{
    [Fact(Timeout = 10_000)]
    public async Task Acquire_lets_a_sent_command_finish_after_the_caller_cancels()
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
        await caller.CancelAsync();

        // The command was sent, so the reply is awaited regardless of the caller's token.
        Assert.False(acquire.IsCompleted);
        reply.SetResult(true);
        Assert.True(await acquire);
    }
}
