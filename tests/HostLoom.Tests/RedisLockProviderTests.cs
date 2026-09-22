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
}
