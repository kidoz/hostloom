using HostLoom.Redis;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

public sealed class RedisConnectionTests
{
    [Fact]
    public async Task CancellingOneWaiter_DoesNotCancelOrDuplicateTheSharedOwnedConnection()
    {
        var completion = new TaskCompletionSource<IConnectionMultiplexer>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var mux = Substitute.For<IConnectionMultiplexer>();
        var attempts = 0;
        await using var connection = new RedisConnection(
            new RedisOptions { Configuration = "localhost:6379" },
            _ =>
            {
                attempts++;
                return completion.Task;
            }
        );
        using var cancellation = new CancellationTokenSource();
        var first = connection.GetMultiplexerAsync(cancellation.Token).AsTask();
        var second = connection.GetMultiplexerAsync(TestContext.Current.CancellationToken).AsTask();
        try
        {
            await cancellation.CancelAsync();
            await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
                first.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
            );
        }
        finally
        {
            completion.TrySetResult(mux);
        }

        Assert.Same(
            mux,
            await second.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
        );
        Assert.Equal(1, attempts);
        await connection.DisposeAsync();
        await mux.Received(1).DisposeAsync();
    }

    [Theory]
    [InlineData(true)]
    [InlineData(false)]
    public async Task Disposal_WaitsForLateConnectionAndPreservesItsOwnership(bool owned)
    {
        var completion = new TaskCompletionSource<IConnectionMultiplexer>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var mux = Substitute.For<IConnectionMultiplexer>();
        var options = new RedisOptions { Configuration = "localhost:6379" };
        if (!owned)
        {
            options.ConnectionFactory = _ => completion.Task;
        }
        await using var connection = new RedisConnection(options, _ => completion.Task);
        var acquiring = connection
            .GetMultiplexerAsync(TestContext.Current.CancellationToken)
            .AsTask();
        var disposing = connection.DisposeAsync().AsTask();
        var alsoDisposing = connection.DisposeAsync().AsTask();
        var pendingAtDisposal = !disposing.IsCompleted;
        completion.SetResult(mux);

        await Task.WhenAll(disposing, alsoDisposing)
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);
        await Assert.ThrowsAsync<ObjectDisposedException>(() => acquiring);
        Assert.True(pendingAtDisposal);
        Assert.False(connection.IsConnected);
        Assert.DoesNotContain(
            mux.ReceivedCalls(),
            call => call.GetMethodInfo().Name.StartsWith("add_", StringComparison.Ordinal)
        );
        await mux.Received(owned ? 1 : 0).DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            connection.GetMultiplexerAsync(TestContext.Current.CancellationToken).AsTask()
        );
    }

    [Fact]
    public async Task FailedInitialization_CanBeRetried()
    {
        var mux = Substitute.For<IConnectionMultiplexer>();
        var attempts = 0;
        // The factory returns a borrowed test double, not a scoped disposable resource.
#pragma warning disable CA2025
        await using var connection = new RedisConnection(
            new RedisOptions
            {
                ConnectionFactory = _ =>
                    ++attempts == 1
                        ? Task.FromException<IConnectionMultiplexer>(
                            new InvalidOperationException("unavailable")
                        )
                        : Task.FromResult(mux),
            }
        );
#pragma warning restore CA2025

        await Assert.ThrowsAsync<InvalidOperationException>(() =>
            connection.GetMultiplexerAsync(TestContext.Current.CancellationToken).AsTask()
        );
        Assert.Same(
            mux,
            await connection.GetMultiplexerAsync(TestContext.Current.CancellationToken)
        );
        Assert.Equal(2, attempts);
        await connection.DisposeAsync();
        await mux.DidNotReceive().DisposeAsync();
    }

    [Fact]
    public async Task Disposal_CancelsAnExternalFactoryThroughTheConnectionLifetime()
    {
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        await using var connection = new RedisConnection(
            new RedisOptions
            {
                ConnectionFactory = async token =>
                {
                    started.SetResult();
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                    throw new InvalidOperationException("unreachable");
                },
            }
        );
        var acquiring = connection
            .GetMultiplexerAsync(TestContext.Current.CancellationToken)
            .AsTask();
        await started.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken
        );

        await connection
            .DisposeAsync()
            .AsTask()
            .WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => acquiring);
    }
}
