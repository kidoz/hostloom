using System.Net;
using HostLoom.Redis;
using HostLoom.Transport.InMemory;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

public sealed class TransportRecoveryRegressionTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Repeated_disposal_cannot_remove_a_successor(bool events)
    {
        await using var broker = new InMemoryRequestBroker();
        var calls = 0;
        var first = await Subscribe();
        await first.DisposeAsync();
        await using var successor = await Subscribe();
        await first.DisposeAsync();
        if (events)
            await broker.PublishAsync(
                "catalog",
                new byte[] { 1 },
                TestContext.Current.CancellationToken
            );
        else
            await broker.RequestAsync(
                "catalog",
                new byte[] { 1 },
                Guid.NewGuid(),
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken
            );
        Assert.Equal(1, calls);

        ValueTask<IAsyncDisposable> Subscribe() =>
            events
                ? broker.SubscribeAsync(
                    "catalog",
                    "audit",
                    (_, _) =>
                    {
                        calls++;
                        return ValueTask.CompletedTask;
                    },
                    TestContext.Current.CancellationToken
                )
                : broker.ListenAsync(
                    "catalog",
                    (_, _) =>
                    {
                        calls++;
                        return ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 2 });
                    },
                    TestContext.Current.CancellationToken
                );
    }

    [Fact]
    public async Task Caller_cancellation_does_not_cancel_accepted_handler_work()
    {
        await using var broker = new InMemoryRequestBroker();
        var entered = new TaskCompletionSource<CancellationToken>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        await using var listener = await broker.ListenAsync(
            "catalog",
            async (_, token) =>
            {
                entered.SetResult(token);
                await release.Task.WaitAsync(token);
                completed.SetResult();
                return new byte[] { 2 };
            },
            TestContext.Current.CancellationToken
        );
        using var caller = new CancellationTokenSource();
        var pending = broker
            .RequestAsync(
                "catalog",
                new byte[] { 1 },
                Guid.NewGuid(),
                TimeSpan.FromSeconds(2),
                caller.Token
            )
            .AsTask();
        var receiverToken = await entered.Task.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );
        await caller.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.False(receiverToken.IsCancellationRequested);
        release.SetResult();
        await completed.Task.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );
    }

    [Fact]
    public async Task Failed_subscriber_does_not_make_outbox_republish_to_healthy_subscribers()
    {
        await using var broker = new InMemoryRequestBroker();
        var healthy = 0;
        await using var broken = await broker.SubscribeAsync(
            "catalog",
            "broken",
            (_, _) => throw new InvalidOperationException("failure"),
            TestContext.Current.CancellationToken
        );
        await using var listener = await broker.SubscribeAsync(
            "catalog",
            "audit",
            (_, _) =>
            {
                healthy++;
                return ValueTask.CompletedTask;
            },
            TestContext.Current.CancellationToken
        );
        var store = new InMemoryOutboxStore();
        await store.AppendAsync(
            new OutboxMessage
            {
                MessageId = Guid.NewGuid(),
                Topic = "catalog",
                MessageType = "catalog",
                Frame = new byte[] { 1 },
                EnqueuedAt = DateTimeOffset.UtcNow,
            },
            TestContext.Current.CancellationToken
        );
        await using var relay = new OutboxRelay(store, broker, new OutboxOptions());
        Assert.Equal(1, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, healthy);
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(true, 1)]
    public async Task Cluster_rejects_incompatible_configuration_before_commands(
        bool hashTags,
        int database
    )
    {
        var mux = Substitute.For<IConnectionMultiplexer>();
        var endpoint = new IPEndPoint(IPAddress.Loopback, 6379);
        var server = Substitute.For<IServer>();
        server.ServerType.Returns(ServerType.Cluster);
        mux.GetEndPoints(Arg.Any<bool>()).Returns([endpoint]);
        mux.GetServer(endpoint, Arg.Any<object>()).Returns(server);
        await using var connection = new RedisConnection(
            mux,
            new RedisOptions { UseHashTags = hashTags, DatabaseIndex = database }
        );
        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            connection.GetDatabaseAsync(TestContext.Current.CancellationToken).AsTask()
        );
        Assert.Contains("UseHashTags", exception.Message, StringComparison.Ordinal);
        mux.DidNotReceive().GetDatabase(Arg.Any<int>(), Arg.Any<object>());
    }
}
