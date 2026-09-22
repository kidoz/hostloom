using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// What a RabbitMQ caller receives when something goes wrong: a client-library or network
/// failure as <see cref="MessagingTransportException"/> with the library's exception inside,
/// an unroutable request as the request timeout, and cancellation with the caller's own token.
/// </summary>
public sealed partial class RabbitMqBrokerTests
{
    [Theory(Timeout = 30_000)]
    [InlineData("publish", "nack")]
    [InlineData("publish", "closed")]
    [InlineData("publish", "unreachable")]
    [InlineData("request", "nack")]
    [InlineData("request", "closed")]
    [InlineData("request", "unreachable")]
    public async Task A_transport_failure_reaches_the_caller_as_a_messaging_transport_exception(
        string operation,
        string failure
    )
    {
        var token = TestContext.Current.CancellationToken;
        var rabbit = new FakeRabbit();
        Exception cause = failure switch
        {
            "nack" => new PublishException(1, isReturn: false),
            "closed" => new AlreadyClosedException(
                new ShutdownEventArgs(ShutdownInitiator.Peer, 320, "connection dropped")
            ),
            _ => new BrokerUnreachableException(new IOException("connection refused")),
        };
        await using var broker = new RabbitMqRequestBroker(
            Options.Create(new RabbitMqOptions { ClientProvidedName = UniqueClient() }),
            _ =>
                failure == "unreachable"
                    ? ValueTask.FromException<IConnection>(cause)
                    : ValueTask.FromResult(rabbit.Connection)
        );
        if (failure != "unreachable")
        {
            // The first publication creates the pooled channel the failing one then reuses.
            await broker.PublishAsync("catalog", new byte[] { 1 }, token);
            rabbit
                .Channels[0]
                .Channel.BasicPublishAsync(
                    Arg.Any<string>(),
                    Arg.Any<string>(),
                    Arg.Any<bool>(),
                    Arg.Any<BasicProperties>(),
                    Arg.Any<ReadOnlyMemory<byte>>(),
                    Arg.Any<CancellationToken>()
                )
                .Returns(_ => ValueTask.FromException(cause));
        }

        var thrown = await Assert.ThrowsAsync<MessagingTransportException>(() =>
            (
                operation == "publish"
                    ? broker.PublishAsync("orders", new byte[] { 2 }, token).AsTask()
                    : broker
                        .RequestAsync(
                            "orders",
                            new byte[] { 2 },
                            Guid.NewGuid(),
                            TimeSpan.FromSeconds(30),
                            token
                        )
                        .AsTask()
            ).WaitAsync(Bound, token)
        );

        Assert.Same(cause, thrown.InnerException);
        Assert.Equal("orders", thrown.Address.Value);
        Assert.Equal(0, broker.PendingRequestCount);
    }

    [Theory(Timeout = 30_000)]
    [InlineData("publish")]
    [InlineData("request")]
    public async Task Caller_cancellation_throws_with_the_callers_own_token(string operation)
    {
        var token = TestContext.Current.CancellationToken;
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);
        await broker.PublishAsync("catalog", new byte[] { 1 }, token);
        var entered = rabbit.Channels[0].StallPublishes();
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(token);

        var pending =
            operation == "publish"
                ? broker.PublishAsync("orders", new byte[] { 2 }, caller.Token).AsTask()
                : broker
                    .RequestAsync(
                        "orders",
                        new byte[] { 2 },
                        Guid.NewGuid(),
                        TimeSpan.FromMinutes(5),
                        caller.Token
                    )
                    .AsTask();
        await entered.WaitAsync(Bound, token);
        await caller.CancelAsync();

        var cancelled = await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            pending.WaitAsync(Bound, token)
        );
        Assert.Equal(caller.Token, cancelled.CancellationToken);
    }

    [Fact(Timeout = 30_000)]
    public async Task An_unroutable_request_times_out_like_an_unbound_one()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit, clock, maxConcurrentPublishes: 2, UniqueClient());
        await broker.PublishAsync("catalog", new byte[] { 1 }, token);
        rabbit
            .Channels[0]
            .Channel.BasicPublishAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<BasicProperties>(),
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(_ => ValueTask.FromException(new PublishException(2, isReturn: true)));
        var timeout = TimeSpan.FromSeconds(30);

        var pending = broker
            .RequestAsync("orders", new byte[] { 2 }, Guid.NewGuid(), timeout, token)
            .AsTask();
        await WaitForChannelAsync(rabbit, channel => channel.Publishes.Count == 2);

        // The broker returned it, and the caller still waits for its own deadline.
        Assert.False(pending.IsCompleted);
        clock.Advance(timeout);
        var timedOut = await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            pending.WaitAsync(Bound, token)
        );
        Assert.Equal("orders", timedOut.Address.Value);
    }
}
