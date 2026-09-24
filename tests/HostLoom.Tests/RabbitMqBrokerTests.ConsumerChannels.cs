using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Listener, subscription, and reply consumers. Stopping one waits for its handlers but not for a
/// channel close the broker may never answer, and a consumer the broker cancels, as it does when
/// its queue is deleted, is logged, counted, and restored on a new channel. Waits run on a
/// <see cref="TestClock"/>, so nothing waits them out.
/// </summary>
public sealed partial class RabbitMqBrokerTests
{
    private const string Consumers = "hostloom.rabbitmq.consumers";
    private const string EventTag = "hostloom.rabbitmq.event";
    private const string RoleTag = "hostloom.rabbitmq.role";

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Stopping_a_consumer_returns_while_its_channel_close_hangs_and_disposal_bounds_the_wait(
        bool eventSubscription
    )
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        var client = UniqueClient();
        using var metrics = new MetricRecorder(client);
        var broker = CreateLogged(rabbit, logger, clock, client);
        var consumer = await StartConsumerAsync(
            broker,
            eventSubscription,
            (_, _) => ValueTask.CompletedTask
        );
        var channel = rabbit.Channels[0];
        var (closeStarted, finishClose) = channel.HoldClose();

        // The close waits for a close-ok that never comes, as against a broker that stopped
        // answering; stopping the consumer returns without it.
        await consumer.DisposeAsync().AsTask().WaitAsync(Bound, token);
        await closeStarted.WaitAsync(Bound, token);
        Assert.Equal(1, broker.ClosingChannelCount);
        metrics.Observe();
        Assert.Equal(1, metrics.Last("hostloom.rabbitmq.channels.closing"));

        // Disposing the broker waits for that close only for its bound, then disposes the
        // connection, which closes the rest.
        var disposing = broker.DisposeAsync().AsTask();
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.False(disposing.IsCompleted);
        clock.Advance(RabbitMqRequestBroker.ChannelCloseBound);
        await disposing.WaitAsync(Bound, token);
        await rabbit.Connection.Received(1).DisposeAsync();
        var warning = Assert.Single(logger.Entries, entry => entry.Event.Id == 1403);
        Assert.Equal(LogLevel.Warning, warning.Level);
        finishClose.SetResult();
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Stopping_a_consumer_waits_for_its_handler_in_flight_and_requeues_later_deliveries(
        bool eventSubscription
    )
    {
        var token = TestContext.Current.CancellationToken;
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);
        var entered = 0;
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var consumer = await StartConsumerAsync(
            broker,
            eventSubscription,
            async (_, handlerToken) =>
            {
                Interlocked.Increment(ref entered);
                using var signal = handlerToken.Register(() => cancelled.TrySetResult());
                started.TrySetResult();
                // Ignores its token until released, as a handler finishing its cleanup might.
                await release.Task.WaitAsync(Bound, token);
                handlerToken.ThrowIfCancellationRequested();
            }
        );
        var channel = rabbit.Channels[0];
        var (closeStarted, finishClose) = channel.HoldClose();
        var first = channel.DeliverAsync("c1", "amq.gen-reply", [1], deliveryTag: 1);
        await started.Task.WaitAsync(Bound, token);

        var disposing = consumer.DisposeAsync().AsTask();
        // The close starts at once, so the broker stops delivering, and disposal waits for the
        // handler rather than for the close.
        await closeStarted.WaitAsync(Bound, token);
        Assert.False(disposing.IsCompleted);

        // A delivery already on its way goes back to the queue without reaching the handler.
        await channel
            .DeliverAsync("c2", "amq.gen-reply", [2], deliveryTag: 2)
            .WaitAsync(Bound, token);
        Assert.Equal(1, Volatile.Read(ref entered));
        Assert.Equal([2ul], channel.Nacks);

        await cancelled.Task.WaitAsync(Bound, token);
        release.SetResult();
        await disposing.WaitAsync(Bound, token);
        await first.WaitAsync(Bound, token);
        // Once released, the handler saw its cancellation, so its delivery was requeued too.
        Assert.Equal([2ul, 1ul], channel.Nacks);
        Assert.Empty(channel.Rejects);
        Assert.Empty(channel.Acks);
        finishClose.SetResult();
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_consumer_the_broker_cancels_is_reported_and_restored_on_a_new_channel(
        bool eventSubscription
    )
    {
        var token = TestContext.Current.CancellationToken;
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        var client = UniqueClient();
        using var metrics = new MetricRecorder(client);
        await using var broker = CreateLogged(rabbit, logger, new TestClock(), client);
        var handled = 0;
        await using var consumer = await StartConsumerAsync(
            broker,
            eventSubscription,
            (_, _) =>
            {
                Interlocked.Increment(ref handled);
                return ValueTask.CompletedTask;
            }
        );
        var cancelled = rabbit.Channels[0];
        var role = eventSubscription ? "event" : "request";
        var queue = eventSubscription
            ? RabbitMqQueueNames.Subscription("catalog", "audit")
            : RabbitMqQueueNames.Request("catalog");

        // What the broker sends when the queue is deleted.
        await cancelled.CancelByBrokerAsync();

        var warning = Assert.Single(logger.Entries, entry => entry.Event.Id == 1411);
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Contains(queue, warning.Message, StringComparison.Ordinal);
        await SchedulingTests.WaitUntilAsync(() => logger.Has(new EventId(1412)));
        var restored = Assert.Single(
            rabbit.Channels,
            channel => channel != cancelled && channel.Consumer is not null
        );
        await restored
            .Channel.Received(1)
            .QueueDeclareAsync(
                queue,
                Arg.Any<bool>(),
                Arg.Is(false),
                Arg.Is(false),
                Arg.Any<IDictionary<string, object?>?>(),
                Arg.Any<bool>(),
                Arg.Any<bool>(),
                Arg.Any<CancellationToken>()
            );
        if (eventSubscription)
        {
            // Declared again although this broker already had: whatever deleted the queue may
            // have deleted the exchange with it.
            Assert.Equal("catalog", Assert.Single(restored.Exchanges).Name);
            Assert.Equal(
                new Binding(queue, "catalog", string.Empty),
                Assert.Single(restored.Bindings)
            );
        }

        // The cancelled channel is closed, not reused.
        await SchedulingTests.WaitUntilAsync(() => broker.ClosingChannelCount == 0);
        await cancelled.Channel.Received(1).DisposeAsync();

        // Deliveries reach the handler again, through the new consumer.
        await restored.DeliverAsync("c1", "amq.gen-reply", [7], deliveryTag: 3);
        Assert.Equal(1, Volatile.Read(ref handled));
        Assert.Equal([3ul], restored.Acks);
        Assert.Equal(
            LogLevel.Information,
            Assert.Single(logger.Entries, entry => entry.Event.Id == 1412).Level
        );
        Assert.Equal(1, metrics.Sum(Consumers, (EventTag, "cancelled"), (RoleTag, role)));
        Assert.Equal(1, metrics.Sum(Consumers, (EventTag, "restored"), (RoleTag, role)));
    }

    [Fact(Timeout = 30_000)]
    public async Task A_failed_restoration_is_retried_after_a_doubling_wait_until_it_succeeds()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        await using var broker = CreateLogged(rabbit, logger, clock, UniqueClient());
        await using var listener = await broker.ListenAsync(
            "catalog",
            (_, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 1 }),
            token
        );
        var cancelled = rabbit.Channels[0];
        // The connection is down as well, so restoring fails until it is back.
        rabbit.Drop(ShutdownInitiator.Peer);

        await cancelled.CancelByBrokerAsync();

        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        var failed = Assert.Single(logger.Entries, entry => entry.Event.Id == 1413);
        Assert.Equal(LogLevel.Warning, failed.Level);
        Assert.IsType<AlreadyClosedException>(failed.Exception);
        clock.Advance(RabbitMqRequestBroker.RestoreBackoff);

        // The second failure waits twice as long.
        await SchedulingTests.WaitUntilAsync(() =>
            logger.Entries.Count(entry => entry.Event.Id == 1413) == 2 && clock.PendingTimers == 1
        );
        rabbit.Reopen();
        clock.Advance(RabbitMqRequestBroker.RestoreBackoff * 2 - TimeSpan.FromTicks(1));
        Assert.False(logger.Has(new EventId(1412)));
        Assert.Single(rabbit.Channels);
        clock.Advance(TimeSpan.FromTicks(1));

        await SchedulingTests.WaitUntilAsync(() => logger.Has(new EventId(1412)));
        Assert.Contains(
            "attempt 3",
            Assert.Single(logger.Entries, entry => entry.Event.Id == 1412).Message,
            StringComparison.Ordinal
        );
        Assert.Equal(2, logger.Entries.Count(entry => entry.Event.Id == 1413));
        Assert.NotNull(Assert.Single(rabbit.Channels, channel => channel != cancelled).Consumer);
    }

    [Fact(Timeout = 30_000)]
    public async Task Stopping_a_listener_ends_a_restoration_waiting_to_retry()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        await using var broker = CreateLogged(rabbit, logger, clock, UniqueClient());
        var listener = await broker.ListenAsync(
            "catalog",
            (_, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 1 }),
            token
        );
        rabbit.Drop(ShutdownInitiator.Peer);
        await rabbit.Channels[0].CancelByBrokerAsync();
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);

        await listener.DisposeAsync().AsTask().WaitAsync(Bound, token);

        Assert.Equal(0, clock.PendingTimers);
        rabbit.Reopen();
        clock.Advance(RabbitMqRequestBroker.RestoreBackoffCap);
        Assert.Single(rabbit.Channels);
        Assert.Single(logger.Entries, entry => entry.Event.Id == 1413);
        Assert.False(logger.Has(new EventId(1412)));
    }

    [Fact(Timeout = 30_000)]
    public async Task A_reply_consumer_the_broker_cancels_is_replaced_by_the_next_request()
    {
        var token = TestContext.Current.CancellationToken;
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        var client = UniqueClient();
        using var metrics = new MetricRecorder(client);
        await using var broker = CreateLogged(rabbit, logger, new TestClock(), client);

        var first = await AskAsync(rabbit, broker, [1]);
        Assert.Equal(new byte[] { 1 }, first.Reply);

        // The reply queue was deleted: nothing will ever arrive on this consumer again.
        await first.ReplyChannel.CancelByBrokerAsync();
        var warning = Assert.Single(logger.Entries, entry => entry.Event.Id == 1411);
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal(1, metrics.Sum(Consumers, (EventTag, "cancelled"), (RoleTag, "reply")));

        // The next request declares a new reply queue and is answered on it.
        var second = await AskAsync(rabbit, broker, [2]);
        Assert.Equal(new byte[] { 2 }, second.Reply);
        Assert.NotSame(first.ReplyChannel, second.ReplyChannel);
        await SchedulingTests.WaitUntilAsync(() => broker.ClosingChannelCount == 0);
        await first.ReplyChannel.Channel.Received(1).DisposeAsync();
        Assert.Single(logger.Entries, entry => entry.Event.Id == 1412);
        Assert.Equal(1, metrics.Sum(Consumers, (EventTag, "restored"), (RoleTag, "reply")));

        async Task<(byte[] Reply, FakeChannel ReplyChannel)> AskAsync(
            FakeRabbit fake,
            RabbitMqRequestBroker requester,
            byte[] answer
        )
        {
            var requestId = Guid.NewGuid();
            var id = requestId.ToString("N");
            var pending = requester
                .RequestAsync("orders", "ask"u8.ToArray(), requestId, Bound, token)
                .AsTask();
            await WaitForChannelAsync(
                fake,
                channel => channel.Publishes.Any(published => published.CorrelationId == id)
            );
            // The reply channel is the newest channel with a consumer.
            var replies = fake.Channels.Last(channel => channel.Consumer is not null);
            await replies.DeliverAsync(id, replyTo: null, body: answer);
            return ((await pending.WaitAsync(Bound, token)).ToArray(), replies);
        }
    }

    private static RabbitMqRequestBroker CreateLogged(
        FakeRabbit rabbit,
        RecordingLogger<RabbitMqRequestBroker> logger,
        TestClock clock,
        string clientProvidedName
    ) =>
        new(
            Options.Create(new RabbitMqOptions { ClientProvidedName = clientProvidedName }),
            _ => ValueTask.FromResult(rabbit.Connection),
            logger,
            clock
        );

    /// <summary>
    /// Starts an event subscription on topic <c>catalog</c> as <c>audit</c>, or a request
    /// listener on address <c>catalog</c> that answers with one byte, around the same handler.
    /// </summary>
    private static async Task<IAsyncDisposable> StartConsumerAsync(
        RabbitMqRequestBroker broker,
        bool eventSubscription,
        Func<ReadOnlyMemory<byte>, CancellationToken, ValueTask> handle
    ) =>
        eventSubscription
            ? await broker.SubscribeAsync(
                "catalog",
                "audit",
                (frame, token) => handle(frame, token),
                TestContext.Current.CancellationToken
            )
            : await broker.ListenAsync(
                "catalog",
                async (frame, token) =>
                {
                    await handle(frame, token);
                    return new byte[] { 1 };
                },
                TestContext.Current.CancellationToken
            );
}
