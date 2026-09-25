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
    public async Task Stopping_a_consumer_waits_for_its_handler_in_flight_before_closing_and_requeues_later_deliveries(
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
        // The broker is asked at once to stop delivering, without waiting for its answer, and
        // the handler is cancelled.
        await channel.DeliveriesStopped.WaitAsync(Bound, token);
        await channel
            .Channel.Received(1)
            .BasicCancelAsync("consumer-tag", true, Arg.Any<CancellationToken>());
        await cancelled.Task.WaitAsync(Bound, token);

        // A delivery already on its way goes back to the queue without reaching the handler.
        await channel
            .DeliverAsync("c2", "amq.gen-reply", [2], deliveryTag: 2)
            .WaitAsync(Bound, token);
        Assert.Equal(1, Volatile.Read(ref entered));
        Assert.Equal([2ul], channel.Nacks);
        // The channel stays open while the handler runs, so what it settles reaches the broker.
        Assert.False(closeStarted.IsCompleted);
        Assert.False(disposing.IsCompleted);

        release.SetResult();
        await first.WaitAsync(Bound, token);
        await closeStarted.WaitAsync(Bound, token);
        await disposing.WaitAsync(Bound, token);
        // Once released, the handler saw its cancellation, so its delivery was requeued too.
        Assert.Equal([2ul, 1ul], channel.Nacks);
        Assert.Empty(channel.Rejects);
        Assert.Empty(channel.Acks);
        finishClose.SetResult();
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_handler_that_finishes_in_spite_of_the_stop_is_settled_before_its_channel_closes(
        bool eventSubscription
    )
    {
        var token = TestContext.Current.CancellationToken;
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        var client = UniqueClient();
        using var metrics = new MetricRecorder(client);
        await using var broker = CreateLogged(rabbit, logger, new TestClock(), client);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = await StartConsumerAsync(
            broker,
            eventSubscription,
            async (_, _) =>
            {
                started.TrySetResult();
                // Ignores its cancellation and succeeds, as a handler finishing work it cannot
                // abandon halfway.
                await release.Task.WaitAsync(Bound, token);
            }
        );
        var channel = rabbit.Channels[0];
        // As the client library does: once the close begins, nothing more can be settled.
        channel.RefuseSettlementsOnceClosing();
        var delivery = channel.DeliverAsync("c1", "amq.gen-reply", [1], deliveryTag: 1);
        await started.Task.WaitAsync(Bound, token);

        var disposing = consumer.DisposeAsync().AsTask();
        // Released once the stop has done whatever it does first: ask the broker to stop
        // delivering, or begin closing the channel.
        await Task.WhenAny(channel.DeliveriesStopped, channel.CloseStarted).WaitAsync(Bound, token);
        release.SetResult();
        await delivery.WaitAsync(Bound, token);
        await disposing.WaitAsync(Bound, token);
        await channel.CloseStarted.WaitAsync(Bound, token);

        // The work completed, so it is answered and acknowledged, not redelivered and run again.
        Assert.Equal([1ul], channel.Acks);
        if (!eventSubscription)
        {
            Assert.Equal("c1", Assert.Single(channel.Publishes).CorrelationId);
        }

        Assert.Empty(channel.Rejects);
        Assert.Empty(channel.Nacks);
        Assert.DoesNotContain(logger.Entries, entry => entry.Level >= LogLevel.Warning);
        Assert.Equal(0, metrics.Sum("hostloom.rabbitmq.deliveries.rejected"));
        Assert.Equal(0, metrics.Sum("hostloom.rabbitmq.deliveries.requeued"));
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_handler_still_running_after_the_drain_bound_is_left_behind_and_its_late_settlement_is_not_a_rejection(
        bool eventSubscription
    )
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        var client = UniqueClient();
        using var metrics = new MetricRecorder(client);
        await using var broker = CreateLogged(rabbit, logger, clock, client);
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = await StartConsumerAsync(
            broker,
            eventSubscription,
            async (_, _) =>
            {
                started.TrySetResult();
                await release.Task.WaitAsync(Bound, token);
            }
        );
        var channel = rabbit.Channels[0];
        channel.RefuseSettlementsOnceClosing();
        var delivery = channel.DeliverAsync("c1", "amq.gen-reply", [1], deliveryTag: 1);
        await started.Task.WaitAsync(Bound, token);

        var disposing = consumer.DisposeAsync().AsTask();
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.False(channel.CloseStarted.IsCompleted);
        clock.Advance(RabbitMqRequestBroker.HandlerDrainBound);

        // The stop gives up on the handler, closes the channel without it, and returns.
        await disposing.WaitAsync(Bound, token);
        await channel.CloseStarted.WaitAsync(Bound, token);
        var abandoned = Assert.Single(logger.Entries, entry => entry.Event.Id == 1406);
        Assert.Equal(LogLevel.Warning, abandoned.Level);
        Assert.Equal("RabbitMqHandlersAbandoned", abandoned.Event.Name);

        // Finishing late, the handler cannot settle on the closed channel. That is not a
        // failure of the delivery: the broker redelivers it, and nothing escapes into the client
        // library's dispatcher.
        release.SetResult();
        await delivery.WaitAsync(Bound, token);
        var unsettled = Assert.Single(logger.Entries, entry => entry.Event.Id == 1405);
        Assert.Equal(LogLevel.Warning, unsettled.Level);
        Assert.Equal("RabbitMqDeliveryUnsettled", unsettled.Event.Name);
        Assert.IsType<AlreadyClosedException>(unsettled.Exception);
        Assert.Contains(
            eventSubscription ? "acknowledged" : "answered",
            unsettled.Message,
            StringComparison.Ordinal
        );
        Assert.DoesNotContain(logger.Entries, entry => entry.Event.Id == 1401);
        Assert.Equal(0, metrics.Sum("hostloom.rabbitmq.deliveries.rejected"));
        Assert.Equal(1, metrics.Sum("hostloom.rabbitmq.deliveries.requeued"));
        Assert.Empty(channel.Acks);
        Assert.Empty(channel.Rejects);
    }

    [Fact(Timeout = 30_000)]
    public async Task A_rejection_a_closed_channel_refuses_is_reported_as_unsettled_and_does_not_escape()
    {
        var token = TestContext.Current.CancellationToken;
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        var client = UniqueClient();
        using var metrics = new MetricRecorder(client);
        await using var broker = CreateLogged(rabbit, logger, new TestClock(), client);
        await using var subscription = await StartConsumerAsync(
            broker,
            eventSubscription: true,
            (_, _) => throw new InvalidOperationException("inventory unavailable")
        );
        var channel = rabbit.Channels[0];
        // The connection dropped under a delivery already dispatched to the handler.
        channel.CloseUnderneath();

        await channel
            .DeliverAsync("c1", "amq.gen-reply", [1], deliveryTag: 1)
            .WaitAsync(Bound, token);

        // Neither rejected nor dead-lettered: the broker redelivers it.
        var unsettled = Assert.Single(logger.Entries, entry => entry.Event.Id == 1405);
        Assert.Equal(LogLevel.Warning, unsettled.Level);
        Assert.Contains("rejected", unsettled.Message, StringComparison.Ordinal);
        var causes = Assert.IsType<AggregateException>(unsettled.Exception).InnerExceptions;
        Assert.IsType<InvalidOperationException>(causes[0]);
        Assert.IsType<AlreadyClosedException>(causes[1]);
        Assert.DoesNotContain(logger.Entries, entry => entry.Event.Id == 1401);
        Assert.Equal(0, metrics.Sum("hostloom.rabbitmq.deliveries.rejected"));
        Assert.Equal(1, metrics.Sum("hostloom.rabbitmq.deliveries.requeued"));
    }

    [Fact(Timeout = 30_000)]
    public async Task Disposal_stops_waiting_for_a_connection_close_the_broker_never_answers()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        var broker = CreateLogged(rabbit, logger, clock, UniqueClient());
        // Connects, so there is a connection to close.
        await (
            await broker.ListenAsync("catalog", (frame, _) => ValueTask.FromResult(frame), token)
        ).DisposeAsync();
        var closeStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var finishClose = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        // As the client library behaves against a broker that stopped answering: the close waits
        // for a close-ok, then for channel zero, about 25 seconds in all.
        rabbit
            .Connection.DisposeAsync()
            .Returns(_ =>
            {
                closeStarted.TrySetResult();
                return new ValueTask(finishClose.Task);
            });

        var disposing = broker.DisposeAsync().AsTask();
        await closeStarted.Task.WaitAsync(Bound, token);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(RabbitMqRequestBroker.ConnectionCloseBound - TimeSpan.FromTicks(1));
        Assert.False(disposing.IsCompleted);
        clock.Advance(TimeSpan.FromTicks(1));

        await disposing.WaitAsync(Bound, token);
        var warning = Assert.Single(logger.Entries, entry => entry.Event.Id == 1407);
        Assert.Equal(LogLevel.Warning, warning.Level);
        Assert.Equal("RabbitMqConnectionStillClosing", warning.Event.Name);

        // The close finishes in the background; a failure then is logged, never thrown.
        finishClose.SetException(new InvalidOperationException("socket already gone"));
        await SchedulingTests.WaitUntilAsync(() =>
            logger.Entries.Count(entry => entry.Event.Id == 1407) == 2
        );
        Assert.IsType<InvalidOperationException>(
            logger.Entries.Last(entry => entry.Event.Id == 1407).Exception
        );
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
        // Restoring recovers neither what the queue held nor what was sent while it was gone,
        // and the warning must not suggest otherwise.
        Assert.Contains(
            "whatever is sent before the queue is declared again",
            warning.Message,
            StringComparison.Ordinal
        );
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
