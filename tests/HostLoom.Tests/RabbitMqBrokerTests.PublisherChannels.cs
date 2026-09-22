using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The publisher channel pool against channels whose close never finishes, as a broker that has
/// stopped answering leaves them: a publication or request still ends at its own deadline, its
/// permit is free at once, channels still closing are capped, and disposal waits for them only
/// for a bounded time. Deadlines run on a <see cref="TestClock"/>, so nothing waits them out.
/// </summary>
public sealed partial class RabbitMqBrokerTests
{
    private static readonly TimeSpan PublishDeadline = TimeSpan.FromSeconds(1);
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    [Fact(Timeout = 30_000)]
    public async Task A_timed_out_publish_returns_at_its_deadline_and_frees_its_permit_while_its_channel_closes()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        var client = UniqueClient();
        using var metrics = new MetricRecorder(client);
        await using var broker = Create(rabbit, clock, maxConcurrentPublishes: 2, client);
        await broker.PublishAsync("catalog", new byte[] { 1 }, token);
        var stalledChannel = rabbit.Channels[0];
        var entered = stalledChannel.StallPublishes();
        var (closeStarted, finishClose) = stalledChannel.HoldClose();

        var stalled = broker.PublishAsync("catalog", new byte[] { 2 }, token).AsTask();
        await entered.WaitAsync(Bound, token);
        clock.Advance(PublishDeadline);

        // The deadline ends the call although the channel it gave up has not finished closing.
        await Assert.ThrowsAsync<TimeoutException>(() => stalled.WaitAsync(Bound, token));
        await closeStarted.WaitAsync(Bound, token);
        Assert.Equal(1, broker.ClosingChannelCount);
        metrics.Observe();
        Assert.Equal(1, metrics.Last("hostloom.rabbitmq.channels.closing"));
        Assert.Equal(
            1,
            metrics.Sum("hostloom.rabbitmq.publishes", ("hostloom.rabbitmq.outcome", "timed_out"))
        );

        // Both permits are free again: two publications own a new channel each at the same time,
        // while the abandoned one is still closing.
        var opening = rabbit.HoldNewChannels();
        var first = broker.PublishAsync("catalog", new byte[] { 3 }, token).AsTask();
        var second = broker.PublishAsync("catalog", new byte[] { 4 }, token).AsTask();
        await rabbit.WaitForChannelRequestsAsync(3, token).WaitAsync(Bound, token);
        opening.SetResult();
        await Task.WhenAll(first, second).WaitAsync(Bound, token);
        Assert.DoesNotContain(stalledChannel.Publishes, p => p.Body[0] > 2);

        finishClose.SetResult();
        await SchedulingTests.WaitUntilAsync(() => broker.ClosingChannelCount == 0);
        metrics.Observe();
        Assert.Equal(0, metrics.Last("hostloom.rabbitmq.channels.closing"));
    }

    [Fact(Timeout = 30_000)]
    public async Task Channels_still_closing_are_capped_and_a_new_channel_waits_within_its_deadline()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        var client = UniqueClient();
        using var metrics = new MetricRecorder(client);
        await using var broker = Create(rabbit, clock, maxConcurrentPublishes: 1, client);
        await broker.PublishAsync("catalog", new byte[] { 1 }, token);
        var stalledChannel = rabbit.Channels[0];
        var entered = stalledChannel.StallPublishes();
        var (_, finishClose) = stalledChannel.HoldClose();
        var stalled = broker.PublishAsync("catalog", new byte[] { 2 }, token).AsTask();
        await entered.WaitAsync(Bound, token);
        clock.Advance(PublishDeadline);
        await Assert.ThrowsAsync<TimeoutException>(() => stalled.WaitAsync(Bound, token));

        // One channel closing is the cap here. The permit is free, but the next publication needs
        // a new channel, so it waits for the close without opening one, and only for its own
        // deadline. Up to that wait everything completes synchronously against the fakes.
        var waiting = broker.PublishAsync("catalog", new byte[] { 3 }, token).AsTask();
        Assert.False(waiting.IsCompleted);
        Assert.Single(rabbit.Channels);
        clock.Advance(PublishDeadline);
        await Assert.ThrowsAsync<TimeoutException>(() => waiting.WaitAsync(Bound, token));
        Assert.Single(rabbit.Channels);
        Assert.Equal(
            2,
            metrics.Sum("hostloom.rabbitmq.publishes", ("hostloom.rabbitmq.outcome", "timed_out"))
        );

        // The close finishing frees the slot for a publication already waiting on it.
        var next = broker.PublishAsync("catalog", new byte[] { 4 }, token).AsTask();
        Assert.False(next.IsCompleted);
        finishClose.SetResult();
        await next.WaitAsync(Bound, token);
        Assert.Equal(2, rabbit.Channels.Count);
        Assert.Equal(4, Assert.Single(rabbit.Channels[1].Publishes).Body[0]);
    }

    [Fact(Timeout = 30_000)]
    public async Task A_timed_out_request_returns_at_its_deadline_and_the_next_is_answered_while_its_channel_closes()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit, clock, maxConcurrentPublishes: 2, UniqueClient());
        await broker.PublishAsync("catalog", new byte[] { 1 }, token);
        var stalledChannel = rabbit.Channels[0];
        var entered = stalledChannel.StallPublishes();
        var (closeStarted, finishClose) = stalledChannel.HoldClose();
        var timeout = TimeSpan.FromSeconds(3);

        var stalled = broker
            .RequestAsync("orders", new byte[] { 2 }, Guid.NewGuid(), timeout, token)
            .AsTask();
        await entered.WaitAsync(Bound, token);
        clock.Advance(timeout);

        var timedOut = await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            stalled.WaitAsync(Bound, token)
        );
        Assert.Equal(timeout, timedOut.Timeout);
        await closeStarted.WaitAsync(Bound, token);
        Assert.Equal(1, broker.ClosingChannelCount);
        Assert.Equal(0, broker.PendingRequestCount);

        var id = Guid.NewGuid();
        var answered = broker.RequestAsync("orders", new byte[] { 3 }, id, timeout, token).AsTask();
        var publisher = await WaitForChannelAsync(
            rabbit,
            channel => channel.Publishes.Any(p => p.CorrelationId == id.ToString("N"))
        );
        Assert.NotSame(stalledChannel, publisher);
        await rabbit
            .Channels.Single(channel => channel.Consumer is not null)
            .DeliverAsync(id.ToString("N"), replyTo: null, body: new byte[] { 4 });
        Assert.Equal(new byte[] { 4 }, (await answered.WaitAsync(Bound, token)).ToArray());
        finishClose.SetResult();
    }

    [Fact(Timeout = 30_000)]
    public async Task Disposal_waits_a_bounded_time_for_a_channel_that_never_finishes_closing()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        var broker = new RabbitMqRequestBroker(
            Options.Create(
                new RabbitMqOptions
                {
                    MaxConcurrentPublishes = 1,
                    PublishTimeout = PublishDeadline,
                    ClientProvidedName = UniqueClient(),
                }
            ),
            _ => ValueTask.FromResult(rabbit.Connection),
            logger,
            clock
        );
        await broker.PublishAsync("catalog", new byte[] { 1 }, token);
        var stalledChannel = rabbit.Channels[0];
        var entered = stalledChannel.StallPublishes();
        var (closeStarted, finishClose) = stalledChannel.HoldClose();
        var stalled = broker.PublishAsync("catalog", new byte[] { 2 }, token).AsTask();
        await entered.WaitAsync(Bound, token);
        clock.Advance(PublishDeadline);
        await Assert.ThrowsAsync<TimeoutException>(() => stalled.WaitAsync(Bound, token));
        await closeStarted.WaitAsync(Bound, token);

        var disposing = broker.DisposeAsync().AsTask();
        // The only timer left is disposal's bound on the channels still closing.
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        Assert.False(disposing.IsCompleted);
        clock.Advance(RabbitMqRequestBroker.ChannelCloseBound - TimeSpan.FromTicks(1));
        Assert.False(disposing.IsCompleted);
        clock.Advance(TimeSpan.FromTicks(1));

        await disposing.WaitAsync(Bound, token);
        await rabbit.Connection.Received(1).DisposeAsync();
        var warning = Assert.Single(logger.Entries, entry => entry.Event.Id == 1403);
        Assert.Equal(LogLevel.Warning, warning.Level);
        finishClose.SetResult();
    }

    [Fact(Timeout = 30_000)]
    public async Task A_channel_whose_publication_was_nacked_is_closed_and_never_reused()
    {
        var token = TestContext.Current.CancellationToken;
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);
        await broker.PublishAsync("catalog", new byte[] { 1 }, token);
        var nacked = rabbit.Channels[0];
        nacked
            .Channel.BasicPublishAsync(
                Arg.Any<string>(),
                Arg.Any<string>(),
                Arg.Any<bool>(),
                Arg.Any<BasicProperties>(),
                Arg.Any<ReadOnlyMemory<byte>>(),
                Arg.Any<CancellationToken>()
            )
            .Returns(_ => ValueTask.FromException(new PublishException(2, isReturn: false)));
        var (closeStarted, finishClose) = nacked.HoldClose();
        finishClose.SetResult();

        var failure = await Assert.ThrowsAsync<MessagingTransportException>(() =>
            broker.PublishAsync("catalog", new byte[] { 2 }, token).AsTask()
        );
        Assert.IsType<PublishException>(failure.InnerException);
        await closeStarted.WaitAsync(Bound, token);
        await broker.PublishAsync("catalog", new byte[] { 3 }, token);

        Assert.Equal(2, rabbit.Channels.Count);
        Assert.Equal(3, Assert.Single(rabbit.Channels[1].Publishes).Body[0]);
    }

    private static RabbitMqRequestBroker Create(
        FakeRabbit rabbit,
        TestClock clock,
        int maxConcurrentPublishes,
        string clientProvidedName
    ) =>
        new(
            Options.Create(
                new RabbitMqOptions
                {
                    MaxConcurrentPublishes = maxConcurrentPublishes,
                    PublishTimeout = PublishDeadline,
                    ClientProvidedName = clientProvidedName,
                }
            ),
            _ => ValueTask.FromResult(rabbit.Connection),
            logger: null,
            clock
        );
}
