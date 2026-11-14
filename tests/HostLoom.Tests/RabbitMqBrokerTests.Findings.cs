using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Logging;
using RabbitMQ.Client;
using Xunit;

namespace HostLoom.Tests;

public sealed partial class RabbitMqBrokerTests
{
    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_pause_resume_retries_transient_failure_and_retires_the_previous_channel(
        bool eventSubscription
    )
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var rabbit = new FakeRabbit();
        var logger = new RecordingLogger<RabbitMqRequestBroker>();
        await using var broker = CreateLogged(rabbit, logger, clock, UniqueClient());
        await using var subscription = await StartConsumerAsync(
            broker,
            eventSubscription,
            (_, _) =>
                throw new ReceiveSuspendedException(
                    "catalog",
                    null,
                    clock.GetUtcNow().AddSeconds(1)
                )
        );
        var original = rabbit.Channels[0];
        await original.DeliverAsync("c1", "amq.gen-reply", [1]).WaitAsync(Bound, token);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        rabbit.Drop(ShutdownInitiator.Peer);
        clock.Advance(TimeSpan.FromSeconds(1));
        await SchedulingTests.WaitUntilAsync(() =>
            logger.Has(new EventId(1413)) && clock.PendingTimers == 1
        );
        rabbit.Reopen();
        clock.Advance(RabbitMqRequestBroker.RestoreBackoff);
        await SchedulingTests.WaitUntilAsync(() => logger.Has(new EventId(1412)));
        await SchedulingTests.WaitUntilAsync(() => !original.Channel.IsOpen);
        Assert.Equal(2, rabbit.Channels.Count);
    }
}
