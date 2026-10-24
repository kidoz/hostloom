using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.Tests;

public sealed partial class RabbitMqBrokerTests
{
    [Theory]
    [InlineData(false, 32)]
    [InlineData(false, 33)]
    [InlineData(true, 32)]
    [InlineData(true, 33)]
    public async Task Oversized_deliveries_are_rejected_before_the_raw_handler_runs(
        bool eventFrame,
        int size
    )
    {
        var rabbit = new FakeRabbit();
        await using var broker = new RabbitMqRequestBroker(
            Options.Create(new RabbitMqOptions()),
            _ => ValueTask.FromResult(rabbit.Connection),
            messagingOptions: Options.Create(new HostLoomOptions { MaxFrameBytes = 32 })
        );
        var handled = 0;
        await using var subscription = eventFrame
            ? await broker.SubscribeAsync(
                "orders",
                "audit",
                (_, _) =>
                {
                    handled++;
                    return ValueTask.CompletedTask;
                },
                TestContext.Current.CancellationToken
            )
            : await broker.ListenAsync(
                "orders",
                (_, _) =>
                {
                    handled++;
                    return ValueTask.FromResult(ReadOnlyMemory<byte>.Empty);
                },
                TestContext.Current.CancellationToken
            );
        var channel = Assert.Single(rabbit.Channels);
        await channel.DeliverAsync("correlation", "amq.gen-reply", new byte[size]);
        Assert.Equal(size > 32 ? 0 : 1, handled);
        Assert.Equal(size > 32 ? 1 : 0, channel.Rejects.Count);
        Assert.Equal(size > 32 ? 0 : 1, channel.Acks.Count);
        Assert.Empty(channel.Nacks);
    }

    [Theory]
    [InlineData(32)]
    [InlineData(33)]
    public async Task Oversized_correlated_replies_fail_only_the_waiting_caller(int size)
    {
        var rabbit = new FakeRabbit();
        await using var broker = new RabbitMqRequestBroker(
            Options.Create(new RabbitMqOptions()),
            _ => ValueTask.FromResult(rabbit.Connection),
            messagingOptions: Options.Create(new HostLoomOptions { MaxFrameBytes = 32 })
        );
        async Task<ReadOnlyMemory<byte>> AskAsync(int replySize)
        {
            var id = Guid.NewGuid();
            var pending = broker
                .RequestAsync(
                    "orders",
                    new byte[1],
                    id,
                    TimeSpan.FromSeconds(5),
                    TestContext.Current.CancellationToken
                )
                .AsTask();
            var channel = await WaitForChannelAsync(rabbit, c => c.Consumer is not null);
            await channel.DeliverAsync(id.ToString("N"), null, new byte[replySize]);
            return await pending;
        }
        if (size > 32)
            await Assert.ThrowsAsync<MalformedEnvelopeException>(async () => await AskAsync(size));
        else
            Assert.Equal(32, (await AskAsync(size)).Length);
        Assert.Equal(32, (await AskAsync(32)).Length);
        Assert.Equal(0, broker.PendingRequestCount);
    }
}
