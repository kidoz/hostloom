using System.Collections.Concurrent;
using System.Text;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Options;
using Xunit;

namespace HostLoom.IntegrationTests;

public sealed class RabbitMqRoutingTests
{
    public static bool Available => BrokerAvailability.RabbitMq;

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task Dotted_topic_subscription_pairs_deliver_to_distinct_queues()
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        var token = deadline.Token;
        var topic = "routing-" + Guid.NewGuid().ToString("N");
        var first = new ConcurrentQueue<string>();
        var second = new ConcurrentQueue<string>();
        var done = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;
        // Declared before the broker so it is disposed after it: the queues are deleted once
        // nothing consumes them.
        await using var scope = new RabbitMqTopologyScope();
        scope.Subscription(topic + ".eu", "updates");
        scope.Subscription(topic, "eu.updates");
        await using var broker = new RabbitMqRequestBroker(Options.Create(new RabbitMqOptions()));
        ValueTask Receive(ConcurrentQueue<string> target, ReadOnlyMemory<byte> bytes)
        {
            target.Enqueue(Encoding.UTF8.GetString(bytes.Span));
            if (Interlocked.Increment(ref count) == 40)
                done.TrySetResult();
            return ValueTask.CompletedTask;
        }
        await using var a = await broker.SubscribeAsync(
            topic + ".eu",
            "updates",
            (bytes, _) => Receive(first, bytes),
            token
        );
        await using var b = await broker.SubscribeAsync(
            topic,
            "eu.updates",
            (bytes, _) => Receive(second, bytes),
            token
        );
        for (var i = 0; i < 20; i++)
            await broker.PublishAsync(topic + ".eu", "catalog"u8.ToArray(), token);
        for (var i = 0; i < 20; i++)
            await broker.PublishAsync(topic, "inventory"u8.ToArray(), token);
        await done.Task.WaitAsync(token);
        Assert.Equal(20, first.Count);
        Assert.Equal(20, second.Count);
        Assert.All(first, value => Assert.Equal("catalog", value));
        Assert.All(second, value => Assert.Equal("inventory", value));
    }

    [Theory(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    [InlineData(RabbitMqQueueNaming.Version2)]
    [InlineData(RabbitMqQueueNaming.Legacy)]
    public async Task Matching_queue_modes_preserve_request_round_trips(RabbitMqQueueNaming naming)
    {
        var address = "request-routing-" + Guid.NewGuid().ToString("N");
        var token = TestContext.Current.CancellationToken;
        await using var scope = new RabbitMqTopologyScope(naming);
        scope.Request(address);
        await using var broker = new RabbitMqRequestBroker(
            Options.Create(new RabbitMqOptions { QueueNaming = naming })
        );
        await using var listener = await broker.ListenAsync(
            address,
            (bytes, _) => ValueTask.FromResult(bytes),
            token
        );
        var response = await broker.RequestAsync(
            address,
            "catalog"u8.ToArray(),
            Guid.NewGuid(),
            TimeSpan.FromSeconds(5),
            token
        );
        Assert.Equal("catalog", Encoding.UTF8.GetString(response.Span));
    }
}
