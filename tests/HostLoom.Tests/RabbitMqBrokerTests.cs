using System.Diagnostics;
using System.Text;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// Drives the RabbitMQ broker against fake <see cref="IConnection"/> and <see cref="IChannel"/>
/// instances, so request/reply correlation is verified without a broker. Deliveries are injected by
/// capturing the consumer the broker registers and invoking it directly.
/// </summary>
public sealed class RabbitMqBrokerTests
{
    [Fact]
    public async Task Request_publishes_its_correlation_id_and_exclusive_reply_queue()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);
        var requestId = Guid.NewGuid();

        var pending = broker
            .RequestAsync("greetings", Encoding.UTF8.GetBytes("ping"), requestId, TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
            .AsTask();

        var client = await WaitForChannelAsync(rabbit, channel => channel.Publishes.Count == 1);
        var published = client.Publishes[0];

        Assert.Equal(string.Empty, published.Exchange);
        Assert.Equal("greetings", published.RoutingKey);
        Assert.Equal(requestId.ToString("N"), published.CorrelationId);
        Assert.Equal(FakeRabbit.GeneratedReplyQueue, published.ReplyTo);
        Assert.Equal("ping", Encoding.UTF8.GetString(published.Body));

        await client.DeliverAsync(requestId.ToString("N"), replyTo: null, body: Encoding.UTF8.GetBytes("pong"));
        var response = await pending;
        Assert.Equal("pong", Encoding.UTF8.GetString(response.ToArray()));
    }

    [Fact]
    public async Task A_reply_for_another_request_never_completes_this_one()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        var pending = broker
            .RequestAsync("greetings", Encoding.UTF8.GetBytes("ping"), Guid.NewGuid(), TimeSpan.FromMilliseconds(300), TestContext.Current.CancellationToken)
            .AsTask();

        var client = await WaitForChannelAsync(rabbit, channel => channel.Publishes.Count == 1);
        await client.DeliverAsync(Guid.NewGuid().ToString("N"), replyTo: null, body: Encoding.UTF8.GetBytes("not yours"));

        // The correlation id does not match, so the reply is dropped and the request still times out.
        await Assert.ThrowsAsync<RequestTimeoutException>(async () => await pending);
    }

    [Fact]
    public async Task Reusing_a_pending_request_id_is_rejected()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);
        var requestId = Guid.NewGuid();

        var first = broker
            .RequestAsync("greetings", Encoding.UTF8.GetBytes("one"), requestId, TimeSpan.FromMilliseconds(400), TestContext.Current.CancellationToken)
            .AsTask();
        await WaitForChannelAsync(rabbit, channel => channel.Publishes.Count == 1);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await broker.RequestAsync("greetings", Encoding.UTF8.GetBytes("two"), requestId, TimeSpan.FromSeconds(1), TestContext.Current.CancellationToken));

        await Assert.ThrowsAsync<RequestTimeoutException>(async () => await first);
    }

    [Fact]
    public async Task A_delivered_request_is_answered_on_its_reply_queue_and_acknowledged()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        await using var subscription = await broker.ListenAsync(
            "greetings",
            (_, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("handled")),
            TestContext.Current.CancellationToken);

        var listener = rabbit.Channels[0];
        await listener.DeliverAsync("corr-1", replyTo: "reply-queue", body: Encoding.UTF8.GetBytes("ask"), deliveryTag: 7);

        var published = Assert.Single(listener.Publishes);
        Assert.Equal("reply-queue", published.RoutingKey);
        Assert.Equal("corr-1", published.CorrelationId);
        Assert.Equal("handled", Encoding.UTF8.GetString(published.Body));
        Assert.Equal([7ul], listener.Acks);
        Assert.Empty(listener.Rejects);
    }

    [Fact]
    public async Task A_request_without_a_reply_queue_is_rejected_without_requeue()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        await using var subscription = await broker.ListenAsync(
            "greetings",
            (_, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>(Encoding.UTF8.GetBytes("handled")),
            TestContext.Current.CancellationToken);

        var listener = rabbit.Channels[0];
        await listener.DeliverAsync("corr-1", replyTo: null, body: Encoding.UTF8.GetBytes("ask"), deliveryTag: 9);

        // Nowhere to send the reply, so the delivery is dropped rather than requeued forever.
        Assert.Empty(listener.Publishes);
        Assert.Equal([9ul], listener.Rejects);
        Assert.Empty(listener.Acks);
    }

    [Fact]
    public async Task A_failing_handler_rejects_the_delivery_without_requeue()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        await using var subscription = await broker.ListenAsync(
            "greetings",
            (_, _) => throw new InvalidOperationException("handler blew up"),
            TestContext.Current.CancellationToken);

        var listener = rabbit.Channels[0];
        await listener.DeliverAsync("corr-1", replyTo: "reply-queue", body: Encoding.UTF8.GetBytes("ask"), deliveryTag: 11);

        Assert.Empty(listener.Publishes);
        Assert.Equal([11ul], listener.Rejects);
    }

    private static RabbitMqRequestBroker Create(FakeRabbit rabbit) =>
        new(Options.Create(new RabbitMqOptions()), _ => ValueTask.FromResult(rabbit.Connection));

    private static async Task<FakeChannel> WaitForChannelAsync(FakeRabbit rabbit, Func<FakeChannel, bool> ready)
    {
        var stopwatch = Stopwatch.StartNew();
        while (stopwatch.Elapsed < TimeSpan.FromSeconds(5))
        {
            var match = rabbit.Channels.FirstOrDefault(channel => ready(channel));
            if (match is not null)
            {
                return match;
            }

            await Task.Delay(5, TestContext.Current.CancellationToken);
        }

        Assert.Fail("Timed out waiting for the expected channel activity.");
        throw new UnreachableException();
    }

    private sealed record Published(string Exchange, string RoutingKey, string? CorrelationId, string? ReplyTo, byte[] Body);

    private sealed class FakeRabbit
    {
        public const string GeneratedReplyQueue = "amq.gen-hostloom-reply";

        private readonly Lock _gate = new();
        private readonly List<FakeChannel> _channels = [];

        public FakeRabbit()
        {
            Connection = Substitute.For<IConnection>();
            Connection.IsOpen.Returns(true);
            Connection
                .CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>())
                .Returns(_ =>
                {
                    var channel = new FakeChannel();
                    lock (_gate)
                    {
                        _channels.Add(channel);
                    }

                    return Task.FromResult(channel.Channel);
                });
        }

        public IConnection Connection { get; }

        public List<FakeChannel> Channels
        {
            get
            {
                lock (_gate)
                {
                    return _channels.ToList();
                }
            }
        }
    }

    private sealed class FakeChannel
    {
        private readonly Lock _gate = new();
        private readonly List<Published> _publishes = [];

        public FakeChannel()
        {
            Channel = Substitute.For<IChannel>();
            Channel.IsOpen.Returns(true);

            Channel
                .QueueDeclareAsync(
                    Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<bool>(),
                    Arg.Any<IDictionary<string, object?>?>(), Arg.Any<bool>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    var requested = call.ArgAt<string>(0);
                    var name = string.IsNullOrEmpty(requested) ? FakeRabbit.GeneratedReplyQueue : requested;
                    return Task.FromResult(new QueueDeclareOk(name, 0, 0));
                });

            Channel
                .BasicConsumeAsync(
                    Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<string>(), Arg.Any<bool>(), Arg.Any<bool>(),
                    Arg.Any<IDictionary<string, object?>?>(), Arg.Any<IAsyncBasicConsumer>(), Arg.Any<CancellationToken>())
                .Returns(call =>
                {
                    Consumer = call.ArgAt<IAsyncBasicConsumer>(6);
                    return Task.FromResult("consumer-tag");
                });

            Channel
                .When(c => c.BasicPublishAsync(
                    Arg.Any<string>(), Arg.Any<string>(), Arg.Any<bool>(),
                    Arg.Any<BasicProperties>(), Arg.Any<ReadOnlyMemory<byte>>(), Arg.Any<CancellationToken>()))
                .Do(call =>
                {
                    var properties = call.ArgAt<BasicProperties>(3);
                    var body = call.ArgAt<ReadOnlyMemory<byte>>(4);
                    lock (_gate)
                    {
                        _publishes.Add(new Published(
                            call.ArgAt<string>(0),
                            call.ArgAt<string>(1),
                            properties.CorrelationId,
                            properties.ReplyTo,
                            body.ToArray()));
                    }
                });

            Channel
                .When(c => c.BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>()))
                .Do(call => Acks.Add(call.ArgAt<ulong>(0)));

            Channel
                .When(c => c.BasicRejectAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>()))
                .Do(call => Rejects.Add(call.ArgAt<ulong>(0)));
        }

        public IChannel Channel { get; }

        public IAsyncBasicConsumer? Consumer { get; private set; }

        public List<ulong> Acks { get; } = [];

        public List<ulong> Rejects { get; } = [];

        public List<Published> Publishes
        {
            get
            {
                lock (_gate)
                {
                    return _publishes.ToList();
                }
            }
        }

        public async Task DeliverAsync(string correlationId, string? replyTo, byte[] body, ulong deliveryTag = 1)
        {
            var consumer = Consumer ?? throw new InvalidOperationException("No consumer has been registered yet.");
            var properties = new BasicProperties { CorrelationId = correlationId, ReplyTo = replyTo };
            await consumer.HandleBasicDeliverAsync(
                "consumer-tag",
                deliveryTag,
                redelivered: false,
                exchange: string.Empty,
                routingKey: "queue",
                properties,
                body,
                CancellationToken.None);
        }
    }
}
