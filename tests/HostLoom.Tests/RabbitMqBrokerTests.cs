using System.Diagnostics;
using System.Text;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
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
            .RequestAsync(
                "greetings",
                Encoding.UTF8.GetBytes("ping"),
                requestId,
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            )
            .AsTask();

        var client = await WaitForChannelAsync(rabbit, channel => channel.Publishes.Count == 1);
        var published = client.Publishes[0];

        Assert.Equal(string.Empty, published.Exchange);
        Assert.Equal("greetings", published.RoutingKey);
        Assert.Equal(requestId.ToString("N"), published.CorrelationId);
        Assert.Equal(FakeRabbit.GeneratedReplyQueue, published.ReplyTo);
        Assert.Equal("ping", Encoding.UTF8.GetString(published.Body));

        await client.DeliverAsync(
            requestId.ToString("N"),
            replyTo: null,
            body: Encoding.UTF8.GetBytes("pong")
        );
        var response = await pending;
        Assert.Equal("pong", Encoding.UTF8.GetString(response.ToArray()));
    }

    [Fact]
    public async Task A_reply_for_another_request_never_completes_this_one()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        var pending = broker
            .RequestAsync(
                "greetings",
                Encoding.UTF8.GetBytes("ping"),
                Guid.NewGuid(),
                TimeSpan.FromMilliseconds(300),
                TestContext.Current.CancellationToken
            )
            .AsTask();

        var client = await WaitForChannelAsync(rabbit, channel => channel.Publishes.Count == 1);
        await client.DeliverAsync(
            Guid.NewGuid().ToString("N"),
            replyTo: null,
            body: Encoding.UTF8.GetBytes("not yours")
        );

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
            .RequestAsync(
                "greetings",
                Encoding.UTF8.GetBytes("one"),
                requestId,
                TimeSpan.FromMilliseconds(400),
                TestContext.Current.CancellationToken
            )
            .AsTask();
        await WaitForChannelAsync(rabbit, channel => channel.Publishes.Count == 1);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await broker.RequestAsync(
                "greetings",
                Encoding.UTF8.GetBytes("two"),
                requestId,
                TimeSpan.FromSeconds(1),
                TestContext.Current.CancellationToken
            )
        );

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
            TestContext.Current.CancellationToken
        );

        var listener = rabbit.Channels[0];
        await listener.DeliverAsync(
            "corr-1",
            replyTo: "reply-queue",
            body: Encoding.UTF8.GetBytes("ask"),
            deliveryTag: 7
        );

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
            TestContext.Current.CancellationToken
        );

        var listener = rabbit.Channels[0];
        await listener.DeliverAsync(
            "corr-1",
            replyTo: null,
            body: Encoding.UTF8.GetBytes("ask"),
            deliveryTag: 9
        );

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
            TestContext.Current.CancellationToken
        );

        var listener = rabbit.Channels[0];
        await listener.DeliverAsync(
            "corr-1",
            replyTo: "reply-queue",
            body: Encoding.UTF8.GetBytes("ask"),
            deliveryTag: 11
        );

        Assert.Empty(listener.Publishes);
        Assert.Equal([11ul], listener.Rejects);
    }

    [Fact]
    public async Task Subscribing_binds_a_named_queue_to_a_fanout_topic_exchange()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        await using var subscription = await broker.SubscribeAsync(
            "orders",
            "audit",
            (_, _) => ValueTask.CompletedTask,
            TestContext.Current.CancellationToken
        );

        var channel = rabbit.Channels[0];
        var exchange = Assert.Single(channel.Exchanges);
        Assert.Equal("orders", exchange.Name);
        Assert.Equal("fanout", exchange.Type);
        Assert.True(exchange.Durable);

        // The queue is named for the subscription, so a second subscriber gets its own backlog
        // rather than competing for the first one's messages.
        var binding = Assert.Single(channel.Bindings);
        Assert.Equal("orders.audit", binding.Queue);
        Assert.Equal("orders", binding.Exchange);
        Assert.Equal(string.Empty, binding.RoutingKey);
    }

    [Fact]
    public async Task Two_subscriptions_on_one_topic_get_separate_queues()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        await using var audit = await broker.SubscribeAsync(
            "orders",
            "audit",
            (_, _) => ValueTask.CompletedTask,
            TestContext.Current.CancellationToken
        );
        await using var shipping = await broker.SubscribeAsync(
            "orders",
            "shipping",
            (_, _) => ValueTask.CompletedTask,
            TestContext.Current.CancellationToken
        );

        var queues = rabbit
            .Channels.SelectMany(channel => channel.Bindings)
            .Select(binding => binding.Queue)
            .Order(StringComparer.Ordinal);
        Assert.Equal(["orders.audit", "orders.shipping"], queues);
    }

    [Fact]
    public async Task Publishing_an_event_goes_to_the_topic_exchange_without_a_routing_key()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        await broker.PublishAsync(
            "orders",
            Encoding.UTF8.GetBytes("placed"),
            TestContext.Current.CancellationToken
        );

        var client = rabbit.Channels[0];
        Assert.Contains(
            client.Exchanges,
            exchange => exchange is { Name: "orders", Type: "fanout" }
        );

        var published = Assert.Single(client.Publishes);
        Assert.Equal("orders", published.Exchange);
        Assert.Equal(string.Empty, published.RoutingKey);
        Assert.Equal("placed", Encoding.UTF8.GetString(published.Body));
    }

    [Fact]
    public async Task A_topic_exchange_is_declared_once_however_often_it_is_published_to()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        for (var i = 0; i < 3; i++)
        {
            await broker.PublishAsync(
                "orders",
                Encoding.UTF8.GetBytes($"e{i}"),
                TestContext.Current.CancellationToken
            );
        }

        Assert.Single(rabbit.Channels[0].Exchanges);
        Assert.Equal(3, rabbit.Channels[0].Publishes.Count);
    }

    [Fact]
    public async Task A_delivered_event_is_acknowledged_and_a_failing_one_is_rejected()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);
        var handled = new List<string>();

        await using var subscription = await broker.SubscribeAsync(
            "orders",
            "audit",
            (frame, _) =>
            {
                var body = Encoding.UTF8.GetString(frame.Span);
                handled.Add(body);
                return body == "bad"
                    ? throw new InvalidOperationException("subscriber failed")
                    : ValueTask.CompletedTask;
            },
            TestContext.Current.CancellationToken
        );

        var channel = rabbit.Channels[0];
        await channel.DeliverAsync(
            "c1",
            replyTo: null,
            body: Encoding.UTF8.GetBytes("good"),
            deliveryTag: 1
        );
        await channel.DeliverAsync(
            "c2",
            replyTo: null,
            body: Encoding.UTF8.GetBytes("bad"),
            deliveryTag: 2
        );

        Assert.Equal(["good", "bad"], handled);
        Assert.Equal([1ul], channel.Acks);
        Assert.Equal([2ul], channel.Rejects);
    }

    [Fact]
    public async Task A_dropped_broker_does_not_replace_the_connection_recovery_owns()
    {
        var rabbit = new FakeRabbit();
        var connections = 0;
        await using var broker = new RabbitMqRequestBroker(
            Options.Create(new RabbitMqOptions()),
            _ =>
            {
                connections++;
                return ValueTask.FromResult(rabbit.Connection);
            }
        );

        await broker
            .PublishAsync(
                "orders",
                Encoding.UTF8.GetBytes("first"),
                TestContext.Current.CancellationToken
            )
            .ConfigureAwait(true);
        Assert.Equal(1, connections);

        // The broker drops: the connection closes with a peer-initiated reason, which is what
        // automatic recovery restores. Replacing it would strand every consumer created on it,
        // silently, while publishing carried on — so the publish is expected to fail here rather
        // than to succeed against a replacement.
        rabbit.Drop(ShutdownInitiator.Peer);

        await Assert
            .ThrowsAnyAsync<Exception>(async () =>
                await broker.PublishAsync(
                    "orders",
                    Encoding.UTF8.GetBytes("during-outage"),
                    TestContext.Current.CancellationToken
                )
            )
            .ConfigureAwait(true);
        Assert.Equal(1, connections);
    }

    [Fact]
    public async Task A_reply_queue_renamed_by_recovery_is_followed()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit);

        var first = broker
            .RequestAsync(
                "greetings",
                Encoding.UTF8.GetBytes("before"),
                Guid.NewGuid(),
                TimeSpan.FromMilliseconds(300),
                TestContext.Current.CancellationToken
            )
            .AsTask();
        var client = await WaitForChannelAsync(rabbit, channel => channel.Publishes.Count == 1);
        Assert.Equal(FakeRabbit.GeneratedReplyQueue, client.Publishes[0].ReplyTo);
        await Assert.ThrowsAsync<RequestTimeoutException>(async () => await first);

        // The reply queue is server-named and exclusive, so recovery re-declares it under a new
        // name. Keeping the connection through a drop is what makes this reachable: the recovered
        // channel reports itself open, so nothing re-declares the reply path on its own.
        rabbit.RenameReplyQueue("amq.gen-after-recovery");

        var second = broker
            .RequestAsync(
                "greetings",
                Encoding.UTF8.GetBytes("after"),
                Guid.NewGuid(),
                TimeSpan.FromMilliseconds(300),
                TestContext.Current.CancellationToken
            )
            .AsTask();
        await WaitForChannelAsync(rabbit, channel => channel.Publishes.Count == 2);

        Assert.Equal("amq.gen-after-recovery", client.Publishes[1].ReplyTo);
        await Assert.ThrowsAsync<RequestTimeoutException>(async () => await second);
    }

    [Fact]
    public async Task A_connection_closed_by_the_application_is_replaced()
    {
        var rabbit = new FakeRabbit();
        var connections = 0;
        await using var broker = new RabbitMqRequestBroker(
            Options.Create(new RabbitMqOptions()),
            _ =>
            {
                connections++;
                rabbit.Reopen();
                return ValueTask.FromResult(rabbit.Connection);
            }
        );

        await broker
            .PublishAsync(
                "orders",
                Encoding.UTF8.GetBytes("first"),
                TestContext.Current.CancellationToken
            )
            .ConfigureAwait(true);

        // Nothing will restore an application-initiated close, so a new connection is the only
        // way forward and the broker must still make one.
        rabbit.Drop(ShutdownInitiator.Application);

        await broker
            .PublishAsync(
                "orders",
                Encoding.UTF8.GetBytes("after-close"),
                TestContext.Current.CancellationToken
            )
            .ConfigureAwait(true);

        Assert.Equal(2, connections);
    }

    private static RabbitMqRequestBroker Create(FakeRabbit rabbit) =>
        new(Options.Create(new RabbitMqOptions()), _ => ValueTask.FromResult(rabbit.Connection));

    private static async Task<FakeChannel> WaitForChannelAsync(
        FakeRabbit rabbit,
        Func<FakeChannel, bool> ready
    )
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

    private sealed record Published(
        string Exchange,
        string RoutingKey,
        string? CorrelationId,
        string? ReplyTo,
        byte[] Body
    );

    private sealed record DeclaredExchange(string Name, string Type, bool Durable);

    private sealed record Binding(string Queue, string Exchange, string RoutingKey);

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

        /// <summary>Closes the connection with the given initiator, as a real shutdown would.</summary>
        public void Drop(ShutdownInitiator initiator)
        {
            Connection.IsOpen.Returns(false);
            Connection.CloseReason.Returns(
                new ShutdownEventArgs(initiator, 320, "connection dropped")
            );

            // Channels die with their connection, which is what makes the broker re-enter
            // EnsureConnectionAsync rather than keep publishing on a cached channel.
            lock (_gate)
            {
                foreach (var channel in _channels)
                {
                    channel.Channel.IsOpen.Returns(false);
                }
            }

            Connection
                .CreateChannelAsync(Arg.Any<CreateChannelOptions?>(), Arg.Any<CancellationToken>())
                .Returns<Task<IChannel>>(_ =>
                    throw new InvalidOperationException("the connection is closed")
                );
        }

        /// <summary>Raises the rename topology recovery performs on a server-named queue.</summary>
        public void RenameReplyQueue(string name) =>
            Connection.QueueNameChangedAfterRecoveryAsync += Raise.Event<
                AsyncEventHandler<QueueNameChangedAfterRecoveryEventArgs>
            >(Connection, new QueueNameChangedAfterRecoveryEventArgs(GeneratedReplyQueue, name));

        /// <summary>Restores the connection, standing in for a freshly created one.</summary>
        public void Reopen()
        {
            Connection.IsOpen.Returns(true);
            Connection.CloseReason.Returns(_ => null!);
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
                    Arg.Any<string>(),
                    Arg.Any<bool>(),
                    Arg.Any<bool>(),
                    Arg.Any<bool>(),
                    Arg.Any<IDictionary<string, object?>?>(),
                    Arg.Any<bool>(),
                    Arg.Any<bool>(),
                    Arg.Any<CancellationToken>()
                )
                .Returns(call =>
                {
                    var requested = call.ArgAt<string>(0);
                    var name = string.IsNullOrEmpty(requested)
                        ? FakeRabbit.GeneratedReplyQueue
                        : requested;
                    return Task.FromResult(new QueueDeclareOk(name, 0, 0));
                });

            Channel
                .BasicConsumeAsync(
                    Arg.Any<string>(),
                    Arg.Any<bool>(),
                    Arg.Any<string>(),
                    Arg.Any<bool>(),
                    Arg.Any<bool>(),
                    Arg.Any<IDictionary<string, object?>?>(),
                    Arg.Any<IAsyncBasicConsumer>(),
                    Arg.Any<CancellationToken>()
                )
                .Returns(call =>
                {
                    Consumer = call.ArgAt<IAsyncBasicConsumer>(6);
                    return Task.FromResult("consumer-tag");
                });

            Channel
                .When(c =>
                    c.BasicPublishAsync(
                        Arg.Any<string>(),
                        Arg.Any<string>(),
                        Arg.Any<bool>(),
                        Arg.Any<BasicProperties>(),
                        Arg.Any<ReadOnlyMemory<byte>>(),
                        Arg.Any<CancellationToken>()
                    )
                )
                .Do(call =>
                {
                    var properties = call.ArgAt<BasicProperties>(3);
                    var body = call.ArgAt<ReadOnlyMemory<byte>>(4);
                    lock (_gate)
                    {
                        _publishes.Add(
                            new Published(
                                call.ArgAt<string>(0),
                                call.ArgAt<string>(1),
                                properties.CorrelationId,
                                properties.ReplyTo,
                                body.ToArray()
                            )
                        );
                    }
                });

            Channel
                .When(c =>
                    c.ExchangeDeclareAsync(
                        Arg.Any<string>(),
                        Arg.Any<string>(),
                        Arg.Any<bool>(),
                        Arg.Any<bool>(),
                        Arg.Any<IDictionary<string, object?>?>(),
                        Arg.Any<bool>(),
                        Arg.Any<bool>(),
                        Arg.Any<CancellationToken>()
                    )
                )
                .Do(call =>
                    Exchanges.Add(
                        new DeclaredExchange(
                            call.ArgAt<string>(0),
                            call.ArgAt<string>(1),
                            call.ArgAt<bool>(2)
                        )
                    )
                );

            Channel
                .When(c =>
                    c.QueueBindAsync(
                        Arg.Any<string>(),
                        Arg.Any<string>(),
                        Arg.Any<string>(),
                        Arg.Any<IDictionary<string, object?>?>(),
                        Arg.Any<bool>(),
                        Arg.Any<CancellationToken>()
                    )
                )
                .Do(call =>
                    Bindings.Add(
                        new Binding(
                            call.ArgAt<string>(0),
                            call.ArgAt<string>(1),
                            call.ArgAt<string>(2)
                        )
                    )
                );

            Channel
                .When(c =>
                    c.BasicAckAsync(Arg.Any<ulong>(), Arg.Any<bool>(), Arg.Any<CancellationToken>())
                )
                .Do(call => Acks.Add(call.ArgAt<ulong>(0)));

            Channel
                .When(c =>
                    c.BasicRejectAsync(
                        Arg.Any<ulong>(),
                        Arg.Any<bool>(),
                        Arg.Any<CancellationToken>()
                    )
                )
                .Do(call => Rejects.Add(call.ArgAt<ulong>(0)));
        }

        public IChannel Channel { get; }

        public IAsyncBasicConsumer? Consumer { get; private set; }

        public List<ulong> Acks { get; } = [];

        public List<ulong> Rejects { get; } = [];

        public List<DeclaredExchange> Exchanges { get; } = [];

        public List<Binding> Bindings { get; } = [];

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

        public async Task DeliverAsync(
            string correlationId,
            string? replyTo,
            byte[] body,
            ulong deliveryTag = 1
        )
        {
            var consumer =
                Consumer
                ?? throw new InvalidOperationException("No consumer has been registered yet.");
            var properties = new BasicProperties
            {
                CorrelationId = correlationId,
                ReplyTo = replyTo,
            };
            await consumer.HandleBasicDeliverAsync(
                "consumer-tag",
                deliveryTag,
                redelivered: false,
                exchange: string.Empty,
                routingKey: "queue",
                properties,
                body,
                CancellationToken.None
            );
        }
    }
}
