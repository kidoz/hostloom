using System.Text;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Options;
using NSubstitute;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A request's <c>ReplyTo</c> is caller-controlled and becomes a default-exchange routing key.
/// By default only server-named reply queues are answered, which is what HostLoom's own client
/// declares; a cancelled or stopping delivery is handed back to the queue instead of dropped; and
/// a configured dead-letter exchange reaches every queue the broker declares.
/// </summary>
public sealed class RabbitMqReplyValidationTests
{
    [Theory]
    [InlineData("amq.gen-hostloom-reply", false, true)]
    [InlineData("amq.gen-hostloom-reply", true, true)]
    [InlineData("amq.rabbitmq.reply-to", false, true)]
    [InlineData("orders.replies", false, false)]
    [InlineData("orders.replies", true, true)]
    [InlineData("amq.gen", false, false)]
    [InlineData("", false, false)]
    [InlineData("", true, false)]
    [InlineData(null, false, false)]
    [InlineData("orders replies", true, false)]
    [InlineData("orders\nreplies", true, false)]
    public void Reply_queue_names_are_checked_against_the_policy(
        string? replyTo,
        bool allowNamed,
        bool expected
    ) => Assert.Equal(expected, RabbitMqRequestBroker.IsAcceptableReplyQueue(replyTo, allowNamed));

    [Fact]
    public void A_reply_queue_name_over_the_amqp_limit_is_refused_even_when_named_queues_are_allowed()
    {
        Assert.True(
            RabbitMqRequestBroker.IsAcceptableReplyQueue(
                new string('q', 255),
                allowNamedQueues: true
            )
        );
        Assert.False(
            RabbitMqRequestBroker.IsAcceptableReplyQueue(
                new string('q', 256),
                allowNamedQueues: true
            )
        );
    }

    [Fact]
    public async Task A_request_naming_a_declared_queue_is_rejected_before_its_handler_runs()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit, new RabbitMqOptions());
        var handled = 0;

        await using var listener = await broker.ListenAsync(
            "orders",
            (_, _) =>
            {
                handled++;
                return ValueTask.FromResult<ReadOnlyMemory<byte>>("handled"u8.ToArray());
            },
            TestContext.Current.CancellationToken
        );

        var channel = rabbit.Channels[0];
        await channel.DeliverAsync(
            "corr-1",
            replyTo: "somebody.elses.queue",
            deliveryTag: 3,
            TestContext.Current.CancellationToken
        );

        Assert.Equal(0, handled);
        Assert.Empty(channel.Publishes);
        Assert.Equal([(3ul, false)], channel.Rejects);
        Assert.Empty(channel.Nacks);
    }

    [Fact]
    public async Task Named_reply_queues_can_be_allowed_for_foreign_clients()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(
            rabbit,
            new RabbitMqOptions { AllowNamedReplyQueues = true }
        );

        await using var listener = await broker.ListenAsync(
            "orders",
            (_, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>("handled"u8.ToArray()),
            TestContext.Current.CancellationToken
        );

        var channel = rabbit.Channels[0];
        await channel.DeliverAsync(
            "corr-1",
            replyTo: "orders.replies",
            deliveryTag: 4,
            TestContext.Current.CancellationToken
        );

        var published = Assert.Single(channel.Publishes);
        Assert.Equal("orders.replies", published.RoutingKey);
        Assert.Equal([4ul], channel.Acks);
    }

    [Fact]
    public async Task A_delivery_cancelled_by_the_client_library_is_requeued_not_rejected()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit, new RabbitMqOptions());
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await using var listener = await broker.ListenAsync(
            "orders",
            async (_, token) =>
            {
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return "never"u8.ToArray();
            },
            TestContext.Current.CancellationToken
        );

        var channel = rabbit.Channels[0];
        await channel.DeliverAsync(
            "corr-1",
            replyTo: "amq.gen-reply",
            deliveryTag: 5,
            cancellationToken: cancelled.Token
        );

        Assert.Equal([(5ul, true)], channel.Nacks);
        Assert.Empty(channel.Rejects);
        Assert.Empty(channel.Acks);
    }

    [Fact]
    public async Task A_delivery_in_flight_when_the_listener_stops_is_requeued()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit, new RabbitMqOptions());
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);

        var listener = await broker.ListenAsync(
            "orders",
            async (_, token) =>
            {
                started.SetResult();
                await Task.Delay(Timeout.InfiniteTimeSpan, token);
                return "never"u8.ToArray();
            },
            TestContext.Current.CancellationToken
        );

        var channel = rabbit.Channels[0];
        var delivery = channel.DeliverAsync(
            "corr-1",
            replyTo: "amq.gen-reply",
            deliveryTag: 6,
            TestContext.Current.CancellationToken
        );
        await started.Task.WaitAsync(
            TimeSpan.FromSeconds(5),
            TestContext.Current.CancellationToken
        );

        await listener.DisposeAsync();
        await delivery.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken);

        Assert.Equal([(6ul, true)], channel.Nacks);
        Assert.Empty(channel.Rejects);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Disposal_closes_the_channel_once_even_when_a_handler_callback_throws(
        bool eventSubscription
    )
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit, new RabbitMqOptions());
        var started = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cancelled = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        async ValueTask Handle(CancellationToken token)
        {
            // Callbacks run in reverse order of registration, so this one signals after the
            // failing one below has run.
            using var signal = token.Register(() => cancelled.TrySetResult());
            using var registration = token.Register(() =>
                throw new InvalidOperationException("callback failed")
            );
            started.TrySetResult();
            await release.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            );
            token.ThrowIfCancellationRequested();
        }

        await using var subscription = eventSubscription
            ? await broker.SubscribeAsync(
                "orders",
                "audit",
                (_, token) => Handle(token),
                TestContext.Current.CancellationToken
            )
            : await broker.ListenAsync(
                "orders",
                async (_, token) =>
                {
                    await Handle(token);
                    return "reply"u8.ToArray();
                },
                TestContext.Current.CancellationToken
            );
        var channel = rabbit.Channels[0];
        var delivery = channel.DeliverAsync(
            "c1",
            "amq.gen-reply",
            8,
            TestContext.Current.CancellationToken
        );
        try
        {
            await started.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            );
            var disposing = subscription.DisposeAsync().AsTask();
            await cancelled.Task.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            );
            // Disposal waits for the handler in flight, which ignores its token until released.
            Assert.False(disposing.IsCompleted);
            release.TrySetResult();
            await Assert.ThrowsAsync<AggregateException>(() =>
                disposing.WaitAsync(TimeSpan.FromSeconds(5), TestContext.Current.CancellationToken)
            );
            // The channel closes in the background rather than on the disposing caller's path.
            await SchedulingTests.WaitUntilAsync(() => broker.ClosingChannelCount == 0);
            await channel.Channel.Received(1).DisposeAsync();
            await subscription.DisposeAsync();
            await channel.Channel.Received(1).DisposeAsync();
        }
        finally
        {
            release.TrySetResult();
            await delivery.WaitAsync(
                TimeSpan.FromSeconds(5),
                TestContext.Current.CancellationToken
            );
        }
    }

    [Fact]
    public async Task Successful_disposal_is_idempotent()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit, new RabbitMqOptions());
        await using var subscription = await broker.SubscribeAsync(
            "orders",
            "audit",
            (_, _) => ValueTask.CompletedTask,
            TestContext.Current.CancellationToken
        );
        await subscription.DisposeAsync();
        await subscription.DisposeAsync();
        await SchedulingTests.WaitUntilAsync(() => broker.ClosingChannelCount == 0);
        await rabbit.Channels[0].Channel.Received(1).DisposeAsync();
    }

    [Fact]
    public async Task A_subscription_delivery_cancelled_in_flight_is_requeued()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit, new RabbitMqOptions());
        using var cancelled = new CancellationTokenSource();
        await cancelled.CancelAsync();

        await using var subscription = await broker.SubscribeAsync(
            "orders",
            "audit",
            async (_, token) => await Task.Delay(Timeout.InfiniteTimeSpan, token),
            TestContext.Current.CancellationToken
        );

        var channel = rabbit.Channels[0];
        await channel.DeliverAsync(
            "c1",
            replyTo: null,
            deliveryTag: 7,
            cancellationToken: cancelled.Token
        );

        Assert.Equal([(7ul, true)], channel.Nacks);
        Assert.Empty(channel.Rejects);
    }

    [Fact]
    public async Task A_dead_letter_exchange_is_declared_on_request_and_subscription_queues()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(
            rabbit,
            new RabbitMqOptions { DeadLetterExchange = "hostloom.dead-letters" }
        );

        await using var listener = await broker.ListenAsync(
            "orders",
            (_, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>("handled"u8.ToArray()),
            TestContext.Current.CancellationToken
        );
        await using var subscription = await broker.SubscribeAsync(
            "orders",
            "audit",
            (_, _) => ValueTask.CompletedTask,
            TestContext.Current.CancellationToken
        );

        var declared = rabbit.Channels.SelectMany(channel => channel.Declarations).ToList();
        Assert.Equal(2, declared.Count);
        Assert.All(
            declared,
            declaration =>
                Assert.Equal(
                    "hostloom.dead-letters",
                    Assert.Contains("x-dead-letter-exchange", declaration.Arguments!)
                )
        );
    }

    [Fact]
    public async Task Without_a_dead_letter_exchange_queues_are_declared_without_arguments()
    {
        var rabbit = new FakeRabbit();
        await using var broker = Create(rabbit, new RabbitMqOptions());

        await using var listener = await broker.ListenAsync(
            "orders",
            (_, _) => ValueTask.FromResult<ReadOnlyMemory<byte>>("handled"u8.ToArray()),
            TestContext.Current.CancellationToken
        );

        var declaration = Assert.Single(rabbit.Channels[0].Declarations);
        Assert.Null(declaration.Arguments);
    }

    private static RabbitMqRequestBroker Create(FakeRabbit rabbit, RabbitMqOptions options) =>
        new(Options.Create(options), _ => ValueTask.FromResult(rabbit.Connection));

    private sealed record Published(string Exchange, string RoutingKey, string? CorrelationId);

    private sealed record Declaration(string Queue, IDictionary<string, object?>? Arguments);

    private sealed class FakeRabbit
    {
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
                    return [.. _channels];
                }
            }
        }
    }

    private sealed class FakeChannel
    {
        private readonly Lock _gate = new();

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
                    var name = string.IsNullOrEmpty(requested) ? "amq.gen-reply" : requested;
                    lock (_gate)
                    {
                        Declarations.Add(
                            new Declaration(name, call.ArgAt<IDictionary<string, object?>?>(4))
                        );
                    }

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
                    lock (_gate)
                    {
                        Publishes.Add(
                            new Published(
                                call.ArgAt<string>(0),
                                call.ArgAt<string>(1),
                                call.ArgAt<BasicProperties>(3).CorrelationId
                            )
                        );
                    }
                });

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
                .Do(call => Rejects.Add((call.ArgAt<ulong>(0), call.ArgAt<bool>(1))));

            Channel
                .When(c =>
                    c.BasicNackAsync(
                        Arg.Any<ulong>(),
                        Arg.Any<bool>(),
                        Arg.Any<bool>(),
                        Arg.Any<CancellationToken>()
                    )
                )
                .Do(call => Nacks.Add((call.ArgAt<ulong>(0), call.ArgAt<bool>(2))));
        }

        public IChannel Channel { get; }

        public IAsyncBasicConsumer? Consumer { get; private set; }

        public List<ulong> Acks { get; } = [];

        public List<(ulong Tag, bool Requeue)> Rejects { get; } = [];

        public List<(ulong Tag, bool Requeue)> Nacks { get; } = [];

        public List<Published> Publishes { get; } = [];

        public List<Declaration> Declarations { get; } = [];

        public async Task DeliverAsync(
            string correlationId,
            string? replyTo,
            ulong deliveryTag,
            CancellationToken cancellationToken = default
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
                Encoding.UTF8.GetBytes("ask"),
                cancellationToken
            );
        }
    }
}
