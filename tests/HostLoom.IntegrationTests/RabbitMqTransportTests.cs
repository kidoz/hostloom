using System.Collections.Concurrent;
using System.Diagnostics;
using System.Globalization;
using System.Text;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Options;
using RabbitMQ.Client;
using RabbitMQ.Client.Events;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Drives the RabbitMQ transport against a real broker from <c>docker-compose.yml</c>. The unit
/// suite covers correlation against fake channels; only these tests prove the adapter against
/// actual AMQP delivery, acknowledgement, and exchange topology. Every name a test mints is
/// recorded in a topology scope, so the durable queues and exchanges it declares are removed
/// from the shared broker after the host that used them is gone.
/// </summary>
[Collection(nameof(RabbitMqTransportTests))]
[CollectionDefinition(nameof(RabbitMqTransportTests), DisableParallelization = true)]
public sealed class RabbitMqTransportTests : IAsyncLifetime
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(30);

    private readonly RabbitMqTopologyScope _scope = new();

    public static bool Available => BrokerAvailability.RabbitMq;

    public ValueTask InitializeAsync() => ValueTask.CompletedTask;

    public ValueTask DisposeAsync() => _scope.DisposeAsync();

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task Request_and_response_round_trip_over_a_real_broker()
    {
        var address = Unique("greeter");
        using var host = await StartAsync(hostLoom =>
            hostLoom.AddHandler<Greet, Greeting, GreetHandler>(address)
        );

        var response = await ClientOf<Greet, Greeting>(host)
            .GetResponseAsync(address, new Greet("Ada"), cancellationToken: Token);

        Assert.Equal("Hello, Ada!", response.Text);
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task Concurrent_requests_each_receive_their_own_reply()
    {
        var address = Unique("concurrent");
        using var host = await StartAsync(hostLoom =>
            hostLoom.AddHandler<Greet, Greeting, GreetHandler>(address)
        );
        var client = ClientOf<Greet, Greeting>(host);

        // One exclusive reply queue carries every reply, so correlation is the only thing keeping
        // these apart. A correlation bug shows up here and nowhere in a serial round trip.
        var responses = await Task.WhenAll(
            Enumerable
                .Range(0, 25)
                .Select(i =>
                    client
                        .GetResponseAsync(address, new Greet($"n{i}"), cancellationToken: Token)
                        .AsTask()
                )
        );

        Assert.Equal(
            [.. Enumerable.Range(0, 25).Select(i => $"Hello, n{i}!").Order(StringComparer.Ordinal)],
            [.. responses.Select(r => r.Text).Order(StringComparer.Ordinal)]
        );
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task A_listener_handles_a_full_prefetch_window_of_requests_at_once()
    {
        var address = Unique("held");
        var gate = new Gate(16);
        using var host = await StartAsync(
            hostLoom => hostLoom.AddHandler<Hold, Held, HoldingHandler>(address),
            services => services.AddSingleton(gate)
        );
        var client = ClientOf<Hold, Held>(host);

        // The default dispatch concurrency equals the prefetch window, so every one of these
        // must be inside the handler before any is released; a serial dispatch would park the
        // first and never let the sixteenth arrive.
        var pending = Enumerable
            .Range(0, 16)
            .Select(i =>
                client.GetResponseAsync(address, new Hold(i), cancellationToken: Token).AsTask()
            )
            .ToArray();
        await gate.AllArrived.WaitAsync(Bound, Token);
        Assert.Equal(16, gate.Peak);
        Assert.All(pending, task => Assert.False(task.IsCompleted));

        gate.Release();
        var responses = await Task.WhenAll(pending);

        Assert.Equal(
            [.. Enumerable.Range(0, 16)],
            [.. responses.Select(response => response.Index).Order()]
        );
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task A_subscription_receives_events_in_publish_order_by_default()
    {
        var topic = Unique("sequence", "sequence");
        var received = new Received();
        received.Expect(16);
        using var host = await StartAsync(
            hostLoom => hostLoom.AddSubscriber<OrderPlaced, SequenceHandler>(topic, "sequence"),
            received: received
        );

        // Each publish awaits its confirmation, so the queue order is the loop order; with the
        // default event dispatch concurrency of 1 the handler must see exactly that order.
        for (var i = 0; i < 16; i++)
        {
            await PublisherOf(host).PublishAsync(topic, new OrderPlaced($"S-{i:D2}"), Token);
        }

        Assert.Equal(
            [.. Enumerable.Range(0, 16).Select(i => $"sequence:S-{i:D2}")],
            await received.WaitInOrderAsync(Bound)
        );
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task A_handler_fault_returns_as_a_remote_fault_without_a_stack_trace()
    {
        var address = Unique("failures");
        using var host = await StartAsync(hostLoom =>
            hostLoom.AddHandler<Fail, Never, FailingHandler>(address)
        );

        var exception = await Assert.ThrowsAsync<RemoteRequestException>(async () =>
            await ClientOf<Fail, Never>(host)
                .GetResponseAsync(address, new Fail("broker fault"), cancellationToken: Token)
        );

        Assert.Contains("broker fault", exception.Message, StringComparison.Ordinal);
        Assert.DoesNotContain(nameof(FailingHandler), exception.Message, StringComparison.Ordinal);
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task A_request_nobody_listens_to_times_out_instead_of_hanging()
    {
        using var host = await StartAsync(hostLoom => hostLoom.AddRequestClient<Greet, Greeting>());

        await Assert.ThrowsAsync<RequestTimeoutException>(async () =>
            await ClientOf<Greet, Greeting>(host)
                .GetResponseAsync(
                    Unique("nobody-home"),
                    new Greet("Ada"),
                    timeout: TimeSpan.FromSeconds(2),
                    cancellationToken: Token
                )
        );
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task Every_subscription_on_a_topic_receives_the_event()
    {
        var topic = Unique("orders", "audit", "shipping");
        var received = new Received();
        received.Expect(2);
        using var host = await StartAsync(
            hostLoom =>
                hostLoom
                    .AddSubscriber<OrderPlaced, AuditHandler>(topic, "audit")
                    .AddSubscriber<OrderPlaced, ShippingHandler>(topic, "shipping"),
            received: received
        );

        await PublisherOf(host).PublishAsync(topic, new OrderPlaced("A-1"), Token);

        // A fanout exchange with one durable queue per subscription: both must see the event.
        Assert.Equal(["audit:A-1", "shipping:A-1"], await received.WaitAsync(Bound));
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task Handlers_sharing_one_subscription_share_a_single_delivery()
    {
        var topic = Unique("orders-shared", "combined");
        var received = new Received();
        received.Expect(2);
        using var host = await StartAsync(
            hostLoom =>
                hostLoom
                    .AddSubscriber<OrderPlaced, AuditHandler>(topic, "combined")
                    .AddSubscriber<OrderPlaced, ShippingHandler>(topic, "combined"),
            received: received
        );

        await PublisherOf(host).PublishAsync(topic, new OrderPlaced("A-2"), Token);

        Assert.Equal(["audit:A-2", "shipping:A-2"], await received.WaitAsync(Bound));
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task An_event_nobody_subscribes_to_is_dropped_rather_than_failing_the_publish()
    {
        using var host = await StartAsync(hostLoom => hostLoom.AddRequestClient<Greet, Greeting>());

        // Published without `mandatory`, so an unroutable event must not fault the publisher.
        await PublisherOf(host).PublishAsync(Unique("unheard"), new OrderPlaced("A-3"), Token);
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task A_foreign_client_using_direct_reply_to_is_answered()
    {
        var address = Unique("direct-reply");
        await using var server = new RabbitMqRequestBroker(
            Options.Create(
                new RabbitMqOptions
                {
                    Uri = Broker,
                    ClientProvidedName = "it-direct-reply-" + Guid.NewGuid().ToString("N"),
                }
            )
        );
        await using var listener = await server.ListenAsync(
            address,
            (frame, _) =>
                ValueTask.FromResult<ReadOnlyMemory<byte>>(
                    Encoding.UTF8.GetBytes("answered " + Encoding.UTF8.GetString(frame.Span))
                ),
            Token
        );

        // A client of another stack: a raw connection consuming the direct reply-to
        // pseudo-queue, which replies can reach only through the channel consuming it.
        await using var connection = await new ConnectionFactory
        {
            Uri = Broker,
        }.CreateConnectionAsync(Token);
        await using var channel = await connection.CreateChannelAsync(cancellationToken: Token);
        var reply = new TaskCompletionSource<(string? CorrelationId, string Body)>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var consumer = new AsyncEventingBasicConsumer(channel);
        consumer.ReceivedAsync += (_, delivery) =>
        {
            reply.TrySetResult(
                (
                    delivery.BasicProperties.CorrelationId,
                    Encoding.UTF8.GetString(delivery.Body.Span)
                )
            );
            return Task.CompletedTask;
        };
        await channel.BasicConsumeAsync("amq.rabbitmq.reply-to", autoAck: true, consumer, Token);

        // What a listener finds in ReplyTo is not the pseudo-queue's name but the address the
        // broker rewrote it to, seen here through a server-named queue of the test's own.
        var spy = await channel.QueueDeclareAsync(cancellationToken: Token);
        await channel.BasicPublishAsync(
            string.Empty,
            spy.QueueName,
            mandatory: true,
            new BasicProperties { ReplyTo = "amq.rabbitmq.reply-to" },
            "spy"u8.ToArray(),
            Token
        );
        var rewritten = await GetOneAsync(channel, spy.QueueName);
        Assert.StartsWith(
            "amq.rabbitmq.reply-to.",
            rewritten.BasicProperties.ReplyTo,
            StringComparison.Ordinal
        );

        await channel.BasicPublishAsync(
            string.Empty,
            RabbitMqQueueNames.Request(address),
            mandatory: true,
            new BasicProperties { CorrelationId = "invoice-7", ReplyTo = "amq.rabbitmq.reply-to" },
            "invoice 7"u8.ToArray(),
            Token
        );

        var (correlationId, body) = await reply.Task.WaitAsync(TimeSpan.FromSeconds(10), Token);
        Assert.Equal("invoice-7", correlationId);
        Assert.Equal("answered invoice 7", body);
    }

    [Theory(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_consumer_whose_queue_is_deleted_is_restored_and_consumes_again(
        bool eventSubscription
    )
    {
        var name = eventSubscription
            ? Unique("restored-events", "audit")
            : Unique("restored-requests");
        var queue = eventSubscription
            ? RabbitMqQueueNames.Subscription(name, "audit")
            : RabbitMqQueueNames.Request(name);
        var role = eventSubscription ? "event" : "request";
        var client = "it-restore-" + Guid.NewGuid().ToString("N");
        using var meters = new MeterCapture(RabbitMqDiagnostics.MeterName, ClientTag, client);
        var log = new EventLog();
        await using var broker = new RabbitMqRequestBroker(
            Options.Create(new RabbitMqOptions { Uri = Broker, ClientProvidedName = client }),
            log
        );
        var arrivals = System.Threading.Channels.Channel.CreateUnbounded<string>();
        await using var consumer = eventSubscription
            ? await broker.SubscribeAsync(
                name,
                "audit",
                (frame, _) =>
                {
                    arrivals.Writer.TryWrite(Encoding.UTF8.GetString(frame.Span));
                    return ValueTask.CompletedTask;
                },
                Token
            )
            : await broker.ListenAsync(
                name,
                (frame, _) =>
                {
                    arrivals.Writer.TryWrite(Encoding.UTF8.GetString(frame.Span));
                    return ValueTask.FromResult(frame);
                },
                Token
            );

        await SendAsync("orders:before");
        Assert.Equal("orders:before", await NextAsync(arrivals));

        // Deleted from another connection, as an operator would: the broker cancels the
        // consumer, and the transport has to notice, declare the queue again, and resubscribe.
        await using (
            var admin = await new ConnectionFactory { Uri = Broker }.CreateConnectionAsync(Token)
        )
        {
            await using var channel = await admin.CreateChannelAsync(cancellationToken: Token);
            await channel.QueueDeleteAsync(queue, cancellationToken: Token);
        }

        using (var restoring = CancellationTokenSource.CreateLinkedTokenSource(Token))
        {
            restoring.CancelAfter(Bound);
            await meters.WaitForAsync(
                Consumers,
                1,
                restoring.Token,
                (EventTag, "restored"),
                (RoleTag, role)
            );
        }

        Assert.Equal(1d, meters.Sum(Consumers, (EventTag, "cancelled"), (RoleTag, role)));
        Assert.Contains(1411, log.EventIds);
        Assert.Contains(1412, log.EventIds);
        Assert.DoesNotContain(1413, log.EventIds);

        await SendAsync("orders:after");
        Assert.Equal("orders:after", await NextAsync(arrivals));

        async Task SendAsync(string text)
        {
            var frame = Encoding.UTF8.GetBytes(text);
            if (eventSubscription)
            {
                await broker.PublishAsync(name, frame, Token);
            }
            else
            {
                var answer = await broker.RequestAsync(
                    name,
                    frame,
                    Guid.NewGuid(),
                    TimeSpan.FromSeconds(10),
                    Token
                );
                Assert.Equal(text, Encoding.UTF8.GetString(answer.Span));
            }
        }
    }

    [Theory(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_handler_that_finishes_after_its_consumer_began_stopping_is_settled_not_rejected(
        bool eventSubscription
    )
    {
        var name = eventSubscription ? Unique("drain-events", "audit") : Unique("drain-requests");
        var queue = eventSubscription
            ? RabbitMqQueueNames.Subscription(name, "audit")
            : RabbitMqQueueNames.Request(name);
        var client = "it-drain-" + Guid.NewGuid().ToString("N");
        using var meters = new MeterCapture(RabbitMqDiagnostics.MeterName, ClientTag, client);
        var log = new EventLog();
        await using var server = new RabbitMqRequestBroker(
            Options.Create(new RabbitMqOptions { Uri = Broker, ClientProvidedName = client }),
            log
        );
        await using var caller = new RabbitMqRequestBroker(
            Options.Create(
                new RabbitMqOptions { Uri = Broker, ClientProvidedName = client + "-caller" }
            )
        );
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var consumer = eventSubscription
            ? await server.SubscribeAsync(
                name,
                "audit",
                async (_, handlerToken) =>
                {
                    entered.TrySetResult();
                    await FinishAfterTheStopAsync(handlerToken);
                },
                Token
            )
            : await server.ListenAsync(
                name,
                async (frame, handlerToken) =>
                {
                    entered.TrySetResult();
                    await FinishAfterTheStopAsync(handlerToken);
                    return frame;
                },
                Token
            );

        Task<ReadOnlyMemory<byte>>? reply = null;
        if (eventSubscription)
        {
            await caller.PublishAsync(name, "orders:late"u8.ToArray(), Token);
        }
        else
        {
            reply = caller
                .RequestAsync(
                    name,
                    "orders:late"u8.ToArray(),
                    Guid.NewGuid(),
                    TimeSpan.FromSeconds(20),
                    Token
                )
                .AsTask();
        }

        await entered.Task.WaitAsync(Bound, Token);
        await consumer.DisposeAsync().AsTask().WaitAsync(Bound, Token);

        // The channel stayed open until the handler returned, so its work was answered and
        // acknowledged, not rejected and not handed back to run again.
        if (reply is not null)
        {
            Assert.Equal(
                "orders:late",
                Encoding.UTF8.GetString((await reply.WaitAsync(Bound, Token)).Span)
            );
        }

        Assert.DoesNotContain(1401, log.EventIds);
        Assert.DoesNotContain(1405, log.EventIds);
        Assert.Equal(0d, meters.Sum(Rejected));
        Assert.Equal(0d, meters.Sum(Requeued));
        Assert.Equal(0u, await ReadyMessagesAsync(queue));

        // Ignores the cancellation the stop sends and finishes its work shortly after it.
        static async Task FinishAfterTheStopAsync(CancellationToken handlerToken)
        {
            var stopping = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            using (handlerToken.Register(() => stopping.TrySetResult()))
            {
                await stopping.Task.WaitAsync(Bound, Token);
            }

            await Task.Delay(TimeSpan.FromMilliseconds(300), Token);
        }
    }

    [Fact(Skip = BrokerAvailability.RabbitMqSkip, SkipUnless = nameof(Available))]
    public async Task A_handler_that_outlives_the_drain_bound_is_left_behind_and_its_event_redelivered()
    {
        var topic = Unique("drain-abandoned", "audit");
        var client = "it-abandoned-" + Guid.NewGuid().ToString("N");
        using var meters = new MeterCapture(RabbitMqDiagnostics.MeterName, ClientTag, client);
        var log = new EventLog();
        await using var server = new RabbitMqRequestBroker(
            Options.Create(new RabbitMqOptions { Uri = Broker, ClientProvidedName = client }),
            log
        );
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var subscription = await server.SubscribeAsync(
            topic,
            "audit",
            async (_, _) =>
            {
                entered.TrySetResult();
                // Ignores its cancellation altogether, for longer than a stop waits.
                await release.Task.WaitAsync(Bound, Token);
            },
            Token
        );
        await server.PublishAsync(topic, "shipments:late"u8.ToArray(), Token);
        await entered.Task.WaitAsync(Bound, Token);

        var started = Stopwatch.GetTimestamp();
        await subscription.DisposeAsync().AsTask().WaitAsync(TimeSpan.FromSeconds(15), Token);
        var stoppedAfter = Stopwatch.GetElapsedTime(started);
        TestContext.Current.TestOutputHelper?.WriteLine(
            string.Create(
                CultureInfo.InvariantCulture,
                $"The subscription stopped after {stoppedAfter.TotalSeconds:F2} s."
            )
        );
        // The documented five-second drain bound, and no longer than a few seconds beyond it.
        Assert.InRange(stoppedAfter, TimeSpan.FromSeconds(4.5), TimeSpan.FromSeconds(8));
        Assert.Contains(1406, log.EventIds);

        // Its channel closed without the handler, so the broker handed the event back, and the
        // next consumer of the subscription receives it.
        await using var successor = new RabbitMqRequestBroker(
            Options.Create(
                new RabbitMqOptions { Uri = Broker, ClientProvidedName = client + "-successor" }
            )
        );
        var redelivered = new TaskCompletionSource<string>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        await using var again = await successor.SubscribeAsync(
            topic,
            "audit",
            (frame, _) =>
            {
                redelivered.TrySetResult(Encoding.UTF8.GetString(frame.Span));
                return ValueTask.CompletedTask;
            },
            Token
        );
        Assert.Equal("shipments:late", await redelivered.Task.WaitAsync(Bound, Token));

        // Finishing late, the abandoned handler cannot acknowledge on the closed channel. That is
        // reported as an unsettled delivery at warning level, not as a rejection.
        release.SetResult();
        using (var reported = CancellationTokenSource.CreateLinkedTokenSource(Token))
        {
            reported.CancelAfter(Bound);
            while (!log.EventIds.Contains(1405))
            {
                await Task.Delay(TimeSpan.FromMilliseconds(20), reported.Token);
            }
        }

        Assert.Equal(LogLevel.Warning, log.LevelOf(1405));
        Assert.DoesNotContain(1401, log.EventIds);
        Assert.Equal(0d, meters.Sum(Rejected));
        Assert.Equal(1d, meters.Sum(Requeued));
    }

    private const string ClientTag = "hostloom.rabbitmq.client";
    private const string EventTag = "hostloom.rabbitmq.event";
    private const string RoleTag = "hostloom.rabbitmq.role";
    private const string Consumers = "hostloom.rabbitmq.consumers";
    private const string Rejected = "hostloom.rabbitmq.deliveries.rejected";
    private const string Requeued = "hostloom.rabbitmq.deliveries.requeued";

    private static readonly Uri Broker = new("amqp://guest:guest@localhost:5672/");

    private static CancellationToken Token => TestContext.Current.CancellationToken;

    private static async Task<string> NextAsync(System.Threading.Channels.Channel<string> arrivals)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(Token);
        deadline.CancelAfter(Bound);
        return await arrivals.Reader.ReadAsync(deadline.Token);
    }

    /// <summary>Takes the next message from a queue, polling within the bound until one is there.</summary>
    private static async Task<BasicGetResult> GetOneAsync(IChannel channel, string queue)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(Token);
        deadline.CancelAfter(Bound);
        while (true)
        {
            if (await channel.BasicGetAsync(queue, autoAck: true, deadline.Token) is { } result)
            {
                return result;
            }

            await Task.Delay(TimeSpan.FromMilliseconds(20), deadline.Token);
        }
    }

    /// <summary>Counts the messages ready in a queue, through a connection of the test's own.</summary>
    private static async Task<uint> ReadyMessagesAsync(string queue)
    {
        await using var admin = await new ConnectionFactory { Uri = Broker }.CreateConnectionAsync(
            Token
        );
        await using var channel = await admin.CreateChannelAsync(cancellationToken: Token);
        return (await channel.QueueDeclarePassiveAsync(queue, Token)).MessageCount;
    }

    /// <summary>Records the event ids the transport logs, and the level of each.</summary>
    private sealed class EventLog : ILogger<RabbitMqRequestBroker>
    {
        private readonly ConcurrentQueue<(int Id, LogLevel Level)> _events = new();

        public IReadOnlyList<int> EventIds => [.. _events.Select(entry => entry.Id)];

        public LogLevel LevelOf(int eventId) => _events.First(entry => entry.Id == eventId).Level;

        public IDisposable? BeginScope<TState>(TState state)
            where TState : notnull => null;

        public bool IsEnabled(LogLevel logLevel) => true;

        public void Log<TState>(
            LogLevel logLevel,
            EventId eventId,
            TState state,
            Exception? exception,
            Func<TState, Exception?, string> formatter
        ) => _events.Enqueue((eventId.Id, logLevel));
    }

    /// <summary>
    /// Mints a name no other run can collide with and records what it may become on the broker:
    /// a request queue, a topic exchange, and one subscription queue per name given. Nothing here
    /// knows which of those a test will declare, and deleting a name that never existed is free.
    /// </summary>
    private string Unique(string prefix, params string[] subscriptions)
    {
        var name = $"it-{prefix}-{Guid.NewGuid():N}";
        _scope.Request(name);
        _scope.Topic(name);
        foreach (var subscription in subscriptions)
        {
            _scope.Subscription(name, subscription);
        }

        return name;
    }

    private static IRequestClient<TRequest, TResponse> ClientOf<TRequest, TResponse>(IHost host)
        where TRequest : class, IRequest<TResponse>
        where TResponse : class =>
        host.Services.GetRequiredService<IRequestClient<TRequest, TResponse>>();

    private static IPublishEndpoint PublisherOf(IHost host) =>
        host.Services.GetRequiredService<IPublishEndpoint>();

    private static async Task<IHost> StartAsync(
        Action<HostLoomBuilder> configure,
        Action<IServiceCollection>? services = null,
        Received? received = null
    )
    {
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received ?? new Received());
        services?.Invoke(builder.Services);
        configure(
            builder
                .Services.AddHostLoom(options => options.RequestTimeout = TimeSpan.FromSeconds(20))
                .UseRabbitMq(options => options.Uri = new Uri("amqp://guest:guest@localhost:5672/"))
        );

        var host = builder.Build();
        await host.StartAsync(Token);
        return host;
    }
}
