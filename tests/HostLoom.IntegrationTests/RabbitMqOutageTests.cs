using System.Diagnostics;
using System.Globalization;
using System.Text;
using System.Threading.Channels;
using HostLoom.IntegrationTests.Infrastructure;
using HostLoom.Transport.RabbitMq;
using Microsoft.Extensions.Options;
using RabbitMQ.Client.Exceptions;
using Xunit;

namespace HostLoom.IntegrationTests;

/// <summary>
/// Opt-in local network experiments for the RabbitMQ transport: HOSTLOOM_RABBITMQ_CHAOS=1, the
/// RabbitMQ from <c>docker-compose.yml</c>, and a private loopback proxy per test that only the
/// broker under test connects through, so the shared broker and its other clients are never
/// touched. They produce the transport meter outcomes only a real broker can: a publication
/// whose confirmation does not arrive in time, and a dropped connection the client library
/// recovers. Every queue and exchange a test declares is deleted afterwards, and a 60-second
/// deadline aborts an experiment and disposes its proxy.
/// </summary>
[Collection(nameof(RabbitMqOutageTests))]
[CollectionDefinition(nameof(RabbitMqOutageTests), DisableParallelization = true)]
public sealed class RabbitMqOutageTests
{
    private const string Skip =
        "Set HOSTLOOM_RABBITMQ_CHAOS=1 and start the local RabbitMQ fixture to run isolated network faults.";
    private const string ClientTag = "hostloom.rabbitmq.client";
    private const string OutcomeTag = "hostloom.rabbitmq.outcome";
    private const string EventTag = "hostloom.rabbitmq.event";
    private const string Publishes = "hostloom.rabbitmq.publishes";
    private const string PublishDuration = "hostloom.rabbitmq.publish.duration";
    private const string Connections = "hostloom.rabbitmq.connections";
    private const string PendingRequests = "hostloom.rabbitmq.requests.pending";
    private const string ClosingChannels = "hostloom.rabbitmq.channels.closing";
    private static readonly Uri Direct = new("amqp://guest:guest@localhost:5672/");
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(10);

    /// <summary>How long a timed-out publication may take beyond its PublishTimeout to return.</summary>
    private static readonly TimeSpan Slack = TimeSpan.FromSeconds(2);

    public static bool Enabled =>
        Environment.GetEnvironmentVariable("HOSTLOOM_RABBITMQ_CHAOS") == "1"
        && BrokerAvailability.RabbitMq;

    [Fact(Timeout = 60_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task HeldConfirmation_TimesOutThePublishAndTheNextIsConfirmedOnAFreshChannel()
    {
        var token = TestContext.Current.CancellationToken;
        var topic = "outage-orders-" + Guid.NewGuid().ToString("N");
        var client = "outage-publisher-" + Guid.NewGuid().ToString("N");
        var publishTimeout = TimeSpan.FromSeconds(1);
        // Declared first so it is disposed last, once both brokers have let go of the topology.
        await using var scope = new RabbitMqTopologyScope();
        scope.Subscription(topic, "audit");
        using var meters = new MeterCapture(RabbitMqDiagnostics.MeterName, ClientTag, client);
        await using var proxy = new TcpFaultProxy("localhost", 5672);
        await using var publisher = new RabbitMqRequestBroker(
            Options.Create(
                new RabbitMqOptions
                {
                    Uri = Through(proxy),
                    ClientProvidedName = client,
                    PublishTimeout = publishTimeout,
                }
            )
        );
        // The subscriber reaches the broker directly, so it shows what the broker accepted while
        // the publisher's replies are held.
        await using var auditor = new RabbitMqRequestBroker(
            Options.Create(
                new RabbitMqOptions { Uri = Direct, ClientProvidedName = client + "-audit" }
            )
        );
        var arrivals = Channel.CreateUnbounded<string>();
        await using var subscription = await auditor.SubscribeAsync(
            topic,
            "audit",
            (frame, _) =>
            {
                arrivals.Writer.TryWrite(Encoding.UTF8.GetString(frame.Span));
                return ValueTask.CompletedTask;
            },
            token
        );

        // A confirmed publication returns its confirming channel to the pool for the next one.
        await publisher.PublishAsync(topic, "order-1"u8.ToArray(), token);
        Assert.Equal(1d, meters.Sum(Publishes, (OutcomeTag, "confirmed")));
        Assert.Equal("order-1", await NextAsync(arrivals, token));

        TimeoutException timedOut;
        TimeSpan returnedAfter;
        proxy.HoldReplies();
        try
        {
            var started = Stopwatch.GetTimestamp();
            var stalled = publisher.PublishAsync(topic, "order-2"u8.ToArray(), token).AsTask();
            // The frame reaches the broker at once and is routed; only its confirmation is held,
            // so the broker accepted an event its publisher is about to report as timed out.
            Assert.Equal("order-2", await NextAsync(arrivals, token));
            // The deadline ends the wait for the confirmation after one second, and the call
            // returns then. The abandoned channel is closed in the background, where its close
            // waits for its own reply behind the same hold, up to the client library's 20-second
            // continuation timeout; the hold stays well inside the 60-second heartbeat, so the
            // connection survives it. A cancelled deadline, not WaitAsync's own
            // TimeoutException, bounds the call.
            using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
            deadline.CancelAfter(publishTimeout + Slack);
            timedOut = await Assert.ThrowsAsync<TimeoutException>(() =>
                stalled.WaitAsync(deadline.Token)
            );
            returnedAfter = Stopwatch.GetElapsedTime(started);
            // Its close-ok is held with every other reply, so the channel is still closing.
            Assert.Equal(1d, meters.Observe(ClosingChannels));
        }
        finally
        {
            proxy.ReleaseReplies();
        }

        Assert.IsAssignableFrom<OperationCanceledException>(timedOut.InnerException);
        Assert.Equal(1d, meters.Sum(Publishes, (OutcomeTag, "timed_out")));
        Assert.Equal(1, meters.Count(PublishDuration, (OutcomeTag, "timed_out")));
        var stalledFor = meters.Sum(PublishDuration, (OutcomeTag, "timed_out"));
        // The duration clock starts just after the deadline does, so allow for that gap.
        var took = string.Create(
            CultureInfo.InvariantCulture,
            $"The timed-out publication returned after {returnedAfter.TotalSeconds:F2} s; its recorded duration is {stalledFor:F2} s."
        );
        TestContext.Current.TestOutputHelper?.WriteLine(took);
        Assert.True(stalledFor >= publishTimeout.TotalSeconds * 0.9, took);
        Assert.True(returnedAfter <= publishTimeout + Slack, took);
        Assert.True(stalledFor <= (publishTimeout + Slack).TotalSeconds, took);

        // The released confirmation of order-2 lands on the channel it was published on. That
        // channel stopped tracking it when the wait was cancelled and was closed rather than
        // pooled, and its number stays taken until the close reply that follows the late
        // confirmation. So order-3 goes out on a new channel with its own sequence numbers and
        // can be confirmed only by its own reply, over the same connection.
        await publisher
            .PublishAsync(topic, "order-3"u8.ToArray(), token)
            .AsTask()
            .WaitAsync(Bound, token);
        Assert.Equal("order-3", await NextAsync(arrivals, token));
        Assert.False(arrivals.Reader.TryRead(out var duplicate), duplicate);
        Assert.Equal(2d, meters.Sum(Publishes, (OutcomeTag, "confirmed")));
        Assert.Equal(1d, meters.Sum(Publishes, (OutcomeTag, "timed_out")));
        Assert.Equal(3d, meters.Sum(Publishes));
        Assert.Equal(1d, meters.Sum(Connections, (EventTag, "opened")));
        Assert.Equal(0d, meters.Sum(Connections, (EventTag, "recovered")));
        // Released, the close-ok lets the abandoned channel finish closing in the background.
        await WaitForGaugeAsync(meters, ClosingChannels, 0d, token);
    }

    [Fact(Timeout = 60_000, Skip = Skip, SkipUnless = nameof(Enabled))]
    public async Task DroppedConnection_IsRecoveredAndRequestsRoundTripOverTheRenamedReplyQueue()
    {
        var token = TestContext.Current.CancellationToken;
        var address = "outage-invoices-" + Guid.NewGuid().ToString("N");
        var client = "outage-requester-" + Guid.NewGuid().ToString("N");
        await using var scope = new RabbitMqTopologyScope();
        scope.Request(address);
        using var meters = new MeterCapture(RabbitMqDiagnostics.MeterName, ClientTag, client);
        await using var proxy = new TcpFaultProxy("localhost", 5672);
        // Listener and client share one broker, so one connection through the proxy carries the
        // request queue's consumer, the publisher pool, and the server-named reply queue.
        await using var broker = new RabbitMqRequestBroker(
            Options.Create(
                new RabbitMqOptions { Uri = Through(proxy), ClientProvidedName = client }
            )
        );
        await using var listener = await broker.ListenAsync(
            address,
            (request, _) =>
                ValueTask.FromResult<ReadOnlyMemory<byte>>(
                    Encoding.UTF8.GetBytes("settled " + Encoding.UTF8.GetString(request.Span))
                ),
            token
        );

        Assert.Equal(
            "settled invoice-1",
            await RequestAsync(broker, address, "invoice-1", Bound, token)
        );

        proxy.SetEnabled(false);
        try
        {
            // The socket is closed and reconnects are refused, so the request fails on the
            // closed connection, as a transport failure around the client library's exception,
            // or at its own short deadline instead of hanging, and it leaves nothing pending.
            var lost = await Assert.ThrowsAnyAsync<Exception>(() =>
                RequestAsync(broker, address, "invoice-2", TimeSpan.FromSeconds(3), token)
            );
            TestContext.Current.TestOutputHelper?.WriteLine(
                $"The request during the outage failed with {lost.GetType().Name} ({lost.InnerException?.GetType().Name})."
            );
            Assert.True(
                lost
                    is MessagingTransportException { InnerException: OperationInterruptedException }
                        or RequestTimeoutException,
                lost.ToString()
            );
            Assert.Equal(0d, meters.Observe(PendingRequests));
            // The client library retries every NetworkRecoveryInterval, five seconds by default.
            // Keeping the outage until the proxy has refused one attempt makes the recovery below
            // a retry after a failed one, as in any outage longer than the interval.
            using var refusal = CancellationTokenSource.CreateLinkedTokenSource(token);
            refusal.CancelAfter(TimeSpan.FromSeconds(20));
            await proxy.Refused.WaitAsync(refusal.Token);
        }
        finally
        {
            proxy.SetEnabled(true);
        }

        // Recovery reports success only after re-declaring the queues and restoring the consumers.
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(30));
        await meters.WaitForAsync(Connections, 1, deadline.Token, (EventTag, "recovered"));
        Assert.Equal(1d, meters.Sum(Connections, (EventTag, "recovered")));
        // Recovered in place: the transport kept the connection rather than opening another,
        // which would have left the restored listener behind on the old one.
        Assert.Equal(1d, meters.Sum(Connections, (EventTag, "opened")));

        // The first reply queue was exclusive to the dropped connection, so the broker deleted
        // it; this reply can only come back through the queue recovery declared under a new
        // server-generated name, which the client must have followed.
        Assert.Equal(
            "settled invoice-3",
            await RequestAsync(broker, address, "invoice-3", Bound, token)
        );
        Assert.Equal(0d, meters.Observe(PendingRequests));
    }

    /// <summary>
    /// Polls an observable gauge, which raises no event to wait on, until it reads
    /// <paramref name="expected"/> or <see cref="Bound"/> passes.
    /// </summary>
    private static async Task WaitForGaugeAsync(
        MeterCapture meters,
        string instrument,
        double expected,
        CancellationToken token
    )
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(Bound);
        while (meters.Observe(instrument) != expected)
        {
            await Task.Delay(TimeSpan.FromMilliseconds(50), deadline.Token);
        }
    }

    private static Uri Through(TcpFaultProxy proxy) =>
        new($"amqp://guest:guest@{TcpFaultProxy.Host}:{proxy.Port}/");

    private static async Task<string> NextAsync(Channel<string> arrivals, CancellationToken token)
    {
        using var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(Bound);
        return await arrivals.Reader.ReadAsync(deadline.Token);
    }

    private static async Task<string> RequestAsync(
        RabbitMqRequestBroker broker,
        string address,
        string invoice,
        TimeSpan timeout,
        CancellationToken token
    )
    {
        var reply = await broker.RequestAsync(
            address,
            Encoding.UTF8.GetBytes(invoice),
            Guid.NewGuid(),
            timeout,
            token
        );
        return Encoding.UTF8.GetString(reply.Span);
    }
}
