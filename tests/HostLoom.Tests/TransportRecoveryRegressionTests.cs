using System.Net;
using HostLoom.Redis;
using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Diagnostics.HealthChecks;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NSubstitute;
using StackExchange.Redis;
using Xunit;

namespace HostLoom.Tests;

public sealed class TransportRecoveryRegressionTests
{
    private static readonly TimeSpan Bounded = TimeSpan.FromSeconds(10);

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Repeated_disposal_cannot_remove_a_successor(bool events)
    {
        await using var broker = new InMemoryRequestBroker();
        var calls = 0;
        var first = await Subscribe();
        await first.DisposeAsync();
        await using var successor = await Subscribe();
        await first.DisposeAsync();
        if (events)
            await broker.PublishAsync(
                "catalog",
                new byte[] { 1 },
                TestContext.Current.CancellationToken
            );
        else
            await broker.RequestAsync(
                "catalog",
                new byte[] { 1 },
                Guid.NewGuid(),
                TimeSpan.FromSeconds(2),
                TestContext.Current.CancellationToken
            );
        Assert.Equal(1, calls);

        ValueTask<IAsyncDisposable> Subscribe() =>
            events
                ? broker.SubscribeAsync(
                    "catalog",
                    "audit",
                    (_, _) =>
                    {
                        calls++;
                        return ValueTask.CompletedTask;
                    },
                    TestContext.Current.CancellationToken
                )
                : broker.ListenAsync(
                    "catalog",
                    (_, _) =>
                    {
                        calls++;
                        return ValueTask.FromResult<ReadOnlyMemory<byte>>(new byte[] { 2 });
                    },
                    TestContext.Current.CancellationToken
                );
    }

    [Fact]
    public async Task Caller_cancellation_does_not_cancel_accepted_handler_work()
    {
        await using var broker = new InMemoryRequestBroker();
        var entered = new TaskCompletionSource<CancellationToken>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var completed = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        await using var listener = await broker.ListenAsync(
            "catalog",
            async (_, token) =>
            {
                entered.SetResult(token);
                await release.Task.WaitAsync(token);
                completed.SetResult();
                return new byte[] { 2 };
            },
            TestContext.Current.CancellationToken
        );
        using var caller = new CancellationTokenSource();
        var pending = broker
            .RequestAsync(
                "catalog",
                new byte[] { 1 },
                Guid.NewGuid(),
                TimeSpan.FromSeconds(2),
                caller.Token
            )
            .AsTask();
        var receiverToken = await entered.Task.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );
        await caller.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.False(receiverToken.IsCancellationRequested);
        release.SetResult();
        await completed.Task.WaitAsync(
            TimeSpan.FromSeconds(2),
            TestContext.Current.CancellationToken
        );
    }

    [Fact(Timeout = 30_000)]
    public async Task A_listener_stopped_mid_request_leaves_the_requester_waiting_for_its_own_timeout()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        await using var broker = new InMemoryRequestBroker(logger: null, clock);
        var entered = new TaskCompletionSource<CancellationToken>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var stopped = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var listener = await broker.ListenAsync(
            "catalog",
            async (_, handlerToken) =>
            {
                entered.SetResult(handlerToken);
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, handlerToken);
                }
                finally
                {
                    stopped.SetResult();
                }

                return new byte[] { 2 };
            },
            token
        );
        var budget = TimeSpan.FromMinutes(5);
        var pending = broker
            .RequestAsync("catalog", new byte[] { 1 }, Guid.NewGuid(), budget, token)
            .AsTask();
        var handlerToken = await entered.Task.WaitAsync(TimeSpan.FromSeconds(10), token);

        await listener.DisposeAsync();
        await stopped.Task.WaitAsync(TimeSpan.FromSeconds(10), token);

        // The handler's token was the listener's to cancel; the requester is not handed that
        // cancellation but waits on its own clock, as it would for an unbound address.
        Assert.True(handlerToken.IsCancellationRequested);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(budget - TimeSpan.FromSeconds(1));
        Assert.False(pending.IsCompleted);
        clock.Advance(TimeSpan.FromSeconds(1));
        var timeout = await Assert.ThrowsAsync<RequestTimeoutException>(() =>
            pending.WaitAsync(TimeSpan.FromSeconds(10), token)
        );
        Assert.Equal("catalog", timeout.Address.Value);
        Assert.Equal(budget, timeout.Timeout);
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task Disposing_the_transport_ends_a_waiting_request_as_disposed(bool bound)
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var broker = new InMemoryRequestBroker(logger: null, clock);
        if (bound)
        {
            _ = await broker.ListenAsync(
                "catalog",
                async (_, handlerToken) =>
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, handlerToken);
                    return new byte[] { 2 };
                },
                token
            );
        }

        var pending = broker
            .RequestAsync("catalog", new byte[] { 1 }, Guid.NewGuid(), TimeSpan.FromHours(1), token)
            .AsTask();
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        await broker.DisposeAsync();

        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            pending.WaitAsync(TimeSpan.FromSeconds(10), token)
        );
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            broker
                .RequestAsync(
                    "catalog",
                    new byte[] { 1 },
                    Guid.NewGuid(),
                    TimeSpan.FromHours(1),
                    token
                )
                .AsTask()
        );
    }

    [Theory(Timeout = 30_000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task A_stopping_listener_and_the_transport_wait_for_the_handler_they_cancelled(
        bool events
    )
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        await using var broker = new InMemoryRequestBroker(logger: null, clock);
        var handler = new StubbornHandler();
        var listener = await handler.AttachAsync(broker, events, token);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(token);
        var pending = StubbornHandler.SendAsync(broker, events, caller.Token);
        var handlerToken = await handler.Entered.Task.WaitAsync(Bounded, token);

        // The caller walks away; the work it started is the listener's to finish or cancel.
        await caller.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 0);

        var stopping = listener.DisposeAsync().AsTask();
        await handler.Cancelled.Task.WaitAsync(Bounded, token);
        Assert.True(handlerToken.IsCancellationRequested);

        // The handler ignores its cancellation. The stop either returned under it, which would
        // let it run on after its container is gone, or waits for it on the transport's clock.
        await SchedulingTests.WaitUntilAsync(() =>
            stopping.IsCompleted || clock.PendingTimers == 1
        );
        Assert.False(stopping.IsCompleted);
        // Disposing the transport waits for the same handler, although its listener has already
        // left the transport's routing table.
        var disposing = broker.DisposeAsync().AsTask();
        Assert.False(disposing.IsCompleted);

        handler.Release.SetResult();
        await stopping.WaitAsync(Bounded, token);
        await disposing.WaitAsync(Bounded, token);
        Assert.True(handler.Returned.Task.IsCompleted);
        // The bounded wait releases its timer just after it completes the task the stop awaited,
        // so the release can land a moment after the stop returns; it must still land.
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 0);
        await listener.DisposeAsync().AsTask().WaitAsync(Bounded, token);
    }

    [Fact(Timeout = 30_000)]
    public async Task A_stopping_listener_gives_up_on_a_stubborn_handler_after_its_bound()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var logger = new RecordingLogger<InMemoryRequestBroker>();
        await using var broker = new InMemoryRequestBroker(logger, clock);
        var handler = new StubbornHandler();
        var listener = await handler.AttachAsync(broker, events: false, token);
        using var caller = CancellationTokenSource.CreateLinkedTokenSource(token);
        var pending = StubbornHandler.SendAsync(broker, events: false, caller.Token);
        await handler.Entered.Task.WaitAsync(Bounded, token);
        await caller.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 0);

        var stopping = listener.DisposeAsync().AsTask();
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        clock.Advance(InMemoryRequestBroker.HandlerDrainBound - TimeSpan.FromTicks(1));
        Assert.False(stopping.IsCompleted);
        clock.Advance(TimeSpan.FromTicks(1));

        // Cleanup is bounded: the stop returns with the handler still running, and says so.
        await stopping.WaitAsync(Bounded, token);
        Assert.False(handler.Returned.Task.IsCompleted);
        var warning = Assert.Single(logger.Entries, entry => entry.Event.Id == 1404);
        Assert.Equal(LogLevel.Warning, warning.Level);
        // The transport does not wait for it a second time.
        await broker.DisposeAsync().AsTask().WaitAsync(Bounded, token);
        Assert.Equal(0, clock.PendingTimers);

        handler.Release.SetResult();
        await handler.Returned.Task.WaitAsync(Bounded, token);
    }

    [Fact(Timeout = 30_000)]
    public async Task Stopping_the_host_returns_when_its_token_ends_though_a_listener_is_still_closing()
    {
        var token = TestContext.Current.CancellationToken;
        var builder = Host.CreateApplicationBuilder();
        builder
            .Services.AddHostLoom()
            .UseTransport<StallingBroker>()
            .AddHealthChecks()
            .AddHandler<Lookup, Found, LookupHandler>("catalog");
        using var host = builder.Build();
        await host.StartAsync(token);
        var broker = (StallingBroker)host.Services.GetRequiredService<IRequestBroker>();
        try
        {
            using var shutdown = CancellationTokenSource.CreateLinkedTokenSource(token);
            var stopping = host.StopAsync(shutdown.Token);
            await broker.Closing.Task.WaitAsync(Bounded, token);
            Assert.False(stopping.IsCompleted);

            // The host's shutdown timeout runs out while the listener is still closing.
            await shutdown.CancelAsync();

            await stopping.WaitAsync(Bounded, token);
            Assert.False(broker.Closed.Task.IsCompleted);
            var ready = await host
                .Services.GetRequiredService<HealthCheckService>()
                .CheckHealthAsync(check => check.Tags.Contains("ready"), token);
            Assert.Equal(HealthStatus.Unhealthy, ready.Status);
        }
        finally
        {
            broker.Release.TrySetResult();
        }

        // The listener finishes closing on its own; disposal does not close it a second time.
        await broker.Closed.Task.WaitAsync(Bounded, token);
        await ((IAsyncDisposable)host).DisposeAsync().AsTask().WaitAsync(Bounded, token);
        Assert.Equal(1, broker.Closes);
    }

    [Fact]
    public async Task Failed_subscriber_does_not_make_outbox_republish_to_healthy_subscribers()
    {
        await using var broker = new InMemoryRequestBroker();
        var healthy = 0;
        await using var broken = await broker.SubscribeAsync(
            "catalog",
            "broken",
            (_, _) => throw new InvalidOperationException("failure"),
            TestContext.Current.CancellationToken
        );
        await using var listener = await broker.SubscribeAsync(
            "catalog",
            "audit",
            (_, _) =>
            {
                healthy++;
                return ValueTask.CompletedTask;
            },
            TestContext.Current.CancellationToken
        );
        var store = new InMemoryOutboxStore();
        await store.AppendAsync(
            new OutboxMessage
            {
                MessageId = Guid.NewGuid(),
                Topic = "catalog",
                MessageType = "catalog",
                Frame = new byte[] { 1 },
                EnqueuedAt = DateTimeOffset.UtcNow,
            },
            TestContext.Current.CancellationToken
        );
        await using var relay = new OutboxRelay(store, broker, new OutboxOptions());
        Assert.Equal(1, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Equal(0, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, healthy);
    }

    [Theory]
    [InlineData(false, 0)]
    [InlineData(true, 1)]
    public async Task Cluster_rejects_incompatible_configuration_before_commands(
        bool hashTags,
        int database
    )
    {
        var mux = Substitute.For<IConnectionMultiplexer>();
        var endpoint = new IPEndPoint(IPAddress.Loopback, 6379);
        var server = Substitute.For<IServer>();
        server.ServerType.Returns(ServerType.Cluster);
        mux.GetEndPoints(Arg.Any<bool>()).Returns([endpoint]);
        mux.GetServer(endpoint, Arg.Any<object>()).Returns(server);
        await using var connection = new RedisConnection(
            mux,
            new RedisOptions { UseHashTags = hashTags, DatabaseIndex = database }
        );
        var exception = await Assert.ThrowsAsync<ArgumentException>(() =>
            connection.GetDatabaseAsync(TestContext.Current.CancellationToken).AsTask()
        );
        Assert.Contains("UseHashTags", exception.Message, StringComparison.Ordinal);
        mux.DidNotReceive().GetDatabase(Arg.Any<int>(), Arg.Any<object>());
    }

    public sealed record Lookup : IRequest<Found>;

    public sealed record Found;

    public sealed class LookupHandler : IRequestHandler<Lookup, Found>
    {
        public ValueTask<Found> HandleAsync(Lookup request, CancellationToken cancellationToken) =>
            ValueTask.FromResult(new Found());
    }

    /// <summary>
    /// A handler that notices its cancellation and carries on until released, as one blocked in
    /// a call that takes no token does.
    /// </summary>
    private sealed class StubbornHandler
    {
        public TaskCompletionSource<CancellationToken> Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Cancelled { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Returned { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        /// <summary>Listens on, or subscribes to, <c>catalog</c> with this handler.</summary>
        public ValueTask<IAsyncDisposable> AttachAsync(
            InMemoryRequestBroker broker,
            bool events,
            CancellationToken cancellationToken
        ) =>
            events
                ? broker.SubscribeAsync(
                    "catalog",
                    "audit",
                    async (_, token) => await RunAsync(token),
                    cancellationToken
                )
                : broker.ListenAsync(
                    "catalog",
                    async (_, token) =>
                    {
                        await RunAsync(token);
                        return new byte[] { 2 };
                    },
                    cancellationToken
                );

        /// <summary>Sends a request to, or publishes an event on, <c>catalog</c>.</summary>
        public static Task SendAsync(
            InMemoryRequestBroker broker,
            bool events,
            CancellationToken cancellationToken
        ) =>
            events
                ? broker.PublishAsync("catalog", new byte[] { 1 }, cancellationToken).AsTask()
                : broker
                    .RequestAsync(
                        "catalog",
                        new byte[] { 1 },
                        Guid.NewGuid(),
                        TimeSpan.FromHours(1),
                        cancellationToken
                    )
                    .AsTask();

        private async Task RunAsync(CancellationToken cancellationToken)
        {
            using var registration = cancellationToken.Register(() => Cancelled.TrySetResult());
            Entered.TrySetResult(cancellationToken);
            await Release.Task;
            Returned.TrySetResult();
        }
    }

    /// <summary>A transport whose listener, once asked to stop, does not finish until released.</summary>
    private sealed class StallingBroker : IRequestBroker
    {
        private int _closes;

        public TaskCompletionSource Closing { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Release { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public TaskCompletionSource Closed { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public int Closes => Volatile.Read(ref _closes);

        public ValueTask<IAsyncDisposable> ListenAsync(
            RequestAddress address,
            RequestFrameHandler handler,
            CancellationToken cancellationToken
        )
        {
            // CA2000: ownership of the listener transfers to the caller, which stops it.
#pragma warning disable CA2000
            return ValueTask.FromResult<IAsyncDisposable>(new Listener(this));
#pragma warning restore CA2000
        }

        public ValueTask<ReadOnlyMemory<byte>> RequestAsync(
            RequestAddress address,
            ReadOnlyMemory<byte> request,
            Guid requestId,
            TimeSpan timeout,
            CancellationToken cancellationToken
        ) => throw new NotSupportedException();

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;

        private sealed class Listener(StallingBroker owner) : IAsyncDisposable
        {
            public async ValueTask DisposeAsync()
            {
                Interlocked.Increment(ref owner._closes);
                owner.Closing.TrySetResult();
                await owner.Release.Task;
                owner.Closed.TrySetResult();
            }
        }
    }
}
