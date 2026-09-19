using HostLoom.Transport.InMemory;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Options;
using Xunit;
using static HostLoom.Tests.PublishSubscribeTests;

namespace HostLoom.Tests;

public sealed class OutboxTests
{
    [Fact]
    public async Task The_in_memory_store_claims_in_enqueue_order_and_honours_leases()
    {
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var first = Message("orders", clock);
        var second = Message("orders", clock);
        await store.AppendAsync(first, TestContext.Current.CancellationToken);
        await store.AppendAsync(second, TestContext.Current.CancellationToken);

        var claimed = await store.ClaimAsync(
            10,
            TimeSpan.FromMinutes(1),
            TestContext.Current.CancellationToken
        );
        Assert.Equal([first.MessageId, second.MessageId], claimed.Select(m => m.MessageId));
        Assert.Empty(
            await store.ClaimAsync(
                10,
                TimeSpan.FromMinutes(1),
                TestContext.Current.CancellationToken
            )
        );

        clock.Advance(TimeSpan.FromMinutes(1));
        var again = await store.ClaimAsync(
            1,
            TimeSpan.FromMinutes(1),
            TestContext.Current.CancellationToken
        );
        Assert.Equal([first.MessageId], again.Select(m => m.MessageId));

        await store.MarkPublishedAsync(first.MessageId, TestContext.Current.CancellationToken);
        await store.MarkFailedAsync(
            second.MessageId,
            "broker down",
            TestContext.Current.CancellationToken
        );
        Assert.Equal([first.MessageId], store.Published.Select(m => m.MessageId));
        var pending = Assert.Single(store.Pending);
        Assert.Equal(second.MessageId, pending.MessageId);
        Assert.Equal(1, pending.Attempts);
        Assert.Equal("broker down", store.LastError);
        // The failure released the lease, so the message is claimable at once.
        Assert.Single(
            await store.ClaimAsync(
                10,
                TimeSpan.FromMinutes(1),
                TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await store.AppendAsync(second, TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task Drain_publishes_frames_unchanged_and_stops_after_a_failure()
    {
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new RecordingEventBroker();
        for (var i = 0; i < 5; i++)
        {
            await store.AppendAsync(
                Message("orders", clock, payload: (byte)i),
                TestContext.Current.CancellationToken
            );
        }

        await using var relay = new OutboxRelay(
            store,
            broker,
            new OutboxOptions { BatchSize = 2 },
            clock
        );
        clock.Advance(TimeSpan.FromSeconds(3));
        var published = await relay.DrainAsync(TestContext.Current.CancellationToken);

        Assert.Equal(5, published);
        Assert.Equal(5, broker.Published.Count);
        Assert.Equal([0, 1, 2, 3, 4], broker.Published.Select(p => p.Frame.Span[0]));
        Assert.All(broker.Published, p => Assert.Equal("orders", p.Topic.Value));
        Assert.Empty(store.Pending);
        Assert.Equal(5, relay.Published);

        broker.FailNext = new InvalidOperationException("broker down");
        await store.AppendAsync(Message("orders", clock), TestContext.Current.CancellationToken);
        await store.AppendAsync(Message("orders", clock), TestContext.Current.CancellationToken);
        await store.AppendAsync(Message("orders", clock), TestContext.Current.CancellationToken);

        // The failed message stays pending with its error, the rest of its batch still goes out,
        // and the drain stops after that batch so the third message waits for the next one.
        Assert.Equal(1, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Equal(2, store.Pending.Count);
        Assert.Equal(1, store.Pending[0].Attempts);
        Assert.Equal("broker down", store.LastError);
        Assert.Equal(1, relay.Failed);
        Assert.Equal(2, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Empty(store.Pending);
    }

    [Fact]
    public async Task The_loop_drains_when_woken_and_at_every_poll_interval()
    {
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new RecordingEventBroker();
        await using var relay = new OutboxRelay(
            store,
            broker,
            new OutboxOptions { PollInterval = TimeSpan.FromSeconds(5) },
            clock
        );
        await relay.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);

        await store.AppendAsync(Message("orders", clock), TestContext.Current.CancellationToken);
        relay.Wake();
        await broker.WaitForAsync(1);
        Assert.Empty(store.Pending);

        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
        await store.AppendAsync(Message("orders", clock), TestContext.Current.CancellationToken);
        clock.Advance(TimeSpan.FromSeconds(5));
        await broker.WaitForAsync(2);

        await relay.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(0, clock.PendingTimers);
        await relay.DisposeAsync();
        await Assert.ThrowsAsync<ObjectDisposedException>(() =>
            relay.StartAsync(TestContext.Current.CancellationToken)
        );
    }

    [Fact]
    public async Task A_published_event_reaches_its_subscribers_through_the_outbox()
    {
        var received = new Received();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .UseInMemoryOutbox()
            .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit")
            .AddSubscriber<OrderPlaced, ShippingHandler>("orders", "shipping");
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);

        await host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", new OrderPlaced("A-1"), TestContext.Current.CancellationToken);

        // Publishing returned when the append did; the relay delivers shortly after.
        await SchedulingTests.WaitUntilAsync(() => received.Sorted().Count == 2);
        Assert.Equal(["audit:A-1", "shipping:A-1"], received.Sorted());
        var store = host.Services.GetRequiredService<InMemoryOutboxStore>();
        Assert.Empty(store.Pending);
        var published = Assert.Single(store.Published);
        Assert.Equal("orders", published.Topic);
        Assert.EndsWith(nameof(OrderPlaced), published.MessageType, StringComparison.Ordinal);
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task A_scoped_store_appends_from_the_publisher_scope_and_relays_from_its_own()
    {
        var builder = Host.CreateApplicationBuilder();
        var shared = new InMemoryOutboxStore();
        builder.Services.AddSingleton(shared);
        builder.Services.AddSingleton<ScopeLog>();
        builder.Services.AddSingleton(new Received());
        builder
            .Services.AddHostLoom()
            .UseInMemory()
            .UseOutbox<ScopedStore>()
            .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit");
        using var host = builder.Build();
        await host.StartAsync(TestContext.Current.CancellationToken);
        var log = host.Services.GetRequiredService<ScopeLog>();

        object? appendStore;
        await using (var scope = host.Services.CreateAsyncScope())
        {
            var publisher = scope.ServiceProvider.GetRequiredService<IPublishEndpoint>();
            var store = scope.ServiceProvider.GetRequiredService<IOutboxStore>();
            await publisher.PublishAsync(
                "orders",
                new OrderPlaced("A-2"),
                TestContext.Current.CancellationToken
            );

            var append = Assert.Single(log.Entries, e => e.Operation == "append");
            Assert.Same(store, append.Store);
            appendStore = append.Store;
        }

        await SchedulingTests.WaitUntilAsync(() => shared.Published.Count == 1);
        var claims = log.Entries.Where(e => e.Operation == "claim").ToList();
        Assert.NotEmpty(claims);
        Assert.All(claims, claim => Assert.NotSame(appendStore, claim.Store));
        await host.StopAsync(TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task The_outbox_needs_a_transport_that_publishes_and_valid_options()
    {
        var requestOnly = Host.CreateApplicationBuilder();
        requestOnly.Services.AddHostLoom().UseTransport<RequestOnlyBroker>().UseInMemoryOutbox();
        using var requestOnlyHost = requestOnly.Build();
        var unsupported = await Assert.ThrowsAsync<NotSupportedException>(async () =>
            await requestOnlyHost.StartAsync(TestContext.Current.CancellationToken)
        );
        Assert.Contains(nameof(IEventBroker), unsupported.Message, StringComparison.Ordinal);

        var invalid = Host.CreateApplicationBuilder();
        invalid.Services.AddHostLoom().UseInMemory().UseInMemoryOutbox(o => o.BatchSize = 0);
        using var invalidHost = invalid.Build();
        var failure = Assert.Throws<OptionsValidationException>(() =>
            invalidHost.Services.GetRequiredService<IOptions<OutboxOptions>>().Value
        );
        Assert.Contains("Outbox:BatchSize", failure.Message, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() =>
            new OutboxRelay(
                new InMemoryOutboxStore(),
                new RecordingEventBroker(),
                new OutboxOptions { PollInterval = TimeSpan.Zero }
            )
        );
    }

    private static OutboxMessage Message(string topic, TimeProvider clock, byte payload = 0) =>
        new()
        {
            MessageId = Guid.NewGuid(),
            Topic = topic,
            MessageType = "tests:Order",
            Frame = new byte[] { payload },
            EnqueuedAt = clock.GetUtcNow(),
        };

    public sealed class ScopeLog
    {
        private readonly Lock _gate = new();
        private readonly List<(string Operation, object? Store)> _entries = [];

        public IReadOnlyList<(string Operation, object? Store)> Entries
        {
            get
            {
                lock (_gate)
                {
                    return [.. _entries];
                }
            }
        }

        public void Record(string operation, object store)
        {
            lock (_gate)
            {
                _entries.Add((operation, store));
            }
        }
    }

    /// <summary>A scoped store over one shared in-memory store, recording which instance handled each call.</summary>
    public sealed class ScopedStore(InMemoryOutboxStore inner, ScopeLog log) : IOutboxStore
    {
        public ValueTask AppendAsync(
            OutboxMessage message,
            CancellationToken cancellationToken = default
        )
        {
            log.Record("append", this);
            return inner.AppendAsync(message, cancellationToken);
        }

        public ValueTask<IReadOnlyList<OutboxMessage>> ClaimAsync(
            int batchSize,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        )
        {
            log.Record("claim", this);
            return inner.ClaimAsync(batchSize, lease, cancellationToken);
        }

        public ValueTask MarkPublishedAsync(
            Guid messageId,
            CancellationToken cancellationToken = default
        ) => inner.MarkPublishedAsync(messageId, cancellationToken);

        public ValueTask MarkFailedAsync(
            Guid messageId,
            string error,
            CancellationToken cancellationToken = default
        ) => inner.MarkFailedAsync(messageId, error, cancellationToken);
    }

    internal sealed class RecordingEventBroker : IEventBroker
    {
        private readonly Lock _gate = new();
        private readonly List<(RequestAddress Topic, ReadOnlyMemory<byte> Frame)> _published = [];
        private readonly SemaphoreSlim _signal = new(0);

        public Exception? FailNext { get; set; }

        public IReadOnlyList<(RequestAddress Topic, ReadOnlyMemory<byte> Frame)> Published
        {
            get
            {
                lock (_gate)
                {
                    return [.. _published];
                }
            }
        }

        public ValueTask<IAsyncDisposable> SubscribeAsync(
            RequestAddress topic,
            string subscription,
            EventFrameHandler handler,
            CancellationToken cancellationToken
        ) => throw new NotSupportedException();

        public ValueTask PublishAsync(
            RequestAddress topic,
            ReadOnlyMemory<byte> frame,
            CancellationToken cancellationToken
        )
        {
            if (FailNext is { } failure)
            {
                FailNext = null;
                throw failure;
            }

            lock (_gate)
            {
                _published.Add((topic, frame));
            }

            _signal.Release();
            return ValueTask.CompletedTask;
        }

        public async Task WaitForAsync(int count)
        {
            using var timeout = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            while (Published.Count < count)
            {
                await _signal.WaitAsync(timeout.Token);
            }
        }
    }
}
