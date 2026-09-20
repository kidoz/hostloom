using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using static HostLoom.Tests.PublishSubscribeTests;

namespace HostLoom.Tests;

/// <summary>
/// Controlled faults across the store-and-forward path: two relays draining one store at once, a
/// transport that refuses every publish and then recovers, a store that cannot record a failure,
/// a relay killed with a message claimed, and a redelivery storm on the receiving side. The
/// hypothesis is the one the outbox exists for: nothing is lost while the transport is down,
/// nothing is published twice inside a claim, a relay that dies leaves its work to the next one,
/// and the inbox turns the at-least-once delivery that follows into one run per subscription.
/// </summary>
public sealed class OutboxInboxChaosTests
{
    private static readonly TimeSpan Bounded = TimeSpan.FromSeconds(30);

    [Fact]
    public async Task Two_relays_draining_one_store_publish_every_message_exactly_once()
    {
        const int messages = 40;
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new OutageBroker();
        var options = new OutboxOptions { BatchSize = 10, ClaimLease = TimeSpan.FromMinutes(1) };
        await using var first = new OutboxRelay(store, broker, options, clock);
        await using var second = new OutboxRelay(store, broker, options, clock);
        for (var index = 0; index < messages; index++)
        {
            await store.AppendAsync(Message("orders", clock, (byte)index), token);
        }

        var drains = await Task.WhenAll(
                Task.Run(async () => await first.DrainAsync(token), token),
                Task.Run(async () => await second.DrainAsync(token), token)
            )
            .WaitAsync(Bounded, token);

        // The claim is the exclusion: every message left the store once, through one relay or
        // the other, and the two relays together account for all of them.
        Assert.Equal(messages, drains.Sum());
        Assert.Equal(messages, broker.Frames.Count);
        Assert.Equal(messages, broker.Frames.Select(frame => frame.Span[0]).Distinct().Count());
        Assert.Empty(store.Pending);
        Assert.Equal(messages, first.Published + second.Published);
        Assert.Equal(0, first.Failed + second.Failed);
    }

    [Fact]
    public async Task A_transport_outage_holds_every_message_back_and_publishes_them_when_it_heals()
    {
        const int messages = 5;
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new OutageBroker();
        var options = new OutboxOptions
        {
            BatchSize = 10,
            MaxAttempts = 5,
            RetryDelay = TimeSpan.FromSeconds(2),
            MaxRetryDelay = TimeSpan.FromSeconds(8),
        };
        await using var relay = new OutboxRelay(store, broker, options, clock);
        for (var index = 0; index < messages; index++)
        {
            await store.AppendAsync(Message("orders", clock, (byte)index), token);
        }

        broker.Unreachable = true;
        Assert.Equal(0, await relay.DrainAsync(token));

        // Held back, not lost and not given up on: every message carries one attempt and a
        // retry time, and none of them reached the dead-letter state.
        Assert.Equal(messages, store.Pending.Count);
        Assert.All(store.Pending, message => Assert.Equal(1, message.Attempts));
        Assert.All(store.Pending, message => Assert.NotNull(message.NextAttemptAt));
        Assert.Empty(store.DeadLettered);
        Assert.Equal(messages, relay.Failed);

        // Nothing is due yet, so a relay that drains during the outage does not spin on them.
        Assert.Equal(0, await relay.DrainAsync(token));
        Assert.Equal(messages, relay.Failed);

        // Recovery: the transport answers, the backoff elapses, and the whole batch goes out in
        // the order it was enqueued.
        broker.Unreachable = false;
        clock.Advance(options.RetryDelay);
        Assert.Equal(messages, await relay.DrainAsync(token));
        Assert.Equal(
            Enumerable.Range(0, messages).Select(index => (byte)index),
            broker.Frames.Select(frame => frame.Span[0])
        );
        Assert.Empty(store.Pending);
        Assert.Empty(store.DeadLettered);
    }

    [Fact]
    public async Task A_store_that_cannot_record_a_failure_leaves_the_message_to_the_next_claim()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var inner = new InMemoryOutboxStore(clock);
        var store = new FlakyOutboxStore(inner);
        var broker = new OutageBroker();
        var logger = new RecordingLogger<OutboxRelay>();
        var options = new OutboxOptions { BatchSize = 10, ClaimLease = TimeSpan.FromSeconds(30) };
        await using var relay = new OutboxRelay(store, broker, options, clock, logger);
        await inner.AppendAsync(Message("orders", clock), token);

        // Both sides fail at once: the transport refuses the publish and the store refuses to
        // remember that it did.
        broker.Unreachable = true;
        store.FailMarkFailed = true;
        Assert.Equal(0, await relay.DrainAsync(token));

        Assert.True(logger.Has(OutboxEvents.StoreFailed));
        var held = Assert.Single(inner.Pending);
        Assert.Equal(0, held.Attempts);
        Assert.Null(held.NextAttemptAt);
        // Still leased to the relay that could not record anything, so nothing claims it yet.
        Assert.Equal(0, await relay.DrainAsync(token));

        // Recovery: the lease expires on its own and the message is published, once, by the
        // next claim. Nothing had to be replayed by hand.
        store.FailMarkFailed = false;
        broker.Unreachable = false;
        clock.Advance(options.ClaimLease + TimeSpan.FromSeconds(1));
        Assert.Equal(1, await relay.DrainAsync(token));
        Assert.Single(broker.Frames);
        Assert.Empty(inner.Pending);
    }

    [Fact]
    public async Task A_relay_killed_with_a_message_claimed_leaves_it_for_the_next_relay()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new OutageBroker();
        var options = new OutboxOptions { BatchSize = 10, ClaimLease = TimeSpan.FromSeconds(30) };
        await using var dying = new OutboxRelay(store, broker, options, clock);
        await using var survivor = new OutboxRelay(store, broker, options, clock);
        await store.AppendAsync(Message("orders", clock), token);
        using var killed = new CancellationTokenSource();

        broker.Hold = true;
        var drain = dying.DrainAsync(killed.Token).AsTask();
        await broker.Entered.Task.WaitAsync(Bounded, token);
        await killed.CancelAsync();

        // The relay dies inside the publish, with the message claimed and nothing recorded.
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() =>
            drain.WaitAsync(Bounded, token)
        );
        Assert.Empty(broker.Frames);
        Assert.Equal(0, dying.Published);

        // The survivor respects the dead relay's lease rather than publishing behind it.
        broker.Hold = false;
        Assert.Equal(0, await survivor.DrainAsync(token));

        // Recovery: the lease expires and the work moves on, which is the at-least-once
        // guarantee doing its job rather than a message being lost with the process.
        clock.Advance(options.ClaimLease + TimeSpan.FromSeconds(1));
        Assert.Equal(1, await survivor.DrainAsync(token));
        Assert.Single(broker.Frames);
        Assert.Empty(store.Pending);
    }

    [Fact]
    public async Task A_redelivery_storm_still_runs_the_handlers_once_per_subscription()
    {
        const int redeliveries = 16;
        var token = TestContext.Current.CancellationToken;
        var received = new Received();
        var builder = Host.CreateApplicationBuilder();
        builder.Services.AddSingleton(received);
        builder.Services.AddSingleton<InboxTests.RedeliveringBroker>();
        builder
            .Services.AddHostLoom()
            .UseTransport<InboxTests.RedeliveringBroker>()
            .UseInMemoryInbox(TimeSpan.FromHours(1))
            .AddSubscriber<OrderPlaced, AuditHandler>("orders", "audit")
            .AddSubscriber<OrderPlaced, ShippingHandler>("orders", "shipping");
        using var host = builder.Build();
        await host.StartAsync(token);
        var broker = (InboxTests.RedeliveringBroker)
            host.Services.GetRequiredService<IRequestBroker>();

        await host
            .Services.GetRequiredService<IPublishEndpoint>()
            .PublishAsync("orders", new OrderPlaced("A-1"), token);

        // A broker that redelivers the same frame many times over, all at once: the set-if-absent
        // behind the inbox is the only thing standing between it and duplicate side effects.
        var storm = Enumerable
            .Range(0, redeliveries)
            .Select(_ => Task.Run(async () => await broker.RedeliverAsync(0, token), token))
            .ToArray();
        await Task.WhenAll(storm).WaitAsync(Bounded, token);

        Assert.Equal(["audit:A-1", "shipping:A-1"], received.Sorted());
        Assert.Equal(2, host.Services.GetRequiredService<InMemoryInboxStore>().Count);
    }

    private static OutboxMessage Message(string topic, TimeProvider clock, byte payload = 0) =>
        new()
        {
            MessageId = Guid.NewGuid(),
            Topic = topic,
            MessageType = "tests:Order",
            Frame = new[] { payload },
            EnqueuedAt = clock.GetUtcNow(),
        };

    /// <summary>A transport that can refuse every publish, or hold one open until the test lets go.</summary>
    private sealed class OutageBroker : IEventBroker
    {
        private readonly Lock _gate = new();
        private readonly List<ReadOnlyMemory<byte>> _frames = [];
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );

        /// <summary>Whether every publish fails as if the broker were unreachable.</summary>
        public bool Unreachable { get; set; }

        /// <summary>Whether a publish blocks until the caller is cancelled.</summary>
        public bool Hold { get; set; }

        /// <summary>Completes when a held publish has been entered.</summary>
        public TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public IReadOnlyList<ReadOnlyMemory<byte>> Frames
        {
            get
            {
                lock (_gate)
                {
                    return [.. _frames];
                }
            }
        }

        public async ValueTask PublishAsync(
            RequestAddress topic,
            ReadOnlyMemory<byte> frame,
            CancellationToken cancellationToken
        )
        {
            if (Unreachable)
            {
                throw new IOException($"the broker refused a publish to '{topic}'.");
            }

            if (Hold)
            {
                Entered.TrySetResult();
                await _release.Task.WaitAsync(cancellationToken);
            }

            lock (_gate)
            {
                _frames.Add(frame);
            }
        }

        public ValueTask<IAsyncDisposable> SubscribeAsync(
            RequestAddress topic,
            string subscription,
            EventFrameHandler handler,
            CancellationToken cancellationToken
        ) => throw new NotSupportedException();
    }

    /// <summary>A store whose failure bookkeeping can be made to fail, as a database outage would.</summary>
    private sealed class FlakyOutboxStore(IOutboxStore inner) : IOutboxStore
    {
        public bool FailMarkFailed { get; set; }

        public ValueTask AppendAsync(
            OutboxMessage message,
            CancellationToken cancellationToken = default
        ) => inner.AppendAsync(message, cancellationToken);

        public ValueTask<IReadOnlyList<OutboxMessage>> ClaimAsync(
            int batchSize,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        ) => inner.ClaimAsync(batchSize, lease, cancellationToken);

        public ValueTask MarkPublishedAsync(
            Guid messageId,
            CancellationToken cancellationToken = default
        ) => inner.MarkPublishedAsync(messageId, cancellationToken);

        public ValueTask MarkFailedAsync(
            Guid messageId,
            string error,
            DateTimeOffset nextAttemptAt,
            CancellationToken cancellationToken = default
        ) =>
            FailMarkFailed
                ? ValueTask.FromException(new IOException("the outbox database refused the update"))
                : inner.MarkFailedAsync(messageId, error, nextAttemptAt, cancellationToken);

        public ValueTask MarkDeadLetteredAsync(
            Guid messageId,
            string error,
            CancellationToken cancellationToken = default
        ) => inner.MarkDeadLetteredAsync(messageId, error, cancellationToken);
    }
}
