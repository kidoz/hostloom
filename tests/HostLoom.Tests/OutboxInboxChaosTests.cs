using System.Collections.Concurrent;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Xunit;
using static HostLoom.Tests.PublishSubscribeTests;

namespace HostLoom.Tests;

/// <summary>
/// Controlled faults across the store-and-forward path: two relays draining one store at once, a
/// transport that refuses every publish and then recovers, a transport that refuses one message
/// and takes the rest, a store that cannot record a failure or a publish, a relay killed with a
/// message claimed, and a redelivery storm on the receiving side. The hypothesis is the one the
/// outbox exists for: nothing is lost while the transport is down, an outage is not charged to
/// every waiting message, nothing is published twice inside a claim, a relay that dies leaves its
/// work to the next one, and the inbox turns the at-least-once delivery that follows into one run
/// per subscription.
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
    public async Task A_transport_outage_charges_only_the_message_it_tried_and_publishes_everything_when_it_heals()
    {
        const int messages = 5;
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var inner = new InMemoryOutboxStore(clock);
        var store = new FlakyOutboxStore(inner);
        var broker = new OutageBroker();
        var options = new OutboxOptions
        {
            BatchSize = 10,
            MaxAttempts = 5,
            ClaimLease = TimeSpan.FromSeconds(30),
            RetryDelay = TimeSpan.FromSeconds(2),
            MaxRetryDelay = TimeSpan.FromSeconds(8),
        };
        await using var relay = new OutboxRelay(store, broker, options, clock);
        for (var index = 0; index < messages; index++)
        {
            await inner.AppendAsync(Message("orders", clock, (byte)index), token);
        }

        broker.Unreachable = true;
        Assert.Equal(0, await relay.DrainAsync(token));

        // The drain stopped at the first refusal: one publish was tried and one attempt counted.
        // The four claimed with it were never tried, so they carry no attempt and no retry time;
        // their claim lease holds them. Nothing is lost and nothing was given up on.
        Assert.Equal(1, broker.Attempts);
        Assert.Equal(messages, inner.Pending.Count);
        Assert.Equal([1, 0, 0, 0, 0], inner.Pending.Select(message => message.Attempts));
        Assert.NotNull(inner.Pending[0].NextAttemptAt);
        Assert.All(inner.Pending.Skip(1), message => Assert.Null(message.NextAttemptAt));
        Assert.Empty(inner.DeadLettered);
        Assert.Equal(1, relay.Failed);

        // Nothing new is due, so a drain during the outage probes with the oldest message the
        // relay still holds. That one refusal costs that one message an attempt; the other three
        // stay held, still untried.
        Assert.Equal(0, await relay.DrainAsync(token));
        Assert.Equal(2, broker.Attempts);
        Assert.Equal([10, 1], store.ClaimSizes);
        Assert.Equal([1, 1, 0, 0, 0], inner.Pending.Select(message => message.Attempts));
        Assert.Equal(2, relay.Failed);

        // Recovery: the first failed message's backoff elapses and it goes out as the probe. The
        // three the relay still holds go out right behind it, under the claim they were taken
        // with, rather than when that claim's lease expires; the drain then carries on in full
        // batches and finds the second failed message due.
        broker.Unreachable = false;
        clock.Advance(options.RetryDelay);
        Assert.Equal(messages, await relay.DrainAsync(token));
        Assert.Equal([10, 1, 1, 10], store.ClaimSizes);
        Assert.Equal([0, 2, 3, 4, 1], broker.Frames.Select(frame => frame.Span[0]));
        Assert.Empty(inner.Pending);
        Assert.Empty(inner.DeadLettered);
        Assert.Equal(2, relay.Failed);
        Assert.Equal(messages, relay.Published);
    }

    [Fact]
    public async Task A_held_message_whose_lease_is_half_gone_is_left_to_lease_expiry_and_published_once()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new OutageBroker();
        // The first retry comes after two thirds of the lease: later than this relay may still
        // publish what it holds under that claim.
        var options = new OutboxOptions
        {
            BatchSize = 10,
            ClaimLease = TimeSpan.FromSeconds(30),
            RetryDelay = TimeSpan.FromSeconds(20),
            MaxRetryDelay = TimeSpan.FromSeconds(20),
        };
        await using var relay = new OutboxRelay(store, broker, options, clock);
        await using var other = new OutboxRelay(store, broker, options, clock);
        for (var index = 0; index < 3; index++)
        {
            await store.AppendAsync(Message("orders", clock, (byte)index), token);
        }

        broker.Unreachable = true;
        Assert.Equal(0, await relay.DrainAsync(token));
        broker.Unreachable = false;

        // Within the first half of the lease the relay would publish the two it holds; past it,
        // the probe goes out alone and the held two are left to their lease.
        clock.Advance(options.RetryDelay);
        Assert.Equal(1, await relay.DrainAsync(token));
        Assert.Equal([0], broker.Frames.Select(frame => frame.Span[0]));
        Assert.Equal(2, store.Pending.Count);

        // Still leased: neither this relay nor another publishes them before the lease expires.
        Assert.Equal(0, await relay.DrainAsync(token));
        Assert.Equal(0, await other.DrainAsync(token));
        Assert.Single(broker.Frames);

        // After it, another relay claims and publishes them, and this one, having let them go,
        // does not publish them again.
        clock.Advance(options.ClaimLease - options.RetryDelay);
        Assert.Equal(2, await other.DrainAsync(token));
        Assert.Equal(0, await relay.DrainAsync(token));
        Assert.Equal([0, 1, 2], broker.Frames.Select(frame => frame.Span[0]));
        Assert.Empty(store.Pending);
    }

    [Fact]
    public async Task A_relay_holds_at_most_a_batch_and_forgets_what_it_holds_when_it_stops()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new OutageBroker();
        var options = new OutboxOptions
        {
            BatchSize = 2,
            ClaimLease = TimeSpan.FromMinutes(1),
            RetryDelay = TimeSpan.FromSeconds(1),
            MaxRetryDelay = TimeSpan.FromSeconds(1),
        };
        await using var relay = new OutboxRelay(store, broker, options, clock);
        for (var index = 0; index < 6; index++)
        {
            await store.AppendAsync(Message("orders", clock, (byte)index), token);
        }

        // The first drain holds message 1 of its batch; each probe after it holds the message
        // that failed before (0, then 2), so the third held message pushes out message 1, the
        // one claimed longest ago.
        broker.Unreachable = true;
        Assert.Equal(0, await relay.DrainAsync(token));
        for (var probe = 0; probe < 2; probe++)
        {
            clock.Advance(options.RetryDelay);
            Assert.Equal(0, await relay.DrainAsync(token));
        }

        Assert.Equal(3, broker.Attempts);

        // The next probe (message 4, with 3 behind it) publishes, and the relay publishes the two
        // it kept, 0 and 2, before carrying on to 5. Message 1, which it no longer holds, waits
        // for its lease, untried.
        broker.Unreachable = false;
        clock.Advance(options.RetryDelay);
        Assert.Equal(5, await relay.DrainAsync(token));
        Assert.Equal([4, 0, 2, 3, 5], broker.Frames.Select(frame => frame.Span[0]));
        var dropped = Assert.Single(store.Pending);
        Assert.Equal(0, dropped.Attempts);

        // What the relay holds is forgotten when it stops, even well inside the lease: message 6,
        // held behind the refused message 1, is not published by the next drain here but when
        // its lease expires.
        broker.Unreachable = true;
        clock.Advance(options.ClaimLease);
        await store.AppendAsync(Message("orders", clock, 6), token);
        Assert.Equal(0, await relay.DrainAsync(token));
        await relay.StopAsync(token);
        broker.Unreachable = false;
        clock.Advance(options.RetryDelay);
        Assert.Equal(1, await relay.DrainAsync(token));
        Assert.Single(store.Pending);
        clock.Advance(options.ClaimLease);
        Assert.Equal(1, await relay.DrainAsync(token));
        Assert.Empty(store.Pending);
        Assert.Equal([4, 0, 2, 3, 5, 1, 6], broker.Frames.Select(frame => frame.Span[0]));
    }

    [Fact]
    public async Task An_outage_across_many_backoffs_costs_one_attempt_per_failed_drain_and_dead_letters_nothing()
    {
        const int messages = 1_000;
        const int failedDrains = 25;
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new OutageBroker { Unreachable = true };
        var logger = new RecordingLogger<OutboxRelay>();
        // Five attempts before a message is dead-lettered, against twenty-five failed drains:
        // charging every pending message per drain, or the same message on every probe, would
        // dead-letter within the first five.
        var options = new OutboxOptions
        {
            PollInterval = TimeSpan.FromHours(1),
            BatchSize = 100,
            ClaimLease = TimeSpan.FromMinutes(1),
            MaxAttempts = 5,
            RetryDelay = TimeSpan.FromSeconds(1),
            MaxRetryDelay = TimeSpan.FromSeconds(8),
        };
        for (var index = 0; index < messages; index++)
        {
            await store.AppendAsync(Message("orders", clock, index), token);
        }

        await using var relay = new OutboxRelay(store, broker, options, clock, logger);
        await relay.StartAsync(token);

        // The loop drains at once and from then on only when its pause ends. The poll interval is
        // an hour and wakes are ignored, so each advance by the pause is exactly one more drain.
        for (var drain = 1; drain <= failedDrains; drain++)
        {
            var expected = drain;
            await SchedulingTests.WaitUntilAsync(() =>
                broker.Attempts == expected && clock.PendingTimers == 1
            );
            relay.Wake();
            if (drain < failedDrains)
            {
                clock.Advance(Pause(options, drain));
            }
        }

        Assert.Equal(failedDrains, relay.Failed);
        Assert.Equal(failedDrains, store.Pending.Sum(message => message.Attempts));
        Assert.Equal(messages, store.Pending.Count);
        Assert.Empty(store.DeadLettered);
        Assert.Equal(0, relay.DeadLettered);
        Assert.Equal(
            failedDrains,
            logger.Entries.Count(entry => entry.Event.Id == OutboxEvents.PublishFailed.Id)
        );
        Assert.Single(logger.Entries, entry => entry.Event.Id == OutboxEvents.RelayBackingOff.Id);
        Assert.False(logger.Has(OutboxEvents.RelayRecovered));

        // Recovery: the broker answers, the pause ends after every lease has run out, and one
        // drain publishes the whole backlog, each message once.
        broker.Unreachable = false;
        clock.Advance(Pause(options, failedDrains) + options.ClaimLease);
        await SchedulingTests.WaitUntilAsync(() => store.Pending.Count == 0);
        Assert.Equal(messages, broker.Frames.Count);
        Assert.Equal(messages, broker.Frames.Select(Payload).Distinct().Count());
        Assert.Empty(store.DeadLettered);
        Assert.Equal(messages, relay.Published);
        Assert.Single(logger.Entries, entry => entry.Event.Id == OutboxEvents.RelayRecovered.Id);
        await relay.StopAsync(token).WaitAsync(Bounded, token);
        Assert.Equal(0, clock.PendingTimers);
    }

    [Fact]
    public async Task A_poison_message_is_dead_lettered_while_the_messages_behind_it_keep_flowing()
    {
        const int poison = -1;
        const int healthy = 30;
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new OutageBroker { Refuses = frame => Payload(frame) == poison };
        var options = new OutboxOptions
        {
            PollInterval = TimeSpan.FromSeconds(1),
            BatchSize = 10,
            ClaimLease = TimeSpan.FromSeconds(30),
            MaxAttempts = 4,
            RetryDelay = TimeSpan.FromSeconds(1),
            MaxRetryDelay = TimeSpan.FromSeconds(4),
        };
        var refused = Message("orders", clock, poison);
        await store.AppendAsync(refused, token);
        for (var index = 0; index < healthy; index++)
        {
            await store.AppendAsync(Message("orders", clock, index), token);
        }

        await using var relay = new OutboxRelay(store, broker, options, clock);
        await relay.StartAsync(token);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);

        // A second at a time, as the loop's own poll and pause timers see it. The oldest message
        // is refused every time; it is due again just as the relay's pause ends, and the probe
        // passes it over for a message behind it, so the rest go out while it backs off.
        var publishedWhileRefusedPending = broker.Frames.Count;
        for (var second = 1; second <= 40; second++)
        {
            clock.Advance(TimeSpan.FromSeconds(1));
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
            if (store.DeadLettered.Count == 0)
            {
                publishedWhileRefusedPending = broker.Frames.Count;
            }
        }

        // Every other message went out before the refused one was given up on, the nine claimed
        // alongside it on the first drain included: the relay held them and published them as
        // soon as a probe got through, rather than leaving them to their lease.
        Assert.Equal(healthy, publishedWhileRefusedPending);
        var dead = Assert.Single(store.DeadLettered);
        Assert.Equal(refused.MessageId, dead.MessageId);
        Assert.Equal(options.MaxAttempts, dead.Attempts);
        Assert.Equal(options.MaxAttempts, relay.Failed);
        Assert.Empty(store.Pending);
        Assert.Equal(healthy, broker.Frames.Count);
        Assert.Equal(healthy, broker.Frames.Select(Payload).Distinct().Count());
        await relay.StopAsync(token).WaitAsync(Bounded, token);
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
    public async Task A_store_that_cannot_mark_a_publish_leaves_the_lease_to_expire_without_counting_an_attempt()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var inner = new InMemoryOutboxStore(clock);
        var store = new FlakyOutboxStore(inner);
        var broker = new OutageBroker();
        var logger = new RecordingLogger<OutboxRelay>();
        // One attempt: counting a publish the store failed to mark would dead-letter a message
        // the transport already has.
        var options = new OutboxOptions
        {
            BatchSize = 10,
            MaxAttempts = 1,
            ClaimLease = TimeSpan.FromSeconds(30),
        };
        await using var relay = new OutboxRelay(store, broker, options, clock, logger);
        await inner.AppendAsync(Message("orders", clock, 0), token);
        await inner.AppendAsync(Message("orders", clock, 1), token);

        // The transport takes both frames; the store refuses to record that it did.
        store.FailMarkPublished = true;
        Assert.Equal(0, await relay.DrainAsync(token));

        Assert.Equal(2, broker.Frames.Count);
        Assert.Empty(inner.DeadLettered);
        Assert.Equal(2, inner.Pending.Count);
        Assert.All(inner.Pending, message => Assert.Equal(0, message.Attempts));
        Assert.All(inner.Pending, message => Assert.Null(message.NextAttemptAt));
        Assert.Equal(0, relay.Failed);
        Assert.Equal(0, relay.Published);
        Assert.Equal(
            2,
            logger.Entries.Count(entry => entry.Event.Id == OutboxEvents.MarkPublishedFailed.Id)
        );
        Assert.False(logger.Has(OutboxEvents.PublishFailed));
        Assert.False(logger.Has(OutboxEvents.DeadLettered));

        // Still leased to this relay, so nothing goes out a second time inside the lease.
        Assert.Equal(0, await relay.DrainAsync(token));
        Assert.Equal(2, broker.Frames.Count);

        // Recovery: the lease expires and the next claim publishes both again. That duplicate is
        // the at-least-once delivery the inbox on the receiving side absorbs.
        store.FailMarkPublished = false;
        clock.Advance(options.ClaimLease + TimeSpan.FromSeconds(1));
        Assert.Equal(2, await relay.DrainAsync(token));
        Assert.Equal([0, 1, 0, 1], broker.Frames.Select(frame => frame.Span[0]));
        Assert.Empty(inner.Pending);
        Assert.Equal(2, inner.Published.Count);
        Assert.Empty(inner.DeadLettered);
        Assert.Equal(0, relay.Failed);
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

    /// <summary>A message whose frame is the four bytes of <paramref name="payload"/>, for runs longer than a byte counts.</summary>
    private static OutboxMessage Message(string topic, TimeProvider clock, int payload) =>
        Message(topic, clock) with
        {
            Frame = BitConverter.GetBytes(payload),
        };

    private static int Payload(ReadOnlyMemory<byte> frame) =>
        frame.Length == sizeof(int) ? BitConverter.ToInt32(frame.Span) : frame.Span[0];

    /// <summary>
    /// The relay's documented pause after <paramref name="failedDrains"/> consecutive failed
    /// drains: the retry delay grown by the backoff factor per failed drain, clamped.
    /// </summary>
    private static TimeSpan Pause(OutboxOptions options, int failedDrains) =>
        TimeSpan.FromTicks(
            (long)
                Math.Min(
                    options.RetryDelay.Ticks
                        * Math.Pow(options.RetryBackoffFactor, failedDrains - 1),
                    options.MaxRetryDelay.Ticks
                )
        );

    /// <summary>A transport that can refuse every publish, or hold one open until the test lets go.</summary>
    internal sealed class OutageBroker : IEventBroker
    {
        private readonly Lock _gate = new();
        private readonly List<ReadOnlyMemory<byte>> _frames = [];
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int _attempts;

        /// <summary>Whether every publish fails as if the broker were unreachable.</summary>
        public bool Unreachable { get; set; }

        /// <summary>Whether a publish blocks until the caller is cancelled.</summary>
        public bool Hold { get; set; }

        /// <summary>Frames refused while every other frame goes out, as with a message the broker rejects.</summary>
        public Func<ReadOnlyMemory<byte>, bool>? Refuses { get; set; }

        /// <summary>Every publish call, refused or not.</summary>
        public int Attempts => Volatile.Read(ref _attempts);

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
            Interlocked.Increment(ref _attempts);
            if (Unreachable || Refuses?.Invoke(frame) == true)
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

        public bool FailMarkPublished { get; set; }

        /// <summary>The batch size of every claim, in order.</summary>
        public ConcurrentQueue<int> ClaimSizes { get; } = new();

        public ValueTask AppendAsync(
            OutboxMessage message,
            CancellationToken cancellationToken = default
        ) => inner.AppendAsync(message, cancellationToken);

        public ValueTask<IReadOnlyList<OutboxMessage>> ClaimAsync(
            int batchSize,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        )
        {
            ClaimSizes.Enqueue(batchSize);
            return inner.ClaimAsync(batchSize, lease, cancellationToken);
        }

        public ValueTask MarkPublishedAsync(
            Guid messageId,
            CancellationToken cancellationToken = default
        ) =>
            FailMarkPublished
                ? ValueTask.FromException(new IOException("the outbox database refused the update"))
                : inner.MarkPublishedAsync(messageId, cancellationToken);

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
