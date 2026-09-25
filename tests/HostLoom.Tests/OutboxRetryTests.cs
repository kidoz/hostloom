using System.Diagnostics.Metrics;
using Xunit;
using static HostLoom.Tests.OutboxInboxChaosTests;
using static HostLoom.Tests.OutboxTests;

namespace HostLoom.Tests;

/// <summary>
/// A message the transport keeps refusing is retried with a growing, clamped delay and then
/// dead-lettered, so a poison message neither spins the relay nor blocks the store forever, and
/// the store keeps the failure's type rather than its message. The relay itself pauses by the
/// same arithmetic after each drain that ends on a failed publish, deaf to wakes, and starts
/// over from the first delay once a publish succeeds.
/// </summary>
public sealed class OutboxRetryTests
{
    [Fact]
    public async Task A_failing_message_backs_off_exponentially_and_is_dead_lettered_after_max_attempts()
    {
        using var recorder = new CounterRecorder("hostloom.outbox.dead_lettered");
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new RecordingEventBroker();
        var options = new OutboxOptions
        {
            BatchSize = 10,
            MaxAttempts = 4,
            RetryDelay = TimeSpan.FromSeconds(1),
            MaxRetryDelay = TimeSpan.FromSeconds(3),
            RetryBackoffFactor = 2,
        };
        await using var relay = new OutboxRelay(store, broker, options, clock);
        var message = Message("orders", clock);
        await store.AppendAsync(message, TestContext.Current.CancellationToken);

        var delays = new List<TimeSpan>();
        for (var attempt = 1; attempt < options.MaxAttempts; attempt++)
        {
            broker.FailNext = new IOException($"broker down: attempt {attempt}");
            Assert.Equal(0, await relay.DrainAsync(TestContext.Current.CancellationToken));
            var pending = Assert.Single(store.Pending);
            Assert.Equal(attempt, pending.Attempts);
            Assert.NotNull(pending.NextAttemptAt);
            delays.Add(pending.NextAttemptAt!.Value - clock.GetUtcNow());

            // Nothing is due yet, so a drain before the backoff elapses claims nothing.
            Assert.Equal(0, await relay.DrainAsync(TestContext.Current.CancellationToken));
            Assert.Null(broker.FailNext);
            clock.Advance(delays[^1]);
        }

        // 1s, 2s, then 4s clamped to the 3s ceiling.
        Assert.Equal(
            [TimeSpan.FromSeconds(1), TimeSpan.FromSeconds(2), TimeSpan.FromSeconds(3)],
            delays
        );
        Assert.Equal(typeof(IOException).FullName, store.LastError);
        Assert.DoesNotContain("broker down", store.LastError, StringComparison.Ordinal);

        broker.FailNext = new IOException("broker down: final");
        Assert.Equal(0, await relay.DrainAsync(TestContext.Current.CancellationToken));

        Assert.Empty(store.Pending);
        var dead = Assert.Single(store.DeadLettered);
        Assert.Equal(message.MessageId, dead.MessageId);
        Assert.Equal(options.MaxAttempts, dead.Attempts);
        Assert.Equal(1, relay.DeadLettered);
        Assert.Equal(options.MaxAttempts, relay.Failed);
        Assert.Equal(0, relay.Published);
        // The listener is process-wide, so another class dead-lettering in parallel may add to it.
        Assert.Contains(1L, recorder.Values);

        // Dead-lettered means never claimed again, however long the relay keeps draining.
        clock.Advance(TimeSpan.FromDays(1));
        Assert.Equal(0, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Empty(broker.Published);
    }

    [Fact]
    public async Task A_requeued_dead_letter_is_published_on_the_next_drain()
    {
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new RecordingEventBroker();
        await using var relay = new OutboxRelay(
            store,
            broker,
            new OutboxOptions { MaxAttempts = 1 },
            clock
        );
        var message = Message("orders", clock);
        await store.AppendAsync(message, TestContext.Current.CancellationToken);
        broker.FailNext = new IOException("broker down");
        Assert.Equal(0, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Single(store.DeadLettered);

        Assert.True(store.Requeue(message.MessageId));
        Assert.False(store.Requeue(message.MessageId));

        Assert.Equal(1, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Empty(store.DeadLettered);
        Assert.Equal([message.MessageId], store.Published.Select(m => m.MessageId));
    }

    [Fact]
    public async Task A_message_with_no_retry_delay_is_claimable_at_once()
    {
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new RecordingEventBroker();
        await using var relay = new OutboxRelay(
            store,
            broker,
            new OutboxOptions { RetryDelay = TimeSpan.Zero, MaxRetryDelay = TimeSpan.Zero },
            clock
        );
        await store.AppendAsync(Message("orders", clock), TestContext.Current.CancellationToken);
        broker.FailNext = new IOException("blip");

        Assert.Equal(0, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Equal(1, await relay.DrainAsync(TestContext.Current.CancellationToken));
        Assert.Empty(store.Pending);
    }

    [Fact]
    public async Task A_woken_relay_does_not_drain_while_it_backs_off()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new OutageBroker { Unreachable = true };
        var options = new OutboxOptions
        {
            PollInterval = TimeSpan.FromHours(1),
            RetryDelay = TimeSpan.FromSeconds(10),
            MaxRetryDelay = TimeSpan.FromSeconds(10),
        };
        await using var relay = new OutboxRelay(store, broker, options, clock);
        await store.AppendAsync(Message("orders", clock), token);
        await relay.StartAsync(token);
        await SchedulingTests.WaitUntilAsync(() =>
            broker.Attempts == 1 && clock.PendingTimers == 1
        );

        // Publishes keep arriving during the outage and each one wakes the relay. Every wake
        // answered would be a drain that fails and charges another message an attempt.
        for (var index = 0; index < 5; index++)
        {
            await store.AppendAsync(Message("orders", clock), token);
            relay.Wake();
        }

        // A woken loop drains at once, so a quarter of a second is ample for it to have tried.
        await Task.Delay(TimeSpan.FromMilliseconds(250), token);
        clock.Advance(options.RetryDelay - TimeSpan.FromTicks(1));
        Assert.Equal(1, broker.Attempts);
        Assert.Equal(1, relay.Failed);

        // The pause ends and the loop tries one message.
        clock.Advance(TimeSpan.FromTicks(1));
        await SchedulingTests.WaitUntilAsync(() =>
            broker.Attempts == 2 && clock.PendingTimers == 1
        );
        Assert.Equal(2, relay.Failed);

        // Stopping during a pause ends the loop and its timer without waiting the pause out.
        await relay.StopAsync(token).WaitAsync(TimeSpan.FromSeconds(30), token);
        Assert.Equal(0, clock.PendingTimers);
        Assert.Equal(2, broker.Attempts);
    }

    [Fact]
    public async Task The_relay_pause_grows_with_each_failed_drain_and_resets_after_a_publish()
    {
        var token = TestContext.Current.CancellationToken;
        var clock = new TestClock();
        var store = new InMemoryOutboxStore(clock);
        var broker = new OutageBroker { Unreachable = true };
        var logger = new RecordingLogger<OutboxRelay>();
        var options = new OutboxOptions
        {
            PollInterval = TimeSpan.FromHours(1),
            MaxAttempts = 100,
            RetryDelay = TimeSpan.FromSeconds(1),
            MaxRetryDelay = TimeSpan.FromSeconds(4),
            RetryBackoffFactor = 2,
        };
        await using var relay = new OutboxRelay(store, broker, options, clock, logger);
        await store.AppendAsync(Message("orders", clock), token);
        await relay.StartAsync(token);
        await AttemptedAsync(1);

        // 1s, 2s, then 4s clamped to the 4s ceiling, twice: advancing to just short of the pause
        // drains nothing, and the last tick of it drains once. The hour-long poll interval never
        // comes into it.
        foreach (var (seconds, attempts) in new[] { (1, 2), (2, 3), (4, 4), (4, 5) })
        {
            clock.Advance(TimeSpan.FromSeconds(seconds) - TimeSpan.FromTicks(1));
            Assert.Equal(attempts - 1, broker.Attempts);
            clock.Advance(TimeSpan.FromTicks(1));
            await AttemptedAsync(attempts);
        }

        // Recovery: the next probe publishes, and the relay is back to waiting for wakes.
        broker.Unreachable = false;
        clock.Advance(options.MaxRetryDelay);
        await SchedulingTests.WaitUntilAsync(() =>
            store.Pending.Count == 0 && clock.PendingTimers == 1
        );
        Assert.Equal(6, broker.Attempts);

        // The next failure pauses for the first retry delay again, not the ceiling.
        broker.Unreachable = true;
        await store.AppendAsync(Message("orders", clock), token);
        relay.Wake();
        await AttemptedAsync(7);
        clock.Advance(options.RetryDelay);
        await AttemptedAsync(8);

        // One line per transition, not per failed drain.
        Assert.Equal(
            2,
            logger.Entries.Count(entry => entry.Event.Id == OutboxEvents.RelayBackingOff.Id)
        );
        Assert.Single(logger.Entries, entry => entry.Event.Id == OutboxEvents.RelayRecovered.Id);
        await relay.StopAsync(token).WaitAsync(TimeSpan.FromSeconds(30), token);

        Task AttemptedAsync(int attempts) =>
            SchedulingTests.WaitUntilAsync(() =>
                broker.Attempts == attempts && clock.PendingTimers == 1
            );
    }

    [Theory]
    [InlineData("Outbox:MaxAttempts", 0, 1, 2, 2.0)]
    [InlineData("Outbox:RetryDelay", 3, -1, 2, 2.0)]
    [InlineData("Outbox:MaxRetryDelay", 3, 5, 2, 2.0)]
    [InlineData("Outbox:RetryBackoffFactor", 3, 1, 2, 0.5)]
    public void Retry_options_are_validated(
        string key,
        int maxAttempts,
        int retryDelaySeconds,
        int maxRetryDelaySeconds,
        double factor
    )
    {
        var options = new OutboxOptions
        {
            MaxAttempts = maxAttempts,
            RetryDelay = TimeSpan.FromSeconds(retryDelaySeconds),
            MaxRetryDelay = TimeSpan.FromSeconds(maxRetryDelaySeconds),
            RetryBackoffFactor = factor,
        };

        var problem = Assert.Single(options.Validate());
        Assert.StartsWith(key, problem, StringComparison.Ordinal);
        Assert.Throws<ArgumentException>(() =>
            new OutboxRelay(new InMemoryOutboxStore(), new RecordingEventBroker(), options)
        );
    }

    private static OutboxMessage Message(string topic, TimeProvider clock) =>
        new()
        {
            MessageId = Guid.NewGuid(),
            Topic = topic,
            MessageType = "tests:Order",
            Frame = new byte[] { 1 },
            EnqueuedAt = clock.GetUtcNow(),
        };

    private sealed class CounterRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly Lock _gate = new();
        private readonly List<long> _values = [];

        public CounterRecorder(string instrumentName)
        {
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (
                    instrument.Meter.Name == HostLoomDiagnostics.MeterName
                    && instrument.Name == instrumentName
                )
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<long>(
                (_, value, _, _) =>
                {
                    lock (_gate)
                    {
                        _values.Add(value);
                    }
                }
            );
            _listener.Start();
        }

        public IReadOnlyList<long> Values
        {
            get
            {
                lock (_gate)
                {
                    return [.. _values];
                }
            }
        }

        public void Dispose() => _listener.Dispose();
    }
}
