using System.Diagnostics.Metrics;
using HostLoom.Locking;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

public sealed class LockingTests
{
    private static readonly TimeSpan OneSecond = TimeSpan.FromSeconds(1);

    [Fact]
    public void Linear_default_delays_grow_by_the_step_and_bound_the_total()
    {
        var policy = new LockingOptions().Retry;

        Assert.Equal(10, policy.RetryLimit);
        Assert.Equal(TimeSpan.FromMilliseconds(50), policy.GetBaseDelay(1));
        Assert.Equal(TimeSpan.FromMilliseconds(100), policy.GetBaseDelay(2));
        Assert.Equal(TimeSpan.FromMilliseconds(500), policy.GetBaseDelay(10));
        // 50 × (1 + … + 10) = 2 750 ms of base delay plus ten jitters of at most 50 ms.
        Assert.Equal(TimeSpan.FromMilliseconds(3250), policy.MaxTotalDelay);
        Assert.Contains("linear", policy.Description, StringComparison.Ordinal);
    }

    [Fact]
    public void Jitter_is_additive_and_stays_within_its_bound()
    {
        var policy = LockRetryPolicy
            .Linear(3, TimeSpan.FromMilliseconds(50))
            .WithJitter(TimeSpan.FromMilliseconds(50));

        for (var i = 0; i < 200; i++)
        {
            var delay = policy.GetDelay(2);
            Assert.InRange(delay, TimeSpan.FromMilliseconds(100), TimeSpan.FromMilliseconds(150));
        }
    }

    [Fact]
    public void Immediate_interval_and_exponential_shapes_compute_their_delays()
    {
        Assert.Equal(TimeSpan.Zero, LockRetryPolicy.Immediate(2).GetDelay(1));
        Assert.Equal(
            TimeSpan.FromMilliseconds(10),
            LockRetryPolicy.Interval(2, TimeSpan.FromMilliseconds(10)).GetBaseDelay(2)
        );

        var exponential = LockRetryPolicy.Exponential(
            3,
            TimeSpan.FromMilliseconds(10),
            TimeSpan.FromMilliseconds(30)
        );
        Assert.Equal(TimeSpan.FromMilliseconds(10), exponential.GetBaseDelay(1));
        Assert.Equal(TimeSpan.FromMilliseconds(20), exponential.GetBaseDelay(2));
        Assert.Equal(TimeSpan.FromMilliseconds(30), exponential.GetBaseDelay(3));
        Assert.Equal(TimeSpan.FromMilliseconds(60), exponential.MaxTotalDelay);
    }

    [Fact]
    public void Retry_policy_rejects_bad_arguments()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => LockRetryPolicy.Immediate(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            LockRetryPolicy.Linear(1, TimeSpan.FromMilliseconds(-1))
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            LockRetryPolicy.Immediate(1).WithJitter(TimeSpan.FromMilliseconds(-1))
        );
        Assert.Throws<ArgumentOutOfRangeException>(() => LockRetryPolicy.Immediate(1).GetDelay(0));
    }

    [Fact]
    public void Validate_names_the_option_key_for_every_violation()
    {
        var options = new LockingOptions
        {
            Namespace = "Bad Space",
            DefaultLease = TimeSpan.Zero,
            MaxLease = TimeSpan.FromSeconds(-1),
            MaxHold = TimeSpan.Zero,
            MaxKeyLength = 0,
        };

        var problems = options.Validate();

        Assert.Contains(problems, p => p.Contains("Locking:Namespace", StringComparison.Ordinal));
        Assert.Contains(
            problems,
            p => p.Contains("Locking:DefaultLease", StringComparison.Ordinal)
        );
        Assert.Contains(problems, p => p.Contains("Locking:MaxLease", StringComparison.Ordinal));
        Assert.Contains(problems, p => p.Contains("Locking:MaxHold", StringComparison.Ordinal));
        Assert.Contains(
            problems,
            p => p.Contains("Locking:MaxKeyLength", StringComparison.Ordinal)
        );
        Assert.Empty(new LockingOptions { Namespace = "billing-1" }.Validate());
    }

    [Fact]
    public void DistributedLock_rejects_invalid_options_and_a_missing_provider()
    {
        var invalid = Assert.Throws<ArgumentException>(() =>
            new DistributedLock(
                new LockingOptions { Namespace = "BAD" },
                new InMemoryLockProvider()
            )
        );
        Assert.Contains("Locking:Namespace", invalid.Message, StringComparison.Ordinal);

        Assert.Throws<ArgumentNullException>(() =>
            new DistributedLock(new LockingOptions { Namespace = "ok" }, provider: null)
        );
    }

    [Fact]
    public async Task Two_locks_over_one_provider_exclude_each_other()
    {
        var clock = new TestClock();
        var provider = new InMemoryLockProvider(clock);
        await using var first = Compose(clock, provider);
        await using var second = Compose(clock, provider);

        await using var held = await first.TryAcquireAsync(
            "order:1",
            cancellationToken: TestContext.Current.CancellationToken
        );
        var contended = await second.TryAcquireAsync(
            "order:1",
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.NotNull(held);
        Assert.Equal("order:1", held.Key);
        Assert.Null(contended);
        Assert.Equal(1, provider.Count);

        await held.DisposeAsync();
        await using var afterRelease = await second.TryAcquireAsync(
            "order:1",
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.NotNull(afterRelease);
        Assert.False(held.IsHeld);
    }

    [Fact]
    public async Task Execute_releases_after_the_action_and_propagates_its_exception()
    {
        var clock = new TestClock();
        var provider = new InMemoryLockProvider(clock);
        await using var locks = Compose(clock, provider);

        var result = await locks.ExecuteWithLockAsync(
            "job",
            _ => ValueTask.FromResult(42),
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.Equal(42, result);
        Assert.Equal(0, provider.Count);

        await Assert.ThrowsAsync<InvalidOperationException>(async () =>
            await locks.ExecuteWithLockAsync(
                "job",
                _ => throw new InvalidOperationException("boom"),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        Assert.Equal(0, provider.Count);
    }

    [Fact]
    public async Task Contention_past_MaxWait_throws_with_the_wait_and_attempts()
    {
        var clock = new TestClock();
        var provider = new InMemoryLockProvider(clock);
        await using var holder = Compose(clock, provider);
        await using var waiter = Compose(clock, provider);
        await using var held = await holder.TryAcquireAsync(
            "k",
            cancellationToken: TestContext.Current.CancellationToken
        );

        var pending = waiter
            .ExecuteWithLockAsync(
                "k",
                _ => ValueTask.FromResult(0),
                new LockOptions
                {
                    MaxWait = TimeSpan.FromMilliseconds(100),
                    Retry = LockRetryPolicy.Interval(5, TimeSpan.FromMilliseconds(40)),
                },
                TestContext.Current.CancellationToken
            )
            .AsTask();

        // 0 ms, 40 ms, 80 ms fail; the last delay is truncated to 20 ms so the final attempt lands
        // on the bound rather than past it.
        clock.Advance(TimeSpan.FromMilliseconds(40));
        clock.Advance(TimeSpan.FromMilliseconds(40));
        clock.Advance(TimeSpan.FromMilliseconds(20));

        var failure = await Assert.ThrowsAsync<LockNotAcquiredException>(() => pending);
        Assert.Equal("k", failure.Key);
        Assert.Equal(4, failure.Attempts);
        Assert.Equal(TimeSpan.FromMilliseconds(100), failure.Waited);
        Assert.Equal(1, provider.Count);
    }

    [Fact]
    public async Task Retry_exhaustion_without_a_wall_clock_bound_throws_after_the_policy()
    {
        var clock = new TestClock();
        var provider = new InMemoryLockProvider(clock);
        await using var holder = Compose(clock, provider);
        await using var waiter = Compose(clock, provider);
        await using var held = await holder.TryAcquireAsync(
            "k",
            cancellationToken: TestContext.Current.CancellationToken
        );

        var pending = waiter
            .TryAcquireAsync(
                "k",
                new LockOptions
                {
                    Retry = LockRetryPolicy.Interval(2, TimeSpan.FromMilliseconds(10)),
                },
                TestContext.Current.CancellationToken
            )
            .AsTask();
        clock.Advance(TimeSpan.FromMilliseconds(10));
        clock.Advance(TimeSpan.FromMilliseconds(10));

        Assert.Null(await pending);
    }

    [Fact]
    public async Task Provider_failures_map_to_LockProviderUnavailableException()
    {
        var clock = new TestClock();
        var faulting = new FaultingLockProvider(
            new InMemoryLockProvider(clock),
            LockOperation.Acquire,
            LockFailureKind.Timeout,
            count: 1
        );
        await using var locks = Compose(clock, faulting);

        var failure = await Assert.ThrowsAsync<LockProviderUnavailableException>(async () =>
            await locks.ExecuteWithLockAsync(
                "k",
                _ => ValueTask.FromResult(0),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        Assert.Equal(LockFailureKind.Timeout, failure.Kind);
        Assert.Equal(1, failure.Attempts);
        Assert.IsType<LockProviderException>(failure.InnerException);

        var raw = new FaultingLockProvider(
            new InMemoryLockProvider(clock),
            LockOperation.Acquire,
            LockFailureKind.Other,
            count: 1,
            custom: new InvalidOperationException("socket")
        );
        await using var overRaw = Compose(clock, raw);
        var wrapped = await Assert.ThrowsAsync<LockProviderUnavailableException>(async () =>
            await overRaw.TryAcquireAsync(
                "k",
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        Assert.Equal(LockFailureKind.Other, wrapped.Kind);
        Assert.IsType<InvalidOperationException>(wrapped.InnerException);
    }

    [Fact]
    public async Task An_expired_lease_is_taken_over_and_the_first_handle_reports_the_loss()
    {
        var clock = new TestClock();
        var provider = new InMemoryLockProvider(clock);
        var logger = new RecordingLogger<DistributedLock>();
        await using var first = Compose(clock, provider, logger);
        await using var second = Compose(clock, provider);

        await using var lease = await first.TryAcquireAsync(
            "k",
            new LockOptions { Lease = OneSecond },
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(lease);
        Assert.Equal(DateTimeOffset.UnixEpoch + OneSecond, lease.LeaseEnd);

        clock.Advance(OneSecond);

        await using var takeover = await second.TryAcquireAsync(
            "k",
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.NotNull(takeover);
        Assert.False(lease.IsHeld);
        Assert.True(lease.LostToken.IsCancellationRequested);
        Assert.True(logger.Has(LockingEvents.LeaseLost));
        Assert.True(logger.Has(LockingEvents.HoldThreshold));

        // A stale owner's release is refused and never evicts the new owner.
        await lease.DisposeAsync();
        Assert.True(takeover.IsHeld);
        Assert.Equal(1, provider.Count);
        Assert.Null(
            await first.TryAcquireAsync(
                "k",
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task Extend_moves_the_lease_end_and_is_capped_by_MaxLease()
    {
        var clock = new TestClock();
        await using var locks = Compose(
            clock,
            new InMemoryLockProvider(clock),
            options: new LockingOptions
            {
                Namespace = "tests",
                DefaultLease = OneSecond,
                MaxLease = TimeSpan.FromSeconds(2),
            }
        );

        await using var handle = await locks.TryAcquireAsync(
            "k",
            new LockOptions { Lease = TimeSpan.FromSeconds(10) },
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(handle);
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromSeconds(2), handle.LeaseEnd);

        clock.Advance(OneSecond);
        Assert.True(await handle.ExtendAsync(OneSecond, TestContext.Current.CancellationToken));
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromSeconds(2), handle.LeaseEnd);

        clock.Advance(TimeSpan.FromMilliseconds(900));
        Assert.True(handle.IsHeld);
        clock.Advance(TimeSpan.FromMilliseconds(100));
        Assert.False(handle.IsHeld);
        Assert.False(await handle.ExtendAsync(OneSecond, TestContext.Current.CancellationToken));
    }

    [Fact]
    public async Task AutoExtend_heartbeats_on_the_clock_and_stops_at_MaxHold()
    {
        var clock = new TestClock();
        var logger = new RecordingLogger<DistributedLock>();
        await using var locks = Compose(
            clock,
            new InMemoryLockProvider(clock),
            logger,
            new LockingOptions { Namespace = "tests", MaxHold = TimeSpan.FromMilliseconds(1200) }
        );

        await using var handle = await locks.TryAcquireAsync(
            "k",
            new LockOptions { Lease = OneSecond, AutoExtend = true },
            TestContext.Current.CancellationToken
        );
        Assert.NotNull(handle);

        // Heartbeats at 0.5 s and 1.0 s each push the lease end a full second out.
        clock.Advance(TimeSpan.FromMilliseconds(500));
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromMilliseconds(1500), handle.LeaseEnd);
        clock.Advance(TimeSpan.FromMilliseconds(500));
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromMilliseconds(2000), handle.LeaseEnd);
        Assert.True(handle.IsHeld);

        // At 1.5 s the hold has passed MaxHold, so the heartbeat stops and the lease runs out.
        clock.Advance(TimeSpan.FromMilliseconds(500));
        Assert.True(logger.Has(LockingEvents.AutoExtendStopped));
        Assert.Equal(DateTimeOffset.UnixEpoch + TimeSpan.FromMilliseconds(2000), handle.LeaseEnd);
        clock.Advance(TimeSpan.FromMilliseconds(500));
        Assert.False(handle.IsHeld);
    }

    [Fact]
    public async Task OnLost_Cancel_cancels_the_token_handed_to_the_action()
    {
        var clock = new TestClock();
        await using var locks = Compose(clock, new InMemoryLockProvider(clock));
        var observed = new TaskCompletionSource();
        var cancelled = false;

        var run = locks
            .ExecuteWithLockAsync(
                "k",
                token =>
                {
                    token.Register(() =>
                    {
                        cancelled = true;
                        observed.TrySetResult();
                    });
                    return new ValueTask(observed.Task);
                },
                new LockOptions { Lease = OneSecond, OnLost = LostLeaseBehavior.Cancel },
                TestContext.Current.CancellationToken
            )
            .AsTask();

        Assert.False(run.IsCompleted);
        clock.Advance(OneSecond);

        await run;
        Assert.True(cancelled);
    }

    [Fact]
    public async Task Observe_keeps_the_action_running_after_the_lease_is_lost()
    {
        var clock = new TestClock();
        await using var locks = Compose(clock, new InMemoryLockProvider(clock));
        var proceed = new TaskCompletionSource();
        var tokenWasCancelled = true;

        var run = locks
            .ExecuteWithLockAsync(
                "k",
                async token =>
                {
                    await proceed.Task;
                    tokenWasCancelled = token.IsCancellationRequested;
                    return 1;
                },
                new LockOptions { Lease = OneSecond },
                TestContext.Current.CancellationToken
            )
            .AsTask();

        clock.Advance(OneSecond);
        proceed.SetResult();

        Assert.Equal(1, await run);
        Assert.False(tokenWasCancelled);
    }

    [Fact]
    public async Task Reentrancy_in_the_same_flow_is_rejected_immediately()
    {
        var clock = new TestClock();
        await using var locks = Compose(clock, new InMemoryLockProvider(clock));

        var failure = await Assert.ThrowsAsync<LockReentrancyException>(async () =>
            await locks.ExecuteWithLockAsync(
                "k",
                async token =>
                    await locks.ExecuteWithLockAsync(
                        "k",
                        _ => ValueTask.FromResult(0),
                        cancellationToken: token
                    ),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        Assert.Equal("k", failure.Key);

        // A different key inside the same flow is ordinary nesting.
        var nested = await locks.ExecuteWithLockAsync(
            "outer",
            async token =>
                await locks.ExecuteWithLockAsync(
                    "inner",
                    _ => ValueTask.FromResult(7),
                    cancellationToken: token
                ),
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.Equal(7, nested);
    }

    [Fact]
    public async Task Without_detection_reentrancy_is_plain_contention()
    {
        var clock = new TestClock();
        await using var locks = Compose(
            clock,
            new InMemoryLockProvider(clock),
            options: new LockingOptions { Namespace = "tests", DetectReentrancy = false }
        );

        await Assert.ThrowsAsync<LockNotAcquiredException>(async () =>
            await locks.ExecuteWithLockAsync(
                "k",
                async token =>
                    await locks.ExecuteWithLockAsync(
                        "k",
                        _ => ValueTask.FromResult(0),
                        new LockOptions { MaxWait = TimeSpan.Zero },
                        token
                    ),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
    }

    [Fact]
    public async Task Disabled_locking_runs_actions_immediately_and_needs_no_provider()
    {
        var logger = new RecordingLogger<DistributedLock>();
        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "tests", Enabled = false },
            provider: null,
            logger: logger
        );

        var result = await locks.ExecuteWithLockAsync(
            "k",
            _ => ValueTask.FromResult("ran"),
            cancellationToken: TestContext.Current.CancellationToken
        );
        await using var handle = await locks.TryAcquireAsync(
            "k",
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal("ran", result);
        Assert.NotNull(handle);
        Assert.True(handle.IsHeld);
        Assert.False(handle.LostToken.CanBeCanceled);
        Assert.True(logger.Has(LockingEvents.Disabled));
        Assert.Equal("(disabled)", LockingProbe.Describe(locks).Provider);
    }

    [Fact]
    public async Task Cancelling_a_waiting_caller_leaves_nothing_held()
    {
        var clock = new TestClock();
        var provider = new InMemoryLockProvider(clock);
        await using var holder = Compose(clock, provider);
        await using var waiter = Compose(clock, provider);
        await using var held = await holder.TryAcquireAsync(
            "k",
            cancellationToken: TestContext.Current.CancellationToken
        );
        using var cancellation = new CancellationTokenSource();

        var pending = waiter
            .ExecuteWithLockAsync(
                "k",
                _ => ValueTask.FromResult(0),
                new LockOptions { Retry = LockRetryPolicy.Interval(5, TimeSpan.FromSeconds(1)) },
                cancellation.Token
            )
            .AsTask();
        Assert.False(pending.IsCompleted);

        await cancellation.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => pending);
        Assert.Equal(1, provider.Count);
        await held!.DisposeAsync();
        Assert.Equal(0, provider.Count);
    }

    [Fact]
    public async Task Release_and_extend_failures_are_logged_not_thrown()
    {
        var clock = new TestClock();
        var inner = new InMemoryLockProvider(clock);
        var logger = new RecordingLogger<DistributedLock>();
        var releaseFaults = new FaultingLockProvider(
            inner,
            LockOperation.Release,
            LockFailureKind.Unavailable,
            count: 1
        );
        await using var locks = Compose(clock, releaseFaults, logger);

        await locks.ExecuteWithLockAsync(
            "k",
            _ => ValueTask.CompletedTask,
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(1, releaseFaults.Faulted);
        Assert.True(logger.Has(LockingEvents.ReleaseFailed));
        Assert.Contains(
            logger.Entries,
            entry =>
                entry.Event.Id == LockingEvents.ReleaseFailed.Id && entry.Level == LogLevel.Warning
        );

        var extendFaults = new FaultingLockProvider(
            inner,
            LockOperation.Extend,
            LockFailureKind.Timeout,
            count: 1
        );
        await using var extending = Compose(clock, extendFaults, logger);
        await using var handle = await extending.TryAcquireAsync(
            "other",
            cancellationToken: TestContext.Current.CancellationToken
        );
        Assert.NotNull(handle);
        Assert.False(await handle.ExtendAsync(OneSecond, TestContext.Current.CancellationToken));
        Assert.True(handle.IsHeld);
        Assert.True(logger.Has(LockingEvents.ExtendFailed));
    }

    [Fact]
    public async Task Metrics_record_acquisition_hold_and_the_enabled_gauge()
    {
        var clock = new TestClock();
        using var recorder = new MetricRecorder("metrics-ns");
        await using var locks = Compose(
            clock,
            new InMemoryLockProvider(clock),
            options: new LockingOptions { Namespace = "metrics-ns" }
        );

        await locks.ExecuteWithLockAsync(
            "k",
            _ =>
            {
                clock.Advance(TimeSpan.FromMilliseconds(250));
                return ValueTask.CompletedTask;
            },
            cancellationToken: TestContext.Current.CancellationToken
        );
        recorder.Observe();

        var acquire = Assert.Single(recorder.Measurements("hostloom.lock.acquire.duration"));
        Assert.Equal("acquired", acquire.Tags[LockingDiagnostics.OutcomeTag]);
        Assert.Equal([1, -1], recorder.Measurements("hostloom.lock.active").Select(m => m.Value));
        Assert.Equal(
            0.25,
            Assert.Single(recorder.Measurements("hostloom.lock.hold.duration")).Value
        );
        Assert.Equal(1, Assert.Single(recorder.Measurements("hostloom.lock.enabled")).Value);
    }

    [Fact]
    public async Task Probe_describes_the_composition_with_option_keys()
    {
        var clock = new TestClock();
        await using var locks = Compose(clock, new InMemoryLockProvider(clock));

        var description = LockingProbe.Describe(locks);

        Assert.Equal("tests", description.Namespace);
        Assert.Equal(nameof(InMemoryLockProvider), description.Provider);
        Assert.True(description.Enabled);
        Assert.Equal(TimeSpan.FromMilliseconds(3250), description.MaxWait);
        Assert.Contains(
            description.Lines,
            l => l.Contains("Locking:Namespace", StringComparison.Ordinal)
        );
        Assert.Contains(
            description.Lines,
            l => l.Contains("Locking:Retry", StringComparison.Ordinal)
        );
        Assert.Contains(
            description.Lines,
            l => l.Contains("Locking:Enabled = true", StringComparison.Ordinal)
        );
    }

    [Fact]
    public async Task Keys_are_validated_and_sensitive_keys_are_hashed()
    {
        var clock = new TestClock();
        await using var locks = Compose(clock, new InMemoryLockProvider(clock));

        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await locks.TryAcquireAsync(
                "has space",
                cancellationToken: TestContext.Current.CancellationToken
            )
        );
        await Assert.ThrowsAsync<ArgumentException>(async () =>
            await locks.TryAcquireAsync(
                new string('k', 513),
                cancellationToken: TestContext.Current.CancellationToken
            )
        );

        var hashed = LockKey.FromSensitive("refresh-token-value");
        Assert.Equal(32, hashed.Length);
        Assert.Equal(hashed, LockKey.FromSensitive("refresh-token-value"));
        Assert.NotEqual(hashed, LockKey.FromSensitive("other"));
        Assert.All(hashed, c => Assert.True(char.IsAsciiHexDigitLower(c)));
    }

    private static DistributedLock Compose(
        TestClock clock,
        ILockProvider provider,
        RecordingLogger<DistributedLock>? logger = null,
        LockingOptions? options = null
    ) => new(options ?? new LockingOptions { Namespace = "tests" }, provider, clock, logger);

    private sealed class MetricRecorder : IDisposable
    {
        private readonly MeterListener _listener = new();
        private readonly List<Measured> _measurements = [];
        private readonly Lock _gate = new();
        private readonly string _namespace;

        public MetricRecorder(string @namespace)
        {
            _namespace = @namespace;
            _listener.InstrumentPublished = (instrument, listener) =>
            {
                if (instrument.Meter.Name == LockingDiagnostics.MeterName)
                {
                    listener.EnableMeasurementEvents(instrument);
                }
            };
            _listener.SetMeasurementEventCallback<double>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.SetMeasurementEventCallback<long>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.SetMeasurementEventCallback<int>(
                (instrument, value, tags, _) => Record(instrument, value, tags)
            );
            _listener.Start();
        }

        public void Observe() => _listener.RecordObservableInstruments();

        public List<Measured> Measurements(string instrument)
        {
            lock (_gate)
            {
                return _measurements.Where(m => m.Instrument == instrument).ToList();
            }
        }

        public void Dispose() => _listener.Dispose();

        private void Record(
            Instrument instrument,
            double value,
            ReadOnlySpan<KeyValuePair<string, object?>> tags
        )
        {
            var dictionary = new Dictionary<string, object?>(StringComparer.Ordinal);
            foreach (var tag in tags)
            {
                dictionary[tag.Key] = tag.Value;
            }

            if (
                !dictionary.TryGetValue(LockingDiagnostics.NamespaceTag, out var ns)
                || !string.Equals(ns as string, _namespace, StringComparison.Ordinal)
            )
            {
                return;
            }

            lock (_gate)
            {
                _measurements.Add(new Measured(instrument.Name, value, dictionary));
            }
        }

        public sealed record Measured(
            string Instrument,
            double Value,
            IReadOnlyDictionary<string, object?> Tags
        );
    }
}
