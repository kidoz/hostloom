using HostLoom.Leadership;
using HostLoom.Locking;
using Microsoft.Extensions.Logging;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A lock that misbehaves in ways the lock contract rules out stands in for a defect: the
/// elector must log it, back off, and keep electing rather than end its loop silently.
/// </summary>
public sealed class LeaderElectorFaultTests
{
    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(2);

    [Fact]
    public async Task An_unexpected_exception_while_acquiring_is_logged_backed_off_and_survived()
    {
        var clock = new TestClock();
        await using var inner = Compose(clock);
        var faulty = new FaultingLock(inner, acquireFaults: 2);
        var logger = new RecordingLogger<LeaderElector>();
        await using var elector = new LeaderElector("scheduler", faulty, Options(), clock, logger);
        var changes = new List<LeadershipChange>();
        using var _ = elector.OnChange(changes.Add);

        await elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => Faults(logger) == 1);

        Assert.False(elector.IsLeader);
        Assert.Equal(LeadershipStatus.Candidate, elector.Status);
        var fault = logger.Entries.Single(entry =>
            entry.Event.Id == LeadershipEvents.LoopFaulted.Id
        );
        Assert.Equal(LogLevel.Error, fault.Level);
        Assert.IsType<InvalidOperationException>(fault.Exception);
        Assert.Contains("'scheduler'", fault.Message, StringComparison.Ordinal);

        // The loop is alive: it armed the back-off timer, faults once more on the next attempt,
        // and acquires on the one after that.
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 1);
        clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => Faults(logger) == 2);
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 1);
        clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => elector.IsLeader);

        Assert.Equal(1, elector.Term);
        Assert.Equal(3, faulty.Attempts);
        Assert.Equal([LeadershipChangeReason.Acquired], changes.Select(change => change.Reason));
        await elector.StopAsync(TestContext.Current.CancellationToken);
        Assert.Equal(LeadershipStatus.Stopped, elector.Status);
        Assert.Equal(2, Faults(logger));
    }

    [Fact]
    public async Task A_release_that_throws_still_records_the_transition_and_the_elector_continues()
    {
        var clock = new TestClock();
        await using var inner = Compose(clock);
        var faulty = new FaultingLock(inner, releaseFaults: 1);
        var logger = new RecordingLogger<LeaderElector>();
        await using var elector = new LeaderElector("scheduler", faulty, Options(), clock, logger);
        var changes = new List<LeadershipChange>();
        using var _ = elector.OnChange(changes.Add);
        await elector.StartAsync(TestContext.Current.CancellationToken);
        await SchedulingTests.WaitUntilAsync(() => elector.IsLeader);
        var token = elector.LeadershipToken;

        // Returns: the transition is recorded before the throwing release reaches the loop.
        await elector.ResignAsync(TestContext.Current.CancellationToken);

        Assert.True(token.IsCancellationRequested);
        Assert.False(elector.IsLeader);
        await SchedulingTests.WaitUntilAsync(() => Faults(logger) == 1);
        Assert.Equal(
            [LeadershipChangeReason.Acquired, LeadershipChangeReason.Resigned],
            changes.Select(change => change.Reason)
        );
        Assert.Equal(LeadershipStatus.Candidate, elector.Status);

        // One retry interval of back-off, then it leads again in a new term.
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers >= 1);
        clock.Advance(Retry);
        await SchedulingTests.WaitUntilAsync(() => elector.IsLeader);
        Assert.Equal(2, elector.Term);
        Assert.Equal(1, Faults(logger));
    }

    [Fact]
    public async Task Cadences_beyond_the_longest_schedulable_delay_are_rejected_at_validation()
    {
        var max = LeadershipOptions.MaxDelay;
        Assert.Equal(TimeSpan.FromMilliseconds(uint.MaxValue - 1), max);

        // The bound is the runtime's own: one millisecond more is refused by Task.Delay itself,
        // which is the failure the validation exists to make impossible inside the loop.
        var clock = new TestClock();
        using var cancellation = new CancellationTokenSource();
        var accepted = Task.Delay(max, clock, cancellation.Token);
        Assert.False(accepted.IsCompleted);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
        {
            _ = Task.Delay(max + TimeSpan.FromMilliseconds(1), clock, cancellation.Token);
        });
        await cancellation.CancelAsync();

        var renew = new LeadershipOptions
        {
            Lease = TimeSpan.FromDays(200),
            RenewInterval = max + TimeSpan.FromMilliseconds(1),
        }.Validate();
        Assert.Contains(
            renew,
            problem =>
                problem.Contains("Leadership:RenewInterval", StringComparison.Ordinal)
                && problem.Contains("longest wait", StringComparison.Ordinal)
        );
        var jitter = new LeadershipOptions
        {
            RetryInterval = max,
            RetryJitter = TimeSpan.FromMilliseconds(1),
        }.Validate();
        Assert.Contains(
            jitter,
            problem =>
                problem.Contains("Leadership:RetryJitter", StringComparison.Ordinal)
                && problem.Contains("longest wait", StringComparison.Ordinal)
        );
        Assert.Contains(
            new LeadershipOptions
            {
                RetryInterval = TimeSpan.MaxValue,
                RetryJitter = TimeSpan.MaxValue,
            }.Validate(),
            problem => problem.Contains("longest wait", StringComparison.Ordinal)
        );
        Assert.Contains(
            new LeadershipOptions { WhenUncoordinated = (UncoordinatedLeadership)7 }.Validate(),
            problem => problem.Contains("Leadership:WhenUncoordinated", StringComparison.Ordinal)
        );

        Assert.Empty(
            new LeadershipOptions { RetryInterval = max, RetryJitter = TimeSpan.Zero }.Validate()
        );
        Assert.Empty(
            new LeadershipOptions
            {
                RetryInterval = TimeSpan.FromSeconds(1),
                RetryJitter = max - TimeSpan.FromSeconds(1),
            }.Validate()
        );

        await using var locks = new DistributedLock(
            new LockingOptions { Namespace = "catalog", Enabled = false },
            provider: null
        );
        var refused = Assert.Throws<ArgumentException>(() =>
            new LeaderElector(
                "scheduler",
                locks,
                new LeadershipOptions { RetryInterval = max, RetryJitter = TimeSpan.FromSeconds(1) }
            )
        );
        Assert.Contains("longest wait", refused.Message, StringComparison.Ordinal);
    }

    private static int Faults(RecordingLogger<LeaderElector> logger) =>
        logger.Entries.Count(entry => entry.Event.Id == LeadershipEvents.LoopFaulted.Id);

    private static DistributedLock Compose(TestClock clock) =>
        new(
            new LockingOptions { Namespace = "catalog", DefaultLease = TimeSpan.FromSeconds(15) },
            new InMemoryLockProvider(clock),
            clock
        );

    private static LeadershipOptions Options() =>
        new()
        {
            Lease = TimeSpan.FromSeconds(15),
            RenewInterval = TimeSpan.FromSeconds(5),
            RetryInterval = Retry,
            RetryJitter = TimeSpan.Zero,
        };

    /// <summary>
    /// Throws a plain <see cref="InvalidOperationException"/> from the first acquisitions or
    /// releases, which no lock contract permits, and delegates everything else.
    /// </summary>
    private sealed class FaultingLock(
        IDistributedLock inner,
        int acquireFaults = 0,
        int releaseFaults = 0
    ) : IDistributedLock
    {
        private int _acquireFaults = acquireFaults;
        private int _releaseFaults = releaseFaults;
        private int _attempts;

        public int Attempts => Volatile.Read(ref _attempts);

        public bool IsCoordinated => inner.IsCoordinated;

        public ValueTask<T> ExecuteWithLockAsync<T>(
            string key,
            Func<CancellationToken, ValueTask<T>> action,
            LockOptions? options = null,
            CancellationToken cancellationToken = default
        ) => inner.ExecuteWithLockAsync(key, action, options, cancellationToken);

        public ValueTask ExecuteWithLockAsync(
            string key,
            Func<CancellationToken, ValueTask> action,
            LockOptions? options = null,
            CancellationToken cancellationToken = default
        ) => inner.ExecuteWithLockAsync(key, action, options, cancellationToken);

        public async ValueTask<ILockHandle?> TryAcquireAsync(
            string key,
            LockOptions? options = null,
            CancellationToken cancellationToken = default
        )
        {
            Interlocked.Increment(ref _attempts);
            if (Interlocked.Decrement(ref _acquireFaults) >= 0)
            {
                throw new InvalidOperationException("The lock misbehaved while acquiring.");
            }

            var handle = await inner.TryAcquireAsync(key, options, cancellationToken);
            return handle is null ? null : new FaultingHandle(this, handle);
        }

        private sealed class FaultingHandle(FaultingLock owner, ILockHandle inner) : ILockHandle
        {
            public string Key => inner.Key;

            public bool IsHeld => inner.IsHeld;

            public DateTimeOffset LeaseEnd => inner.LeaseEnd;

            public bool IsCoordinated => inner.IsCoordinated;

            public CancellationToken LostToken => inner.LostToken;

            public ValueTask<bool> ExtendAsync(
                TimeSpan lease,
                CancellationToken cancellationToken = default
            ) => inner.ExtendAsync(lease, cancellationToken);

            public async ValueTask DisposeAsync()
            {
                // Release the backend lease first, so the next attempt can succeed.
                await inner.DisposeAsync();
                if (Interlocked.Decrement(ref owner._releaseFaults) >= 0)
                {
                    throw new InvalidOperationException("The lock misbehaved while releasing.");
                }
            }
        }
    }
}
