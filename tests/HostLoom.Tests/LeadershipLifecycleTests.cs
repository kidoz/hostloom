using HostLoom.Leadership;
using HostLoom.Leadership.Testing;
using HostLoom.Locking;
using Microsoft.Extensions.Hosting;
using Xunit;

namespace HostLoom.Tests;

public sealed class LeadershipLifecycleTests
{
    private static readonly TimeSpan Bound = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Lease = TimeSpan.FromSeconds(15);
    private static readonly TimeSpan Renew = TimeSpan.FromSeconds(5);
    private static readonly TimeSpan Retry = TimeSpan.FromSeconds(2);

    [Theory]
    [InlineData(LeadershipChangeReason.Lost)]
    [InlineData(LeadershipChangeReason.Resigned)]
    [InlineData(LeadershipChangeReason.Stopped)]
    public async Task A_pending_renewal_is_cancelled_when_leadership_ends(
        LeadershipChangeReason reason
    )
    {
        var clock = new TestClock();
        var backend = new InMemoryLockProvider(clock);
        var provider = new PendingRenewalProvider(backend);
        await using var locks = new DistributedLock(
            new() { Namespace = "billing" },
            provider,
            clock
        );
        await using var elector = new LeaderElector("scheduler", locks, Options(), clock);
        var ended = new TaskCompletionSource<LeadershipChange>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        using var listener = elector.OnChange(change =>
        {
            if (!change.IsLeader)
            {
                ended.TrySetResult(change);
            }
        });
        await elector.StartAsync(TestContext.Current.CancellationToken);
        await elector
            .WaitForLeadershipAsync(TestContext.Current.CancellationToken)
            .AsTask()
            .WaitAsync(Bound, TestContext.Current.CancellationToken);
        var leadershipToken = elector.LeadershipToken;
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 3);
        clock.Advance(Renew);
        await provider.Entered.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);

        await EndAsync(elector, clock, reason, Lease - Renew);
        var change = await ended.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);

        Assert.Equal(reason, change.Reason);
        Assert.True(provider.RenewalToken.IsCancellationRequested);
        Assert.True(leadershipToken.IsCancellationRequested);
        Assert.False(elector.IsLeader);
        Assert.Equal(0, backend.Count);

        // The old leader has stepped down without waiting for the backend to answer renewal.
        await using var followerLocks = new DistributedLock(
            new() { Namespace = "billing" },
            backend,
            clock
        );
        await using var follower = new LeaderElector("scheduler", followerLocks, Options(), clock);
        await follower.StartAsync(TestContext.Current.CancellationToken);
        await follower
            .WaitForLeadershipAsync(TestContext.Current.CancellationToken)
            .AsTask()
            .WaitAsync(Bound, TestContext.Current.CancellationToken);
        Assert.True(follower.IsLeader);
        Assert.False(elector.IsLeader);
    }

    [Theory]
    [InlineData(LeadershipChangeReason.Lost)]
    [InlineData(LeadershipChangeReason.Resigned)]
    [InlineData(LeadershipChangeReason.Stopped)]
    public async Task A_throwing_cancellation_callback_does_not_prevent_release_or_reacquisition(
        LeadershipChangeReason reason
    )
    {
        var clock = new TestClock();
        var backend = new InMemoryLockProvider(clock);
        await using var locks = new DistributedLock(
            new() { Namespace = "billing" },
            backend,
            clock
        );
        await using var elector = new LeaderElector("scheduler", locks, Options(), clock);
        var ended = new TaskCompletionSource<LeadershipChange>(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        using var listener = elector.OnChange(change =>
        {
            if (!change.IsLeader)
            {
                ended.TrySetResult(change);
            }
        });
        await elector.StartAsync(TestContext.Current.CancellationToken);
        await elector
            .WaitForLeadershipAsync(TestContext.Current.CancellationToken)
            .AsTask()
            .WaitAsync(Bound, TestContext.Current.CancellationToken);
        var leadershipToken = elector.LeadershipToken;
        using var callback = leadershipToken.Register(() =>
            throw new InvalidOperationException("Consumer cleanup failed.")
        );
        await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 3);

        await EndAsync(elector, clock, reason, Lease);
        var change = await ended.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);

        Assert.Equal(reason, change.Reason);
        Assert.True(leadershipToken.IsCancellationRequested);
        Assert.False(elector.IsLeader);
        Assert.Equal(0, backend.Count);
        if (reason != LeadershipChangeReason.Stopped)
        {
            // Only the candidate retry remains after cleanup. The same elector must still run.
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers == 1);
            clock.Advance(Retry);
            await elector
                .WaitForLeadershipAsync(TestContext.Current.CancellationToken)
                .AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
            Assert.Equal(2, elector.Term);
            Assert.False(elector.LeadershipToken.IsCancellationRequested);
        }

        await elector
            .StopAsync(TestContext.Current.CancellationToken)
            .WaitAsync(Bound, TestContext.Current.CancellationToken);
        Assert.Equal(LeadershipStatus.Stopped, elector.Status);
        Assert.Equal(0, backend.Count);
        await elector
            .DisposeAsync()
            .AsTask()
            .WaitAsync(Bound, TestContext.Current.CancellationToken);
    }

    [Fact]
    public async Task The_documented_consumer_resumes_even_if_a_new_term_starts_during_cleanup()
    {
        using var leadership = new ManualLeadership("reconciler");
        var firstRun = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var cleanupStarted = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var finishCleanup = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var secondRun = new TaskCompletionSource(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        var runs = 0;
        using var consumer = new ReconcilerLoop(
            leadership,
            async token =>
            {
                var run = Interlocked.Increment(ref runs);
                (run == 1 ? firstRun : secondRun).TrySetResult();
                try
                {
                    await Task.Delay(Timeout.InfiniteTimeSpan, token);
                }
                finally
                {
                    if (run == 1)
                    {
                        cleanupStarted.TrySetResult();
                        await finishCleanup.Task.WaitAsync(
                            Bound,
                            TestContext.Current.CancellationToken
                        );
                    }
                }
            }
        );

        await consumer.StartAsync(TestContext.Current.CancellationToken);
        try
        {
            leadership.Acquire();
            await firstRun.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            leadership.Lose();
            await cleanupStarted.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            leadership.Acquire();
            Assert.False(leadership.LeadershipToken.IsCancellationRequested);
            finishCleanup.TrySetResult();

            await secondRun.Task.WaitAsync(Bound, TestContext.Current.CancellationToken);
            Assert.Equal(2, Volatile.Read(ref runs));
            Assert.False(consumer.ExecuteTask!.IsCompleted);
        }
        finally
        {
            finishCleanup.TrySetResult();
            await consumer
                .StopAsync(TestContext.Current.CancellationToken)
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
    }

    private static LeadershipOptions Options() =>
        new()
        {
            Lease = Lease,
            RenewInterval = Renew,
            RetryInterval = Retry,
            RetryJitter = TimeSpan.Zero,
        };

    private static async Task EndAsync(
        LeaderElector elector,
        TestClock clock,
        LeadershipChangeReason reason,
        TimeSpan remaining
    )
    {
        if (reason == LeadershipChangeReason.Lost)
        {
            clock.Advance(remaining);
        }
        else if (reason == LeadershipChangeReason.Resigned)
        {
            await elector
                .ResignAsync(TestContext.Current.CancellationToken)
                .AsTask()
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
        else
        {
            await elector
                .StopAsync(TestContext.Current.CancellationToken)
                .WaitAsync(Bound, TestContext.Current.CancellationToken);
        }
    }

    private sealed class PendingRenewalProvider(ILockProvider inner) : ILockProvider
    {
        public TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public CancellationToken RenewalToken { get; private set; }

        public ValueTask<bool> TryAcquireAsync(
            string key,
            string owner,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        ) => inner.TryAcquireAsync(key, owner, lease, cancellationToken);

        public ValueTask<bool> ReleaseAsync(
            string key,
            string owner,
            CancellationToken cancellationToken = default
        ) => inner.ReleaseAsync(key, owner, cancellationToken);

        public async ValueTask<bool> ExtendAsync(
            string key,
            string owner,
            TimeSpan lease,
            CancellationToken cancellationToken = default
        )
        {
            RenewalToken = cancellationToken;
            Entered.TrySetResult();
            await Task.Delay(Timeout.InfiniteTimeSpan, cancellationToken);
            return false;
        }
    }

    // The consumer loop shown in the leadership READMEs and how-to. The delegate represents
    // application work; the test delays its cleanup until a new leadership token is published.
    private sealed class ReconcilerLoop(
        ILeadership leadership,
        Func<CancellationToken, Task> runWhileLeaderAsync
    ) : BackgroundService
    {
        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                await leadership.WaitForLeadershipAsync(stoppingToken);
                var leadershipToken = leadership.LeadershipToken;
                using var linked = CancellationTokenSource.CreateLinkedTokenSource(
                    stoppingToken,
                    leadershipToken
                );
                try
                {
                    await runWhileLeaderAsync(linked.Token);
                }
                catch (OperationCanceledException) when (leadershipToken.IsCancellationRequested)
                {
                    // Lost this term; wait to lead again.
                }
            }
        }
    }
}
