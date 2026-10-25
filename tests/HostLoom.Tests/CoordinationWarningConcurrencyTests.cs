using HostLoom.Leadership;
using HostLoom.Locking;
using HostLoom.NodeLeasing;
using Xunit;

namespace HostLoom.Tests;

public sealed class CoordinationWarningConcurrencyTests
{
    [Theory]
    [InlineData(UncoordinatedLeadership.Follow)]
    [InlineData(UncoordinatedLeadership.Lead)]
    public async Task Concurrent_starts_and_acquisition_warn_once_per_leader_elector(
        UncoordinatedLeadership posture
    )
    {
        for (var round = 0; round < 16; round++)
        {
            var clock = new TestClock();
            var logger = new RecordingLogger<LeaderElector>();
            await using var locks = Disabled();
            await using var elector = new LeaderElector(
                "scheduler",
                locks,
                new LeadershipOptions { WhenUncoordinated = posture, RetryJitter = TimeSpan.Zero },
                clock,
                logger
            );
            await StartTogetherAsync(() =>
                elector.StartAsync(TestContext.Current.CancellationToken)
            );
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers > 0);
            Assert.Equal(posture == UncoordinatedLeadership.Lead, elector.IsLeader);
            await elector.StopAsync(TestContext.Current.CancellationToken);
            Assert.Single(logger.Entries, entry => entry.Event == LeadershipEvents.Uncoordinated);
            Assert.Single(logger.Entries, entry => entry.Event == LeadershipEvents.Started);
        }
    }

    [Theory]
    [InlineData(UncoordinatedNodeAssignment.None)]
    [InlineData(UncoordinatedNodeAssignment.AssignZero)]
    public async Task Concurrent_starts_and_acquisition_warn_once_per_node_elector(
        UncoordinatedNodeAssignment posture
    )
    {
        for (var round = 0; round < 16; round++)
        {
            var clock = new TestClock();
            var logger = new RecordingLogger<NodeIdElector>();
            await using var locks = Disabled();
            await using var elector = new NodeIdElector(
                "orders-workers",
                locks,
                new NodeLeasingOptions { WhenUncoordinated = posture, RetryJitter = TimeSpan.Zero },
                clock,
                logger
            );
            await StartTogetherAsync(() =>
                elector.StartAsync(TestContext.Current.CancellationToken)
            );
            await SchedulingTests.WaitUntilAsync(() => clock.PendingTimers > 0);
            Assert.Equal(
                posture == UncoordinatedNodeAssignment.AssignZero ? 0 : (int?)null,
                elector.NodeId
            );
            await elector.StopAsync(TestContext.Current.CancellationToken);
            Assert.Single(logger.Entries, entry => entry.Event == NodeLeasingEvents.Uncoordinated);
            Assert.Single(logger.Entries, entry => entry.Event == NodeLeasingEvents.Started);
        }
    }

    private static DistributedLock Disabled() =>
        new(new LockingOptions { Namespace = "catalog", Enabled = false }, provider: null);

    private static async Task StartTogetherAsync(Func<Task> start)
    {
        var ready = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var waiting = 0;
        var starts = Enumerable
            .Range(0, 16)
            .Select(_ =>
                Task.Run(
                    async () =>
                    {
                        if (Interlocked.Increment(ref waiting) == 16)
                            ready.TrySetResult();
                        await release.Task;
                        await start();
                    },
                    TestContext.Current.CancellationToken
                )
            )
            .ToArray();
        try
        {
            await ready.Task.WaitAsync(
                TimeSpan.FromSeconds(10),
                TestContext.Current.CancellationToken
            );
        }
        finally
        {
            release.TrySetResult();
            await Task.WhenAll(starts)
                .WaitAsync(TimeSpan.FromSeconds(10), TestContext.Current.CancellationToken);
        }
    }
}
