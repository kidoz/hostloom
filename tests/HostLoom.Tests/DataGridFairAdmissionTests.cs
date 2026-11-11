using HostLoom.DataGrid.Runtime;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridFairAdmissionTests
{
    private static readonly int[] ExpectedClientTurns =
    [
        0,
        100,
        100,
        10,
        20,
        1,
        11,
        2,
        3,
        4,
        5,
        6,
        7,
    ];

    [Fact]
    public async Task ClientTurnsRotatePrincipalsPreserveTheirFifoAndRetainControlReservation()
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        var first = Guid.NewGuid();
        var second = Guid.NewGuid();
        var order = new List<int>();
        for (var i = 0; i < 8; i++)
            Assert.True(queue.TryWrite(Work(first, i, order)));
        for (var i = 10; i < 12; i++)
            Assert.True(queue.TryWrite(Work(second, i, order)));
        Assert.True(queue.TryWrite(Work(Guid.Empty, 20, order)));
        for (var i = 0; i < 10; i++)
            Assert.True(queue.TryWrite(new(() => order.Add(100), _ => { }, 1, true)));
        for (var i = 0; i < 21; i++)
            (await queue.ReadAsync(TestContext.Current.CancellationToken)).Action();
        Assert.Equal(Enumerable.Repeat(100, 8).Concat(ExpectedClientTurns), order);
        Assert.Empty(queue.Complete());
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public async Task PrincipalCountAndByteRefusalsLeaveOtherPrincipalsAdmissibleAndRecover(
        bool bytes
    )
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        var principal = Guid.NewGuid();
        var work = new DataGridRuntimeWork(
            () => { },
            _ => { },
            bytes ? (int)DataGridRuntimeQueue.PrincipalByteLimit : 1,
            false,
            principal
        );
        var count = bytes ? 1 : DataGridRuntimeQueue.PrincipalLimit;
        for (var i = 0; i < count; i++)
            Assert.True(queue.TryWrite(work));
        Assert.False(queue.TryWrite(work, out var refusal));
        Assert.Equal(DataGridRuntimeAdmissionFailure.Capacity, refusal);
        Assert.True(queue.TryWrite(work with { Principal = Guid.NewGuid() }));
        Assert.Same(work, await queue.ReadAsync(TestContext.Current.CancellationToken));
        Assert.True(queue.TryWrite(work));
        Assert.Equal(count + 1, queue.Complete().Length);
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.queue.rejected"
                && m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.ReasonTag) is "principal"
        );
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact]
    public async Task PrincipalChurnAndGlobalSaturationKeepControlCapacityAndDrainExactlyOnce()
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        var drained = 0;
        for (var i = 0; i < 512; i++)
        {
            var work = new DataGridRuntimeWork(() => { }, _ => drained++, 1, false, Guid.NewGuid());
            Assert.True(queue.TryWrite(work));
            Assert.Same(work, await queue.ReadAsync(TestContext.Current.CancellationToken));
        }
        for (var i = 0; i < 16; i++)
        {
            var principal = Guid.NewGuid();
            for (var j = 0; j < 8; j++)
                Assert.True(queue.TryWrite(new(() => { }, _ => drained++, 1, false, principal)));
        }
        Assert.False(
            queue.TryWrite(
                new(() => { }, _ => drained++, 1, false, Guid.NewGuid()),
                out var refusal
            )
        );
        Assert.Equal(DataGridRuntimeAdmissionFailure.Capacity, refusal);
        Assert.True(queue.TryWrite(new(() => { }, _ => drained++, 1, true)));
        foreach (var work in queue.Complete())
            work.Reject(new IOException("Stopped."));
        Assert.Equal(129, drained);
        Assert.Empty(queue.Complete());
        Assert.False(
            queue.TryWrite(new(() => { }, _ => { }, 1, false, Guid.NewGuid()), out var stopped)
        );
        Assert.Equal(DataGridRuntimeAdmissionFailure.Stopped, stopped);
        capture.AssertBalanced();
    }

    [Fact(Timeout = 20000)]
    public async Task ConcurrentSamePrincipalCannotExceedItsShareOrConsumeOtherPrincipalTurn()
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        var principal = Guid.NewGuid();
        var work = new DataGridRuntimeWork(() => { }, _ => { }, 1, false, principal);
        var accepted = 0;
        await Task.WhenAll(
            Enumerable
                .Range(0, 32)
                .Select(_ =>
                    Task.Run(
                        () =>
                        {
                            for (var i = 0; i < 32; i++)
                                if (queue.TryWrite(work))
                                    Interlocked.Increment(ref accepted);
                        },
                        TestContext.Current.CancellationToken
                    )
                )
        );
        Assert.Equal(8, accepted);
        var other = work with { Principal = Guid.NewGuid() };
        Assert.True(queue.TryWrite(other));
        Assert.Same(work, await queue.ReadAsync(TestContext.Current.CancellationToken));
        Assert.True(queue.TryWrite(work));
        Assert.Same(other, await queue.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Equal(8, queue.Complete().Length);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact]
    public async Task NewAndReturningPrincipalsJoinTailWithoutOvertakingExistingTurn()
    {
        using var queue = new DataGridRuntimeQueue();
        var a = new DataGridRuntimeWork(() => { }, _ => { }, 1, false, Guid.NewGuid());
        var b = a with { Principal = Guid.NewGuid() };
        var c = a with { Principal = Guid.NewGuid() };
        Assert.True(queue.TryWrite(a));
        Assert.True(queue.TryWrite(a));
        Assert.True(queue.TryWrite(b));
        Assert.Same(a, await queue.ReadAsync(TestContext.Current.CancellationToken));
        Assert.True(queue.TryWrite(c));
        Assert.Same(b, await queue.ReadAsync(TestContext.Current.CancellationToken));
        Assert.True(queue.TryWrite(b));
        Assert.Same(a, await queue.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Same(c, await queue.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Same(b, await queue.ReadAsync(TestContext.Current.CancellationToken));
        Assert.Empty(queue.Complete());
    }

    private static DataGridRuntimeWork Work(Guid principal, int number, List<int> order) =>
        new(() => order.Add(number), _ => { }, 1, false, principal);
}
