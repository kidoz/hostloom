using HostLoom.DataGrid.Runtime;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridOverloadTests
{
    [Fact]
    public void QueueReportsDefiniteCapacityAndStoppedRefusalsWithoutExecutingWork()
    {
        using var queue = new DataGridRuntimeQueue();
        var executed = false;
        var work = new DataGridRuntimeWork(
            () => executed = true,
            _ => { },
            (int)DataGridRuntimeQueue.ByteLimit,
            false
        );
        Assert.True(queue.TryWrite(work, out var accepted));
        Assert.Equal(DataGridRuntimeAdmissionFailure.None, accepted);
        Assert.False(queue.TryWrite(work, out var capacity));
        Assert.Equal(DataGridRuntimeAdmissionFailure.Capacity, capacity);
        Assert.Single(queue.Complete());
        Assert.False(queue.TryWrite(work, out var stopped));
        Assert.Equal(DataGridRuntimeAdmissionFailure.Stopped, stopped);
        Assert.False(executed);
    }
}
