using System.Collections.Concurrent;
using HostLoom.DataGrid.Runtime;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridControlBudgetTests
{
    [Fact]
    public async Task OneSnapshotImageIsBoundedIndependentlyAndStopKeepsItsChargeUntilRelease()
    {
        var images = new DataGridSnapshotInstallBudget();
        Assert.Null(images.TryAcquire(0));
        Assert.Null(images.TryAcquire(DataGridSnapshotInstallBudget.ByteLimit + 1));
        var leases = new ConcurrentBag<DataGridSnapshotInstallBudget.Lease>();
        await Task.WhenAll(
            Enumerable
                .Range(0, 32)
                .Select(_ =>
                    Task.Run(
                        () =>
                        {
                            if (
                                images.TryAcquire(DataGridSnapshotInstallBudget.ByteLimit) is
                                { } lease
                            )
                                leases.Add(lease);
                        },
                        TestContext.Current.CancellationToken
                    )
                )
        );
        var held = Assert.Single(leases);
        Assert.Equal(1, images.Progress.Active);
        Assert.Equal(DataGridSnapshotInstallBudget.ByteLimit, images.Progress.Bytes);
        var controls = new DataGridRuntimeOperationBudget(control: true);
        using var control = controls.TryAcquire(
            Guid.Empty,
            (int)DataGridRuntimeQueue.ByteLimit,
            out _
        );
        Assert.NotNull(control);
        images.Complete();
        Assert.Null(images.TryAcquire(1));
        Assert.True(images.Progress.Stopped);
        Assert.Equal(1, images.Progress.Active);
        await Task.WhenAll(
            Enumerable
                .Range(0, 32)
                .Select(_ => Task.Run(held.Dispose, TestContext.Current.CancellationToken))
        );
        Assert.Equal(0, images.Progress.Active);
        Assert.Equal(0, images.Progress.Bytes);
        Assert.Null(images.TryAcquire(1));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RetainedControlCapacityIsIndependentFromClientCapacityAndStopDoesNotRefund(
        bool bytes
    )
    {
        using var capture = new DataGridMeterCapture();
        var controls = new DataGridRuntimeOperationBudget(control: true);
        var clients = new DataGridRuntimeOperationBudget();
        var held = new List<DataGridRuntimeOperationBudget.Lease>();
        try
        {
            for (var i = 0; i < (bytes ? 1 : 128); i++)
                held.Add(
                    Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                        clients.TryAcquire(
                            Guid.Empty,
                            bytes ? (int)DataGridRuntimeQueue.ByteLimit : 1,
                            out _
                        )
                    )
                );
            for (var i = 0; i < (bytes ? 1 : 256); i++)
                held.Add(
                    Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                        controls.TryAcquire(
                            Guid.Empty,
                            bytes ? (int)DataGridRuntimeQueue.ByteLimit : 1,
                            out _
                        )
                    )
                );
            Assert.Null(controls.TryAcquire(Guid.Empty, 1, out var full));
            Assert.Equal(DataGridRuntimeAdmissionFailure.Capacity, full);
            Assert.Null(clients.TryAcquire(Guid.Empty, 1, out _));
            Assert.Equal(bytes ? 1 : 256, capture.Sum("runtime.control.active"));
            Assert.Equal(
                bytes ? DataGridRuntimeQueue.ByteLimit : 256,
                capture.Sum("runtime.control.bytes")
            );
            Assert.All(
                capture.Measurements.Where(m =>
                    m.Name == "hostloom.datagrid.runtime.control.active"
                ),
                m => Assert.Equal("{item}", m.Unit)
            );
            Assert.All(
                capture.Measurements.Where(m =>
                    m.Name == "hostloom.datagrid.runtime.control.bytes"
                ),
                m => Assert.Equal("By", m.Unit)
            );
            controls.Complete();
            Assert.Null(controls.TryAcquire(Guid.Empty, 1, out var stopped));
            Assert.Equal(DataGridRuntimeAdmissionFailure.Stopped, stopped);
            Assert.Equal(bytes ? 1 : 256, capture.Sum("runtime.control.active"));
        }
        finally
        {
            foreach (var lease in held)
                lease.Dispose();
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 20000)]
    public async Task ConcurrentControlReservationsAndRepeatedReleaseRespectCombinedLimit()
    {
        using var capture = new DataGridMeterCapture();
        var controls = new DataGridRuntimeOperationBudget(control: true);
        var held = new ConcurrentBag<DataGridRuntimeOperationBudget.Lease>();
        try
        {
            await Task.WhenAll(
                Enumerable
                    .Range(0, 32)
                    .Select(_ =>
                        Task.Run(
                            () =>
                            {
                                for (var i = 0; i < 32; i++)
                                    if (
                                        controls.TryAcquire(Guid.Empty, 1024, out var failure) is
                                        { } lease
                                    )
                                        held.Add(lease);
                            },
                            TestContext.Current.CancellationToken
                        )
                    )
            );
            Assert.Equal(256, held.Count);
            controls.Complete();
            Assert.Equal(256, capture.Sum("runtime.control.active"));
            await Task.WhenAll(
                Enumerable
                    .Range(0, 32)
                    .Select(_ =>
                        Task.Run(
                            () =>
                            {
                                foreach (var lease in held)
                                    lease.Dispose();
                            },
                            TestContext.Current.CancellationToken
                        )
                    )
            );
        }
        finally
        {
            foreach (var lease in held)
                lease.Dispose();
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }
}
