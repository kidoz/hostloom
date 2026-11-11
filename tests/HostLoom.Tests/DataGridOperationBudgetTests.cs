using System.Collections.Concurrent;
using HostLoom.DataGrid.Runtime;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridOperationBudgetTests
{
    [Fact]
    public async Task DequeueDoesNotRefundPrincipalShareBeforeOwnerCompletion()
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        var budget = new DataGridRuntimeOperationBudget();
        var principal = Guid.NewGuid();
        var held = new List<DataGridRuntimeOperationBudget.Lease>();
        try
        {
            for (var i = 0; i < 8; i++)
            {
                var lease = Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                    budget.TryAcquire(principal, 1024, out _)
                );
                held.Add(lease);
                Assert.True(
                    queue.TryWrite(new(() => { }, _ => lease.Dispose(), 1024, false, principal))
                );
            }
            for (var i = 0; i < 8; i++)
                (await queue.ReadAsync(TestContext.Current.CancellationToken)).Action();
            Assert.Equal(0, capture.Sum("runtime.queue.items", "client"));
            Assert.Equal(8, capture.Sum("runtime.admission.active"));
            Assert.Equal(8192, capture.Sum("runtime.admission.bytes"));
            Assert.All(
                capture.Measurements.Where(m =>
                    m.Name == "hostloom.datagrid.runtime.admission.active"
                ),
                m => Assert.Equal("{operation}", m.Unit)
            );
            Assert.All(
                capture.Measurements.Where(m =>
                    m.Name == "hostloom.datagrid.runtime.admission.bytes"
                ),
                m => Assert.Equal("By", m.Unit)
            );
            Assert.Null(budget.TryAcquire(principal, 1, out var failure));
            Assert.Equal(DataGridRuntimeAdmissionFailure.Capacity, failure);
            using var other = Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                budget.TryAcquire(Guid.NewGuid(), 1, out _)
            );
            held[0].Dispose();
            using var reused = Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                budget.TryAcquire(principal, 1024, out _)
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void PrincipalLimitsCoverAllHeldWorkAndRecoverAfterIdempotentRelease(bool bytes)
    {
        using var capture = new DataGridMeterCapture();
        var budget = new DataGridRuntimeOperationBudget();
        var principal = Guid.NewGuid();
        var held = new List<DataGridRuntimeOperationBudget.Lease>();
        var count = bytes ? 4 : 8;
        var charge = bytes ? 1024 * 1024 : 1;
        try
        {
            for (var i = 0; i < count; i++)
                held.Add(
                    Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                        budget.TryAcquire(principal, charge, out _)
                    )
                );
            Assert.Null(budget.TryAcquire(principal, charge, out var failure));
            Assert.Equal(DataGridRuntimeAdmissionFailure.Capacity, failure);
            Assert.Equal(count, capture.Sum("runtime.admission.active"));
            Assert.Equal((long)count * charge, capture.Sum("runtime.admission.bytes"));
            using var other = Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                budget.TryAcquire(Guid.NewGuid(), charge, out _)
            );
            held[0].Dispose();
            held[0].Dispose();
            using var reused = Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                budget.TryAcquire(principal, charge, out _)
            );
            Assert.Contains(
                capture.Measurements,
                m =>
                    m.Name == "hostloom.datagrid.runtime.queue.rejected"
                    && m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.ReasonTag) is "principal"
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

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GlobalLimitsRemainBindingAcrossPrincipalsAndTrustedLocalWork(bool bytes)
    {
        using var capture = new DataGridMeterCapture();
        var budget = new DataGridRuntimeOperationBudget();
        var held = new List<DataGridRuntimeOperationBudget.Lease>();
        try
        {
            for (var i = 0; i < (bytes ? 4 : 128); i++)
                held.Add(
                    Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                        budget.TryAcquire(
                            bytes ? Guid.NewGuid() : Guid.Empty,
                            bytes ? (int)DataGridRuntimeQueue.PrincipalByteLimit : 1,
                            out _
                        )
                    )
                );
            Assert.Null(budget.TryAcquire(Guid.NewGuid(), 1, out var failure));
            Assert.Equal(DataGridRuntimeAdmissionFailure.Capacity, failure);
            Assert.Contains(
                capture.Measurements,
                m =>
                    m.Name == "hostloom.datagrid.runtime.queue.rejected"
                    && Equals(
                        m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.ReasonTag),
                        bytes ? "bytes" : "items"
                    )
            );
            Assert.Null(budget.TryAcquire(Guid.NewGuid(), -1, out _));
        }
        finally
        {
            foreach (var lease in held)
                lease.Dispose();
        }
        capture.AssertBalanced();
    }

    [Fact(Timeout = 20000)]
    public async Task ConcurrentAcquireAndRepeatedReleaseCannotExceedPrincipalShareOrUnderflow()
    {
        using var capture = new DataGridMeterCapture();
        var budget = new DataGridRuntimeOperationBudget();
        var principal = Guid.NewGuid();
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
                                        budget.TryAcquire(principal, 1024, out var failure) is
                                        { } lease
                                    )
                                        held.Add(lease);
                            },
                            TestContext.Current.CancellationToken
                        )
                    )
            );
            Assert.Equal(8, held.Count);
            Assert.Equal(8, capture.Sum("runtime.admission.active"));
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

    [Fact]
    public void StopRefusesNewChargesWithoutReleasingOwnedWorkAndChurnLeavesNoPermanentIdentityCharge()
    {
        using var capture = new DataGridMeterCapture();
        var budget = new DataGridRuntimeOperationBudget();
        for (var i = 0; i < 512; i++)
        {
            using var lease = Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                budget.TryAcquire(Guid.NewGuid(), 17, out _)
            );
        }
        using (
            var held = Assert.IsType<DataGridRuntimeOperationBudget.Lease>(
                budget.TryAcquire(Guid.NewGuid(), 19, out _)
            )
        )
        {
            Assert.Null(budget.TryAcquire(Guid.NewGuid(), 1, out var stopping, stopping: true));
            Assert.Equal(DataGridRuntimeAdmissionFailure.Stopped, stopping);
            budget.Complete();
            budget.Complete();
            Assert.Null(budget.TryAcquire(Guid.NewGuid(), 1, out var stopped));
            Assert.Equal(DataGridRuntimeAdmissionFailure.Stopped, stopped);
            Assert.Equal(1, capture.Sum("runtime.admission.active"));
            Assert.Equal(19, capture.Sum("runtime.admission.bytes"));
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }
}
