using System.Collections.Concurrent;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridTransportDiagnosticsTests
{
    [Theory]
    [InlineData(false, 0, 16)]
    [InlineData(false, 1, 4)]
    [InlineData(false, 2, 2)]
    [InlineData(true, 0, 112)]
    [InlineData(true, 1, 12)]
    [InlineData(true, 4, 12)]
    [InlineData(false, 5, 1)]
    public void EveryReservationBudgetRefusesOverflowAndBalancesIdempotentRelease(
        bool client,
        int resourceId,
        int capacity
    )
    {
        using var capture = new DataGridMeterCapture();
        using var budget = new SemaphoreSlim(capacity, capacity);
        var role = client ? DataGridTransportRole.Client : DataGridTransportRole.Peer;
        var resource = (DataGridTransportResource)resourceId;
        var bytes =
            resource == DataGridTransportResource.Request ? 2L * 1024 * 1024
            : resource == DataGridTransportResource.Snapshot ? 8192
            : 0;
        var held = Enumerable
            .Range(0, capacity)
            .Select(_ =>
                DataGridTransportLease.Acquire(
                    budget,
                    role,
                    DataGridTransportDirection.Inbound,
                    resource,
                    bytes,
                    TestContext.Current.CancellationToken
                )
            )
            .ToArray();
        try
        {
            Assert.Null(
                DataGridTransportLease.TryAcquire(
                    budget,
                    role,
                    DataGridTransportDirection.Inbound,
                    resource,
                    token: TestContext.Current.CancellationToken
                )
            );
            Assert.Equal(0, budget.CurrentCount);
            Assert.Equal(
                capacity,
                capture
                    .Measurements.Where(m => m.Name.EndsWith(".active", StringComparison.Ordinal))
                    .Sum(m => m.Value)
            );
        }
        finally
        {
            foreach (var lease in held)
            {
                lease.Dispose();
                lease.Dispose();
            }
        }
        Assert.Equal(capacity, budget.CurrentCount);
        Assert.Equal(1, capture.Sum("transport.admission.rejected"));
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory]
    [InlineData(false, 24)]
    [InlineData(true, 8)]
    public void HandshakeBurstAndRefillAreBoundedAcrossBothDirections(bool client, int capacity)
    {
        using var capture = new DataGridMeterCapture();
        var clock = new Clock();
        var budget = new DataGridHandshakeRate(
            capacity,
            client ? DataGridTransportRole.Client : DataGridTransportRole.Peer,
            clock
        );
        for (var i = 0; i < capacity; i++)
            Assert.True(budget.TryAcquire((DataGridTransportDirection)(i % 2)));
        Assert.False(budget.TryAcquire(DataGridTransportDirection.Inbound));
        clock.Advance(TimeSpan.FromTicks(TimeSpan.TicksPerSecond / capacity + 1));
        Assert.True(budget.TryAcquire(DataGridTransportDirection.Outbound));
        Assert.False(budget.TryAcquire(DataGridTransportDirection.Inbound));
        clock.Advance(TimeSpan.FromHours(1));
        for (var i = 0; i < capacity; i++)
            Assert.True(budget.TryAcquire(DataGridTransportDirection.Inbound));
        Assert.False(budget.TryAcquire(DataGridTransportDirection.Inbound));
        Assert.Equal(3, capture.Sum("transport.admission.rejected"));
        Assert.All(
            capture.Measurements,
            m =>
                Assert.Equal(
                    "rate",
                    m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ReasonTag)
                )
        );
        capture.AssertPrivate();
    }

    [Fact]
    public void CancellationBeforeReservationDoesNotConsumeCapacityOrEmitRefusal()
    {
        using var capture = new DataGridMeterCapture();
        using var budget = new SemaphoreSlim(1, 1);
        using var canceled = new CancellationTokenSource();
        canceled.Cancel();
        Assert.Throws<OperationCanceledException>(() =>
            DataGridTransportLease.Acquire(
                budget,
                DataGridTransportRole.Peer,
                DataGridTransportDirection.Outbound,
                DataGridTransportResource.Connection,
                token: canceled.Token
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DataGridTransportLease.Acquire(
                budget,
                DataGridTransportRole.Peer,
                DataGridTransportDirection.Outbound,
                DataGridTransportResource.Connection,
                -1,
                TestContext.Current.CancellationToken
            )
        );
        Assert.Equal(1, budget.CurrentCount);
        Assert.Empty(capture.Measurements);
    }

    [Fact(Timeout = 20000)]
    public async Task ConcurrentReservationsCannotExceedBudgetOrReleaseTwice()
    {
        using var capture = new DataGridMeterCapture();
        using var budget = new SemaphoreSlim(12, 12);
        var held = new ConcurrentBag<DataGridTransportLease>();
        await Task.WhenAll(
            Enumerable
                .Range(0, 32)
                .Select(_ =>
                    Task.Run(
                        () =>
                        {
                            var lease = DataGridTransportLease.TryAcquire(
                                budget,
                                DataGridTransportRole.Client,
                                DataGridTransportDirection.Inbound,
                                DataGridTransportResource.Request,
                                2L * 1024 * 1024,
                                TestContext.Current.CancellationToken
                            );
                            if (lease is not null)
                                held.Add(lease);
                        },
                        TestContext.Current.CancellationToken
                    )
                )
        );
        Assert.Equal(12, held.Count);
        Assert.Equal(20, capture.Sum("transport.admission.rejected"));
        await Task.WhenAll(
            held.Select(lease =>
                Task.Run(
                    () =>
                    {
                        lease.Dispose();
                        lease.Dispose();
                    },
                    TestContext.Current.CancellationToken
                )
            )
        );
        Assert.Equal(12, budget.CurrentCount);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact]
    public void PrincipalBudgetEnforcesBothIdentityAndGlobalBoundsWithoutIdentityLabels()
    {
        using var capture = new DataGridMeterCapture();
        var pool = new DataGridPrincipalAdmission();
        var principal = Guid.NewGuid();
        var held = Enumerable
            .Range(0, 8)
            .Select(_ => pool.Acquire(principal, TestContext.Current.CancellationToken))
            .ToList();
        try
        {
            Assert.Throws<DataGridTransportAdmissionException>(() =>
                pool.Acquire(principal, TestContext.Current.CancellationToken)
            );
            for (var i = 1; i < 128; i++)
                held.Add(pool.Acquire(Guid.NewGuid(), TestContext.Current.CancellationToken));
            Assert.Throws<DataGridTransportAdmissionException>(() =>
                pool.Acquire(Guid.NewGuid(), TestContext.Current.CancellationToken)
            );
            Assert.Equal(135, capture.Reserved("principal", "client"));
        }
        finally
        {
            foreach (var lease in held)
            {
                lease.Dispose();
                lease.Dispose();
            }
        }
        using (pool.Acquire(Guid.NewGuid(), TestContext.Current.CancellationToken)) { }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private sealed class Clock : TimeProvider
    {
        private long _ticks;
        public override long TimestampFrequency => TimeSpan.TicksPerSecond;

        public override long GetTimestamp() => _ticks;

        internal void Advance(TimeSpan duration) => _ticks += duration.Ticks;
    }
}
