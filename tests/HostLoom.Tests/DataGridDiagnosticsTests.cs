using System.Diagnostics.Metrics;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using HostLoom.DataGrid.Transport;
using Xunit;
using GridClient = HostLoom.DataGrid.Client.DataGridClient;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
[CollectionDefinition(nameof(DataGridDiagnosticsTests), DisableParallelization = true)]
public sealed class DataGridDiagnosticsTests
{
    [Fact]
    public void ActorLaneItemLimitsAreIndependentAndDrainBalancesReservations()
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        for (var i = 0; i < 128; i++)
            Assert.True(queue.TryWrite(Work(false, 7)));
        Assert.False(queue.TryWrite(Work(false, 7)));
        for (var i = 0; i < 256; i++)
            Assert.True(queue.TryWrite(Work(true, 11)));
        Assert.False(queue.TryWrite(Work(true, 11)));
        Assert.Equal(128, capture.Sum("runtime.queue.items", "client"));
        Assert.Equal(256, capture.Sum("runtime.queue.items", "control"));
        Assert.Equal(128 * 7, capture.Sum("runtime.queue.bytes", "client"));
        Assert.Equal(256 * 11, capture.Sum("runtime.queue.bytes", "control"));
        Assert.Equal(384, queue.Complete().Length);
        Assert.Empty(queue.Complete());
        Assert.False(queue.TryWrite(Work(false, 1)));
        capture.AssertBalanced();
        Assert.Equal(
            2,
            capture.Measurements.Count(m =>
                m.Name.EndsWith("queue.rejected", StringComparison.Ordinal)
                && Equals(m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.ReasonTag), "items")
            )
        );
        capture.AssertPrivate();
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void EncodedByteLimitRejectsBeforeAdmissionAndCannotWrap(bool control)
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        Assert.True(queue.TryWrite(Work(control, checked((int)DataGridRuntimeQueue.ByteLimit))));
        Assert.False(queue.TryWrite(Work(control, 1)));
        Assert.False(queue.TryWrite(Work(control, -1)));
        Assert.True(queue.TryWrite(Work(!control, 1)));
        Assert.Equal(2, queue.Complete().Length);
        capture.AssertBalanced();
        Assert.Equal(
            2,
            capture.Measurements.Count(m =>
                m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.ReasonTag) is "bytes"
            )
        );
    }

    [Fact]
    public async Task ReservedControlLaneYieldsAfterEightTurnsAndReleasesDequeuedBytes()
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        for (var i = 0; i < 10; i++)
            Assert.True(queue.TryWrite(Work(true, 3)));
        for (var i = 0; i < 2; i++)
            Assert.True(queue.TryWrite(Work(false, 5)));
        for (var i = 0; i < 8; i++)
            Assert.True((await queue.ReadAsync(TestContext.Current.CancellationToken)).Control);
        Assert.False((await queue.ReadAsync(TestContext.Current.CancellationToken)).Control);
        Assert.Equal(2, capture.Sum("runtime.queue.items", "control"));
        Assert.Equal(5, capture.Sum("runtime.queue.bytes", "client"));
        for (var i = 0; i < 2; i++)
            Assert.True((await queue.ReadAsync(TestContext.Current.CancellationToken)).Control);
        Assert.False((await queue.ReadAsync(TestContext.Current.CancellationToken)).Control);
        Assert.Empty(queue.Complete());
        capture.AssertBalanced();
    }

    [Fact(Timeout = 20000)]
    public async Task ConcurrentProducersCannotExceedEitherLaneAndRejectedWorkNeverExecutes()
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        var accepted = 0;
        var executed = 0;
        var rejectedOnDrain = 0;
        await Task.WhenAll(
            Enumerable
                .Range(0, 32)
                .Select(producer =>
                    Task.Run(
                        () =>
                        {
                            for (var i = 0; i < 32; i++)
                                if (
                                    queue.TryWrite(
                                        new(
                                            () => Interlocked.Increment(ref executed),
                                            _ => Interlocked.Increment(ref rejectedOnDrain),
                                            1024,
                                            producer % 2 == 0
                                        )
                                    )
                                )
                                    Interlocked.Increment(ref accepted);
                        },
                        TestContext.Current.CancellationToken
                    )
                )
        );
        Assert.Equal(384, accepted);
        Assert.Equal(128, capture.Sum("runtime.queue.items", "client"));
        Assert.Equal(256, capture.Sum("runtime.queue.items", "control"));
        foreach (var work in queue.Complete())
            work.Reject(new IOException("Owned queue stopped."));
        Assert.Equal(384, rejectedOnDrain);
        Assert.Equal(0, executed);
        Assert.Empty(queue.Complete());
        capture.AssertBalanced();
    }

    [Fact]
    public async Task CanceledWaitDoesNotConsumeNextWorkAndStoppedAdmissionIsExplicit()
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        using var canceled = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        var waiting = queue.ReadAsync(canceled.Token).AsTask();
        await canceled.CancelAsync();
        await Assert.ThrowsAnyAsync<OperationCanceledException>(() => waiting);
        Assert.False(queue.TryWrite(Work(true, 1), stopping: true));
        Assert.True(queue.TryWrite(Work(false, 2)));
        Assert.Equal(2, (await queue.ReadAsync(TestContext.Current.CancellationToken)).Bytes);
        capture.AssertBalanced();
    }

    [Fact(Timeout = 20000)]
    public async Task SdkCapacityAndDisableEmitOncePerCallAndReleaseAllActiveCounts()
    {
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        using var certificate = fixture.Issue("catalog-client.example", clientOnly: true);
        await using var client = new GridClient(
            new(
                fixture.Configuration,
                DataGridPeerFixture.Principal,
                certificate,
                fixture.Root,
                Enumerable
                    .Range(0, 3)
                    .Select(i => new DataGridClientEndpoint(
                        fixture.Configuration.GetMember(i).MemberId,
                        fixture.Configuration.GetMember(i).DnsName,
                        fixture.Configuration.GetMember(i).Port,
                        fixture.Fingerprints[i],
                        System.Net.IPAddress.Loopback
                    ))
                    .ToArray(),
                fixture.Build
            )
        );
        var request = DataGridClientRequest.Read(
            DataGridPeerFixture.Map,
            [DataGridPeerFixture.Key]
        );
        // These owned TCP listeners deliberately never speak TLS. Calls reserve their SDK reservations
        // synchronously before yielding at handshake/reconnect; no server or guessed delay is needed.
        var pending = Enumerable
            .Range(0, 8)
            .Select(_ => client.ExecuteAsync(request, TestContext.Current.CancellationToken))
            .ToArray();
        var refused = await client.ExecuteAsync(request, TestContext.Current.CancellationToken);
        Assert.Equal(DataGridClientFailureCode.CapacityExceeded, refused.Reply.Failure!.Code);
        Assert.False(refused.MayHaveBeenAdmitted);
        client.Disable();
        var results = await Task.WhenAll(pending);
        Assert.All(results, result => Assert.False(result.MayHaveBeenAdmitted));
        Assert.Equal(9, capture.Sum("client.calls"));
        Assert.Equal(8, capture.Sum("client.attempts"));
        Assert.Equal(
            9,
            capture.Measurements.Count(m => m.Name == "hostloom.datagrid.client.call.duration")
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.client.calls"
                && m.Tags.GetValueOrDefault(DataGridClientDiagnostics.OutcomeTag)
                    is "capacity_exceeded"
                && m.Tags.GetValueOrDefault(DataGridClientDiagnostics.AdmissionTag) is false
        );
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static DataGridRuntimeWork Work(bool control, int bytes) =>
        new(() => { }, _ => { }, bytes, control);
}

internal sealed record DataGridMeasurement(
    string Name,
    string? Unit,
    double Value,
    Dictionary<string, object?> Tags
);

internal sealed class DataGridMeterCapture : IDisposable
{
    private readonly MeterListener _listener = new();
    private readonly object _sync = new();
    private readonly List<DataGridMeasurement> _measurements = [];
    private TaskCompletionSource _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);

    internal DataGridMeterCapture()
    {
        _listener.InstrumentPublished = (instrument, listener) =>
        {
            if (
                instrument.Meter.Name
                is DataGridRuntimeDiagnostics.MeterName
                    or DataGridClientDiagnostics.MeterName
                    or DataGridTransportDiagnostics.MeterName
                    or DataGridStorageDiagnostics.MeterName
            )
                listener.EnableMeasurementEvents(instrument);
        };
        _listener.SetMeasurementEventCallback<long>(
            (instrument, value, tags, _) => Add(instrument, value, tags)
        );
        _listener.SetMeasurementEventCallback<double>(
            (instrument, value, tags, _) => Add(instrument, value, tags)
        );
        _listener.Start();
    }

    private void Add(
        Instrument instrument,
        double value,
        ReadOnlySpan<KeyValuePair<string, object?>> tags
    )
    {
        var owned = new Dictionary<string, object?>();
        foreach (var tag in tags)
            owned.Add(tag.Key, tag.Value);
        lock (_sync)
        {
            _measurements.Add(new(instrument.Name, instrument.Unit, value, owned));
            _changed.TrySetResult();
            _changed = new(TaskCreationOptions.RunContinuationsAsynchronously);
        }
    }

    internal DataGridMeasurement[] Measurements
    {
        get
        {
            lock (_sync)
                return _measurements.ToArray();
        }
    }

    internal async Task WaitFor(
        Func<DataGridMeasurement[], bool> predicate,
        CancellationToken token
    )
    {
        while (true)
        {
            Task changed;
            lock (_sync)
            {
                if (predicate(_measurements.ToArray()))
                    return;
                changed = _changed.Task;
            }
            await changed.WaitAsync(token);
        }
    }

    internal double Reserved(string resource, string role, string direction = "inbound") =>
        Measurements
            .Where(m =>
                m.Name == "hostloom.datagrid.transport.reservations.active"
                && Equals(m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.RoleTag), role)
                && Equals(
                    m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.DirectionTag),
                    direction
                )
                && Equals(
                    m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ResourceTag),
                    resource
                )
            )
            .Sum(m => m.Value);

    internal double Sum(string suffix, string? lane = null) =>
        Measurements
            .Where(m =>
                m.Name == "hostloom.datagrid." + suffix
                && (
                    lane is null
                    || Equals(m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.LaneTag), lane)
                )
            )
            .Sum(m => m.Value);

    internal void AssertBalanced()
    {
        foreach (
            var group in Measurements
                .Where(m =>
                    m.Name.EndsWith(".active", StringComparison.Ordinal)
                    || m.Name.EndsWith("queue.items", StringComparison.Ordinal)
                    || m.Name.EndsWith("queue.bytes", StringComparison.Ordinal)
                    || m.Name == "hostloom.datagrid.transport.reservations.bytes"
                )
                .GroupBy(m =>
                    (
                        m.Name,
                        Lane: m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.LaneTag),
                        Operation: m.Tags.GetValueOrDefault(
                            DataGridRuntimeDiagnostics.OperationTag
                        ),
                        Role: m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.RoleTag),
                        Direction: m.Tags.GetValueOrDefault(
                            DataGridTransportDiagnostics.DirectionTag
                        ),
                        Resource: m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ResourceTag)
                    )
                )
        )
        {
            double sum = 0;
            foreach (var value in group)
            {
                sum += value.Value;
                Assert.True(sum >= 0, group.Key.ToString());
            }
            Assert.Equal(0, sum);
        }
    }

    internal void AssertPrivate()
    {
        var allowed = new HashSet<string>
        {
            DataGridRuntimeDiagnostics.LaneTag,
            DataGridRuntimeDiagnostics.ReasonTag,
            DataGridRuntimeDiagnostics.OperationTag,
            DataGridRuntimeDiagnostics.OutcomeTag,
            DataGridClientDiagnostics.AdmissionTag,
            DataGridClientDiagnostics.SubmittedTag,
            DataGridTransportDiagnostics.RoleTag,
            DataGridTransportDiagnostics.DirectionTag,
            DataGridTransportDiagnostics.ResourceTag,
            DataGridTransportDiagnostics.ReasonTag,
            DataGridTransportDiagnostics.OutcomeTag,
            DataGridStorageDiagnostics.OutcomeTag,
            DataGridRuntimeDiagnostics.MemberOrdinalTag,
            DataGridRuntimeDiagnostics.MapOrdinalTag,
            DataGridRuntimeDiagnostics.PeerOrdinalTag,
            DataGridRuntimeDiagnostics.StateTag,
            DataGridRuntimeDiagnostics.PositionTag,
            DataGridRuntimeDiagnostics.MeasureTag,
        };
        foreach (var measurement in Measurements)
        {
            Assert.True(double.IsFinite(measurement.Value));
            Assert.All(
                measurement.Tags,
                tag =>
                {
                    Assert.Contains(tag.Key, allowed);
                    if (
                        tag.Key
                        is DataGridClientDiagnostics.AdmissionTag
                            or DataGridClientDiagnostics.SubmittedTag
                    )
                        Assert.IsType<bool>(tag.Value);
                    else if (
                        tag.Key
                        is DataGridRuntimeDiagnostics.MemberOrdinalTag
                            or DataGridRuntimeDiagnostics.PeerOrdinalTag
                            or DataGridRuntimeDiagnostics.MapOrdinalTag
                    )
                        Assert.InRange(
                            Assert.IsType<int>(tag.Value),
                            0,
                            tag.Key == DataGridRuntimeDiagnostics.MapOrdinalTag ? 15 : 2
                        );
                    else
                    {
                        var value = Assert.IsType<string>(tag.Value);
                        var valid = tag.Key switch
                        {
                            DataGridStorageDiagnostics.OutcomeTag => value
                                is "recovered"
                                    or "failed",
                            DataGridRuntimeDiagnostics.StateTag => value
                                is "follower"
                                    or "candidate"
                                    or "leader"
                                    or "faulted"
                                    or "stopped"
                                    or "local_ready_hint"
                                    or "busy"
                                    or "storage_faulted"
                                    or "storage_closed",
                            DataGridRuntimeDiagnostics.PositionTag => value
                                is "term"
                                    or "log_end"
                                    or "committed"
                                    or "applied"
                                    or "snapshot"
                                    or "last_read_barrier"
                                    or "last_read_barrier_term",
                            DataGridRuntimeDiagnostics.MeasureTag => value
                                is "charged"
                                    or "limit"
                                    or "wal_published"
                                    or "snapshot_published",
                            DataGridTransportDiagnostics.RoleTag => value is "peer" or "client",
                            DataGridTransportDiagnostics.DirectionTag => value
                                is "inbound"
                                    or "outbound",
                            DataGridTransportDiagnostics.ResourceTag => value
                                is "connection"
                                    or "handshake"
                                    or "member"
                                    or "principal"
                                    or "request"
                                    or "snapshot",
                            DataGridTransportDiagnostics.ReasonTag => value
                                is "capacity"
                                    or "rate"
                                    or "unauthorized"
                                    or "invalid_request",
                            DataGridTransportDiagnostics.OutcomeTag => value
                                is "completed"
                                    or "rejected"
                                    or "retired"
                                    or "authentication"
                                    or "protocol"
                                    or "canceled"
                                    or "io"
                                    or "faulted",
                            DataGridRuntimeDiagnostics.LaneTag => value is "control" or "client",
                            DataGridRuntimeDiagnostics.ReasonTag => value
                                is "stopped"
                                    or "bytes"
                                    or "items"
                                    or "principal",
                            DataGridRuntimeDiagnostics.OperationTag => value
                                is "read"
                                    or "mutation"
                                    or "open_session"
                                    or "close_session"
                                    or "time_advance"
                                    or "reclaim"
                                    or "other",
                            _ => value
                                is "success"
                                    or "applied"
                                    or "read"
                                    or "not_leader"
                                    or "busy"
                                    or "outcome_unknown"
                                    or "unavailable"
                                    or "invalid_request"
                                    or "capacity_exceeded"
                                    or "canceled"
                                    or "faulted"
                                    or "persisted"
                                    or "unauthorized"
                                    or "protocol_mismatch"
                                    or "quorum_unavailable"
                                    or "session_expired"
                                    or "request_retired"
                                    or "request_identity_mismatch"
                                    or "missing_map"
                                    or "redirect"
                                    or "other",
                        };
                        Assert.True(valid, value);
                    }
                }
            );
            Assert.DoesNotContain(
                measurement.Tags.Values,
                value =>
                    value?.ToString()?.Contains("catalog", StringComparison.OrdinalIgnoreCase)
                        is true
            );
            if (measurement.Name.EndsWith(".duration", StringComparison.Ordinal))
            {
                Assert.Equal("s", measurement.Unit);
                Assert.True(measurement.Value >= 0);
            }
            if (measurement.Name.EndsWith(".bytes", StringComparison.Ordinal))
                Assert.Equal("By", measurement.Unit);
        }
    }

    public void Dispose() => _listener.Dispose();
}
