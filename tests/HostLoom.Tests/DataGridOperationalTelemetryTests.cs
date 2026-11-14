using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridOperationalTelemetryTests
{
    [Fact]
    public void PhysicalExpiryObservationsExcludeLogicalAbsenceAndResetOnRestore()
    {
        using var fixture = new DataGridPeerFixture();
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        var open = machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                Guid.NewGuid(),
                new(TimeSpan.TicksPerSecond * 4),
                default
            )
        );
        var time = new DataGridTimestamp(TimeSpan.TicksPerSecond * 2);
        var expired = Request(1, 7, DataGridExpiryKind.Absolute, TimeSpan.TicksPerSecond);
        machine.Apply(
            2,
            DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, expired, default)
        );
        machine.Apply(3, DataGridCommittedCommand.AdvanceTime(time));
        Assert.Null(machine.Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key));
        Assert.Equal(0, machine.GetMapUsage(0).ExpiredReclaimed);
        Assert.Equal(1, machine.StoredKeyCount);
        machine.Apply(
            4,
            DataGridCommittedCommand.Mutate(
                DataGridPeerFixture.Principal,
                Request(2, 9, DataGridExpiryKind.Never, 0),
                time
            )
        );
        Assert.Equal(1, machine.GetMapUsage(0).ExpiredReclaimed);
        var second = Request(3, 11, DataGridExpiryKind.Absolute, TimeSpan.TicksPerSecond);
        var result = machine.Apply(
            5,
            DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, second, time)
        );
        machine.Apply(6, DataGridCommittedCommand.Reclaim(DataGridPeerFixture.Map, time));
        Assert.Equal(2, machine.GetMapUsage(0).ExpiredReclaimed);
        Assert.Equal(0, machine.StoredKeyCount);
        Assert.Same(
            result,
            machine.Apply(
                7,
                DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, second, time)
            )
        );
        Assert.Equal(2, machine.GetMapUsage(0).ExpiredReclaimed);
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var restored = DataGridLocalStateMachine.Restore(
            fixture.Configuration,
            snapshot.Descriptor,
            snapshot.GetContent()
        );
        Assert.Equal(0, restored.GetMapUsage(0).ExpiredReclaimed);
        Assert.Equal(
            snapshot.GetContent(),
            restored.SaveSnapshot(snapshot.Descriptor.SnapshotId, 1).GetContent()
        );
        Assert.Equal(0, restored.StoredKeyCount);
        Assert.True(restored.RetainedResultByteCount > 0);
        Assert.Equal(1, open.SessionNumber);

        DataGridMutationCommand Request(
            ulong number,
            byte value,
            DataGridExpiryKind kind,
            long ticks
        ) =>
            new(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                open.SessionNumber,
                number,
                kind,
                ticks,
                [new(DataGridPeerFixture.Key, [value])]
            );
    }

    [Fact]
    public void RaftDeadlineHasExplicitProvenanceWhileStepDownRemainsUnavailable()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var node = new RaftNode(config, 0, 1, new(config));
        var election = Assert.Single(node.Start().OfType<RaftTimer>());
        var voteWrite = Assert.Single(node.Timeout(election.Token, default).OfType<RaftWrite>());
        var vote = node.Persisted(voteWrite.Token, true).OfType<RaftSend>().First().Message;
        var noop = Assert.Single(
            node.Receive(
                    new(
                        RaftMessageKind.VoteResponse,
                        vote.To,
                        0,
                        node.CurrentTerm,
                        vote.Token,
                        config.IncarnationId,
                        config.ComputeDigest(),
                        accepted: true
                    ),
                    default
                )
                .OfType<RaftWrite>()
        );
        _ = node.Persisted(noop.Token, true);
        Assert.Equal(RaftRole.Leader, node.Role);
        var deadline = AdmitRead();
        var expired = Assert.Single(node.Timeout(deadline, default).OfType<RaftCompletion>());
        Assert.Equal(RaftCompletionKind.Unavailable, expired.Kind);
        Assert.True(expired.TimedOut);
        Assert.Empty(node.Timeout(deadline, default).OfType<RaftCompletion>());
        _ = AdmitRead();
        var steppedDown = Assert.Single(
            node.ObserveLeader(node.CurrentTerm + 1).OfType<RaftCompletion>()
        );
        Assert.Equal(RaftCompletionKind.Unavailable, steppedDown.Kind);
        Assert.False(steppedDown.TimedOut);

        RaftToken AdmitRead()
        {
            var effects = node.Read(
                Guid.NewGuid(),
                DataGridPeerFixture.Map,
                DataGridPeerFixture.Key,
                default
            );
            var timer = Assert.Single(effects.OfType<RaftTimer>());
            Assert.Equal(5000, timer.DelayMilliseconds);
            _ = node.Persisted(Assert.Single(effects.OfType<RaftWrite>()).Token, true);
            return timer.Token;
        }
    }

    [Fact]
    public void ProcessMemoryIsPolledOncePerProcessWithoutAttributingItToMembers()
    {
        using var capture = new DataGridMeterCapture();
        using var queue = new DataGridRuntimeQueue();
        Assert.True(queue.TryWrite(new(() => { }, _ => { }, 1, false)));
        queue.Complete();
        capture.Poll();
        var memory = capture
            .Measurements.Where(m => m.Name == "hostloom.datagrid.process.memory")
            .ToArray();
        Assert.Equal(2, memory.Length);
        Assert.All(
            memory,
            measurement =>
            {
                Assert.Equal("By", measurement.Unit);
                Assert.True(measurement.Value > 0);
                Assert.Single(measurement.Tags);
            }
        );
        Assert.Contains(
            memory,
            m => m.Tags[DataGridRuntimeDiagnostics.MeasureTag] is "working_set"
        );
        Assert.Contains(
            memory,
            m => m.Tags[DataGridRuntimeDiagnostics.MeasureTag] is "managed_estimate"
        );
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact]
    public void GroupRecoveryReportsEveryConfiguredMapAndFailsWithoutExposingTheDirectory()
    {
        Assert.SkipUnless(
            OperatingSystem.IsLinux() || OperatingSystem.IsMacOS(),
            "Unix file-store evidence."
        );
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        var configuration = new DataGridBootstrapConfiguration(
            fixture.Configuration.IncarnationId,
            Enumerable.Range(0, 3).Select(fixture.Configuration.GetMember).ToArray(),
            [
                fixture.Configuration.GetMap(0),
                new(
                    Guid.NewGuid(),
                    "inventory",
                    DataGridMapOwnership.Derived,
                    "inventory-service",
                    "inventory-loader",
                    10,
                    1024,
                    new(1, 1, 2, 1, 1)
                ),
            ]
        );
        var path = Directory.CreateTempSubdirectory("hostloom-telemetry-recovery-").FullName;
        try
        {
            using (DataGridFileStore.Bootstrap(path, configuration, 0)) { }
            using (DataGridFileStore.Open(path, configuration, 0)) { }
            File.WriteAllBytes(Path.Combine(path, "CURRENT"), [0]);
            Assert.Throws<InvalidDataException>(() =>
                DataGridFileStore.Open(path, configuration, 0)
            );
            for (var map = 0; map < 2; map++)
            {
                var events = capture
                    .Measurements.Where(m =>
                        m.Name == "hostloom.datagrid.storage.map.recovery.events"
                        && Equals(m.Tags[DataGridRuntimeDiagnostics.MapOrdinalTag], map)
                    )
                    .ToArray();
                Assert.Equal(
                    2,
                    events.Count(m => m.Tags[DataGridStorageDiagnostics.OutcomeTag] is "started")
                );
                Assert.Single(
                    events,
                    m => m.Tags[DataGridStorageDiagnostics.OutcomeTag] is "recovered"
                );
                Assert.Single(
                    events,
                    m => m.Tags[DataGridStorageDiagnostics.OutcomeTag] is "failed"
                );
                Assert.Equal(
                    2,
                    capture.Measurements.Count(m =>
                        m.Name == "hostloom.datagrid.storage.map.recovery.duration"
                        && Equals(m.Tags[DataGridRuntimeDiagnostics.MapOrdinalTag], map)
                        && m.Unit == "s"
                    )
                );
            }
            Assert.Equal(2, capture.Sum("storage.recoveries"));
            capture.AssertPrivate();
        }
        finally
        {
            Directory.Delete(path, true);
        }
    }
}
