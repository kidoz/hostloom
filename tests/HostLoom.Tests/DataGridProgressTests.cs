using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Storage;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridProgressTests
{
    [Fact]
    public void MapUsageCapturesChargedExpiredEntriesUntilReclaimWithoutAdvancingState()
    {
        using var fixture = new DataGridPeerFixture();
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        var opened = machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                Guid.NewGuid(),
                new(TimeSpan.TicksPerSecond * 4),
                default
            )
        );
        var command = new DataGridMutationCommand(
            DataGridMutationKind.Put,
            DataGridPeerFixture.Map,
            opened.SessionNumber,
            1,
            DataGridExpiryKind.Absolute,
            TimeSpan.TicksPerSecond,
            [new(DataGridPeerFixture.Key, [7])]
        );
        machine.Apply(
            2,
            DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, command, default)
        );
        var before = machine.GetMapUsage(0);
        Assert.Equal(1, before.ChargedKeys);
        Assert.Equal(DataGridPeerFixture.Key.ByteCount + 1, before.ChargedBytes);
        machine.Apply(3, DataGridCommittedCommand.AdvanceTime(new(TimeSpan.TicksPerSecond * 2)));
        Assert.Null(machine.Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key));
        Assert.Equal(before, machine.GetMapUsage(0));
        machine.Apply(
            4,
            DataGridCommittedCommand.Reclaim(DataGridPeerFixture.Map, machine.AppliedTime)
        );
        Assert.Equal(0, machine.GetMapUsage(0).ChargedKeys);
        Assert.Equal(0, machine.GetMapUsage(0).ChargedBytes);
        Assert.Equal(1, before.ChargedKeys);
        Assert.Equal(4, machine.LastAppliedIndex);
        Assert.Throws<ArgumentOutOfRangeException>(() => machine.GetMapUsage(-1));
        Assert.Throws<ArgumentOutOfRangeException>(() => machine.GetMapUsage(1));
    }

    [Fact]
    public void ReplicaObservationRequiresCorrelatedAckAndResetsWithLeadership()
    {
        using var fixture = new DataGridPeerFixture();
        var node = new RaftNode(fixture.Configuration, 0, 1, new(fixture.Configuration));
        var append = Elect(node, fixture.Configuration);
        Assert.False(node.ReplicationProgress(1).Acknowledged);
        var through = append.Entries[^1].Index;
        var changedToken = new RaftMessage(
            RaftMessageKind.AppendResponse,
            1,
            0,
            node.CurrentTerm,
            new(999, 999),
            fixture.Configuration.IncarnationId,
            fixture.Configuration.ComputeDigest(),
            through,
            accepted: true
        );
        node.Receive(changedToken, default);
        Assert.False(node.ReplicationProgress(1).Acknowledged);
        Drain(
            node,
            node.Receive(
                new(
                    RaftMessageKind.AppendResponse,
                    1,
                    0,
                    node.CurrentTerm,
                    append.Token,
                    fixture.Configuration.IncarnationId,
                    fixture.Configuration.ComputeDigest(),
                    through,
                    accepted: true
                ),
                default
            )
        );
        Assert.True(node.ReplicationProgress(1).Acknowledged);
        Assert.Equal(through, node.ReplicationProgress(1).MatchedIndex);
        // No new response is required to read the observation: it carries historical evidence only.
        Assert.Equal(through, node.ReplicationProgress(1).MatchedIndex);
        Drain(node, node.ObserveLeader(node.CurrentTerm + 1));
        Assert.False(node.ReplicationProgress(1).Acknowledged);
        Elect(node, fixture.Configuration);
        Assert.False(node.ReplicationProgress(1).Acknowledged);
        Assert.Throws<ArgumentOutOfRangeException>(() => node.ReplicationProgress(3));
    }

    [Fact]
    public void CachedStorageProgressTracksPublishedGenerationAndSurvivesDisposalWithoutIo()
    {
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        using var directory = new OwnedDirectory();
        DataGridStorageProgress progress;
        using (var store = DataGridFileStore.Bootstrap(directory.Path, fixture.Configuration, 0))
        {
            AssertPublishedLength(store, directory.Path);
            var command = DataGridCommittedCommand.AdvanceTime(new(1));
            var image = new RaftDurableImage(
                fixture.Configuration,
                1,
                0,
                [new(1, 1, DataGridCommittedCommandCodec.Encode(command))],
                1
            );
            store.Persist(image, TestContext.Current.CancellationToken);
            AssertPublishedLength(store, directory.Path);
            Assert.Equal(1, store.Progress.DurableCommitIndex);
            var machine = store.RestoreApplication();
            var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
            store.Persist(
                new(fixture.Configuration, 1, 0, commitIndex: 1, snapshot: snapshot),
                TestContext.Current.CancellationToken
            );
            Assert.Equal(snapshot.Descriptor.ByteCount, store.Progress.PublishedSnapshotBytes);
            AssertPublishedLength(store, directory.Path);
            store.RollGeneration(TestContext.Current.CancellationToken);
            AssertPublishedLength(store, directory.Path);
            progress = store.Progress;
            store.Dispose();
            Assert.True(store.Progress.Closed);
            Assert.Equal(progress.PublishedWalBytes, store.Progress.PublishedWalBytes);
        }
        using (var recovered = DataGridFileStore.Open(directory.Path, fixture.Configuration, 0))
            Assert.Equal(progress, recovered.Progress);
        Assert.Equal(1, capture.Sum("storage.recoveries"));
        capture.AssertPrivate();
    }

    [Fact]
    public void FailedPersistencePreservesLastPublishedProgressAndRecoveryReportsFailurePrivately()
    {
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        using var directory = new OwnedDirectory();
        using (DataGridFileStore.Bootstrap(directory.Path, fixture.Configuration, 0)) { }
        using (
            var store = DataGridFileStore.Open(
                directory.Path,
                fixture.Configuration,
                0,
                observe: step =>
                {
                    if (step == DataGridStorageStep.WalWritten)
                        throw new IOException("catalog confidential storage marker");
                }
            )
        )
        {
            var before = store.Progress;
            Assert.Throws<IOException>(() =>
                store.Persist(
                    new(fixture.Configuration, 1, 0),
                    TestContext.Current.CancellationToken
                )
            );
            Assert.True(store.Progress.Faulted);
            Assert.Equal(before.PublishedWalBytes, store.Progress.PublishedWalBytes);
            Assert.Equal(before.DurableCommitIndex, store.Progress.DurableCommitIndex);
            Assert.Equal(before.GenerationRecordCount, store.Progress.GenerationRecordCount);
        }
        File.WriteAllBytes(System.IO.Path.Combine(directory.Path, "CURRENT"), [0]);
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Open(directory.Path, fixture.Configuration, 0)
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.storage.recoveries"
                && m.Tags.GetValueOrDefault(DataGridStorageDiagnostics.OutcomeTag) is "failed"
        );
        Assert.Equal(
            2,
            capture.Measurements.Count(m => m.Name == "hostloom.datagrid.storage.recovery.duration")
        );
        capture.AssertPrivate();
    }

    private static RaftMessage Elect(RaftNode node, DataGridBootstrapConfiguration configuration)
    {
        var timer = Assert.Single(node.Start().OfType<RaftTimer>());
        var votes = Drain(node, node.Timeout(timer.Token, default));
        var vote = votes.OfType<RaftSend>().First(e => e.Message.To == 1).Message;
        var leader = Drain(
            node,
            node.Receive(
                new(
                    RaftMessageKind.VoteResponse,
                    1,
                    0,
                    node.CurrentTerm,
                    vote.Token,
                    configuration.IncarnationId,
                    configuration.ComputeDigest(),
                    accepted: true
                ),
                default
            )
        );
        Assert.Equal(RaftRole.Leader, node.Role);
        return leader
            .OfType<RaftSend>()
            .First(e => e.Message.To == 1 && e.Message.Kind == RaftMessageKind.AppendRequest)
            .Message;
    }

    private static List<RaftEffect> Drain(RaftNode node, IReadOnlyList<RaftEffect> initial)
    {
        var all = new List<RaftEffect>();
        var pending = new Queue<RaftEffect>(initial);
        while (pending.TryDequeue(out var effect))
        {
            all.Add(effect);
            if (effect is RaftWrite write)
                foreach (var next in node.Persisted(write.Token, true))
                    pending.Enqueue(next);
        }
        return all;
    }

    private static void AssertPublishedLength(DataGridFileStore store, string directory) =>
        Assert.Equal(
            new FileInfo(
                System.IO.Path.Combine(directory, $"generation-{store.GenerationId:N}.wal")
            ).Length,
            store.Progress.PublishedWalBytes
        );

    private sealed class OwnedDirectory : IDisposable
    {
        internal OwnedDirectory()
        {
            Assert.SkipUnless(
                OperatingSystem.IsLinux() || OperatingSystem.IsMacOS(),
                "Unix file-store experiment."
            );
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "hostloom-progress-" + Guid.NewGuid().ToString("N")
            );
            Directory.CreateDirectory(Path);
        }

        internal string Path { get; }

        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}
