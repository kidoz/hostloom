using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridMaintenanceTests
{
    [Fact]
    public void MaximumConfiguredStateFitsTheSharedSnapshotEnvelope()
    {
        Assert.Equal(
            DataGridSnapshotDescriptor.MaxSnapshotByteCount,
            RaftDurableImage.MaxImageBytes
        );
        Assert.InRange(
            DataGridLocalStateMachine.MaximumEncodedSnapshotBytes,
            DataGridMapConfiguration.GroupMaxDataByteCount + 1,
            RaftDurableImage.MaxImageBytes
        );
    }

    [Fact]
    public void ReservedLogHeadroomPermitsMaintenanceWhenClientAdmissionIsFull()
    {
        using var fixture = new DataGridPeerFixture();
        var leader = Leader(fixture.Configuration, RaftDurableImage.ClientEntryLimit);
        Assert.Equal(
            RaftCompletionKind.Busy,
            Assert
                .Single(
                    leader
                        .Read(
                            Guid.NewGuid(),
                            DataGridPeerFixture.Map,
                            DataGridPeerFixture.Key,
                            default
                        )
                        .OfType<RaftCompletion>()
                )
                .Kind
        );
        var write = Assert.Single(leader.Maintain(new(1)).OfType<RaftWrite>());
        var added = write
            .Image.Entries.TakeLast(2)
            .Select(entry => DataGridCommittedCommandCodec.ReadFrom(entry.GetCommand()))
            .ToArray();
        Assert.Equal(DataGridCommittedCommandKind.TimeAdvance, added[0].Kind);
        Assert.Equal(DataGridCommittedCommandKind.Reclaim, added[1].Kind);
        Assert.Equal(DataGridPeerFixture.Map, added[1].MapId);
        Assert.Equal(leader.Durable.CommitIndex, write.Image.CommitIndex);
        Assert.True(write.Image.Entries.Count < RaftDurableImage.MaxEntries);
    }

    [Fact]
    public void CompactionDistinguishesOversizeBusyPreparedAndUnchangedWithoutLosingState()
    {
        using var fixture = new DataGridPeerFixture();
        var node = Leader(fixture.Configuration, 0);
        var before = node.Durable;
        var refused = node.PrepareCompaction(Guid.NewGuid(), maxImageBytes: 1);
        Assert.Equal(RaftCompactionStatus.TooLarge, refused.Status);
        Assert.Empty(refused.Effects);
        Assert.Same(before, node.Durable);
        var prepared = node.PrepareCompaction(Guid.NewGuid());
        Assert.Equal(RaftCompactionStatus.Prepared, prepared.Status);
        Assert.Equal(RaftCompactionStatus.Busy, node.PrepareCompaction(Guid.NewGuid()).Status);
        var write = Assert.Single(prepared.Effects.OfType<RaftWrite>());
        _ = node.Persisted(write.Token, true);
        Assert.Equal(before.CommitIndex, node.Durable.SnapshotIndex);
        Assert.Equal(RaftCompactionStatus.Unchanged, node.PrepareCompaction(Guid.NewGuid()).Status);
        Assert.Empty(node.Maintain(default).OfType<RaftCompletion>());
    }

    [Fact]
    public void FollowerDoesNotInventReplicatedCleanup()
    {
        using var fixture = new DataGridPeerFixture();
        var node = new RaftNode(fixture.Configuration, 0, 1, new(fixture.Configuration));
        Assert.Empty(node.Maintain(new(1)));
        Assert.Equal(0, node.Durable.LastIndex);
    }

    private static RaftNode Leader(DataGridBootstrapConfiguration configuration, int entries)
    {
        var command = DataGridCommittedCommandCodec.Encode(
            DataGridCommittedCommand.AdvanceTime(default)
        );
        var node = new RaftNode(
            configuration,
            0,
            1,
            new(
                configuration,
                1,
                0,
                Enumerable.Range(1, entries).Select(i => new RaftEntry(i, 1, command)),
                entries
            )
        );
        var timer = Assert.Single(node.Start().OfType<RaftTimer>());
        var election = Assert.Single(node.Timeout(timer.Token, default).OfType<RaftWrite>());
        var vote = node.Persisted(election.Token, true).OfType<RaftSend>().First().Message;
        var elected = node.Receive(
            new(
                RaftMessageKind.VoteResponse,
                vote.To,
                0,
                node.CurrentTerm,
                vote.Token,
                configuration.IncarnationId,
                configuration.ComputeDigest(),
                accepted: true
            ),
            default
        );
        var noop = Assert.Single(elected.OfType<RaftWrite>());
        var append = node.Persisted(noop.Token, true).OfType<RaftSend>().First().Message;
        var committed = node.Receive(
            new(
                RaftMessageKind.AppendResponse,
                append.To,
                0,
                node.CurrentTerm,
                append.Token,
                configuration.IncarnationId,
                configuration.ComputeDigest(),
                index: node.Durable.LastIndex,
                accepted: true
            ),
            default
        );
        _ = node.Persisted(Assert.Single(committed.OfType<RaftWrite>()).Token, true);
        Assert.Equal(RaftRole.Leader, node.Role);
        return node;
    }
}
