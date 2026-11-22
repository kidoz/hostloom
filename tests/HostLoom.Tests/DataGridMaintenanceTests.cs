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

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void MaintenanceReservesOneEntryForSingletonReelection(int excessEntries)
    {
        using var fixture = new DataGridPeerFixture(memberCount: 1);
        var advance = DataGridCommittedCommandCodec.Encode(
            DataGridCommittedCommand.AdvanceTime(default)
        );
        // The first election, maintenance pair, and second election require four entries.
        var entries = Enumerable
            .Range(1, RaftDurableImage.MaxEntries - 4 + excessEntries)
            .Select(i => new RaftEntry(i, 1, advance))
            .ToArray();
        CheckMaintenanceAndReelection(fixture.Configuration, entries, excessEntries == 0);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    public void MaintenanceReservesOneEncodedTimeAdvanceForSingletonReelection(int excessBytes)
    {
        using var fixture = new DataGridPeerFixture(memberCount: 1);
        var advance = DataGridCommittedCommandCodec.Encode(
            DataGridCommittedCommand.AdvanceTime(default)
        );
        var reclaim = DataGridCommittedCommandCodec.Encode(
            DataGridCommittedCommand.Reclaim(DataGridPeerFixture.Map, default)
        );
        var initialBytes =
            RaftDurableImage.MaxLogBytes - 3 * advance.Length - reclaim.Length + excessBytes;
        byte[] Mutation(int length) =>
            DataGridCommittedCommandCodec.Encode(
                DataGridFileStorageTests.Mutation(1, new byte[length])
            );

        var overhead = Mutation(0).Length;
        var count = RaftDurableImage.MaxLogBytes / DataGridMutationItem.MaxValueByteCount;
        var valueLength = initialBytes / count - overhead;
        var command = Mutation(valueLength);
        var longerCommand = Mutation(valueLength + 1);
        var entries = Enumerable
            .Range(1, count)
            .Select(i => new RaftEntry(i, 1, i <= initialBytes % count ? longerCommand : command))
            .ToArray();
        Assert.Equal(initialBytes, entries.Sum(entry => entry.ByteCount));
        CheckMaintenanceAndReelection(fixture.Configuration, entries, excessBytes == 0);
    }

    private static void CheckMaintenanceAndReelection(
        DataGridBootstrapConfiguration configuration,
        RaftEntry[] entries,
        bool maintenanceFits
    )
    {
        RaftNode Elect(RaftDurableImage image, long epoch)
        {
            var node = new RaftNode(configuration, 0, epoch, image);
            var timer = Assert.Single(node.Start().OfType<RaftTimer>());
            PersistWrites(node, node.Timeout(timer.Token, default));
            Assert.Equal(RaftRole.Leader, node.Role);
            Assert.Equal(image.LastIndex + 1, node.Durable.CommitIndex);
            return node;
        }

        var leader = Elect(new(configuration, 1, 0, entries, entries.Length), 1);
        var effects = leader.Maintain(new(1));
        if (maintenanceFits)
        {
            PersistWrites(leader, effects);
            Assert.Equal(entries.Length + 3, leader.Durable.CommitIndex);
        }
        else
            Assert.Empty(effects);

        _ = Elect(leader.Durable, 2);
    }

    private static void PersistWrites(RaftNode node, IReadOnlyList<RaftEffect> effects)
    {
        while (effects.OfType<RaftWrite>().SingleOrDefault() is { } write)
            effects = node.Persisted(write.Token, true);
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
