using HostLoom.Conformance;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridChangeFeedTests
{
    private static readonly Guid Map = DataGridPeerFixture.Map;
    private static readonly DataGridKey Key = DataGridPeerFixture.Key;

    [Fact]
    public void AppliedMutationsAndReclamationRecordActualChanges()
    {
        using var fixture = new DataGridPeerFixture();
        var feed = new DataGridChangeFeed(new ManualTimeProvider());
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        var cursor = feed.Read(Map, null, default).Cursor;
        var open = machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                Guid.NewGuid(),
                new(20),
                new(10)
            ),
            feed
        );
        var put = new DataGridMutationCommand(
            DataGridMutationKind.Put,
            Map,
            open.SessionNumber,
            1,
            DataGridExpiryKind.Relative,
            5,
            [new(Key, [1])]
        );
        machine.Apply(
            2,
            DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, put, new(10)),
            feed
        );
        machine.Apply(
            3,
            DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, put, new(11)),
            feed
        );
        var rejected = DataGridClientCodecTests.Mutation(
            open.SessionNumber,
            2,
            DataGridMutationKind.PutIfAbsent
        );
        machine.Apply(
            4,
            DataGridCommittedCommand.Mutate(DataGridPeerFixture.Principal, rejected, new(12)),
            feed
        );
        machine.Apply(5, DataGridCommittedCommand.AdvanceTime(new(15)), feed);
        var beforeReclaim = feed.Read(Map, cursor, machine.AppliedTime);
        Assert.Equal(2, Assert.Single(beforeReclaim.Changes).Index);
        Assert.Equal(new DataGridTimestamp(15), beforeReclaim.AppliedTime);
        Assert.Null(machine.Read(Map, Key));

        machine.Apply(6, DataGridCommittedCommand.Reclaim(Map, new(15)), feed);
        var reclaim = feed.Read(Map, beforeReclaim.Cursor, machine.AppliedTime);
        var change = Assert.Single(reclaim.Changes);
        Assert.Equal(6, change.Index);
        Assert.Equal(Key, change.Key);
    }

    [Theory]
    [InlineData(1)]
    [InlineData(DataGridKey.MaxByteCount)]
    public void RetentionEnforcesCountAndByteLimitsWithAnExplicitGap(int keyBytes)
    {
        var feed = new DataGridChangeFeed(new ManualTimeProvider());
        IDataGridCommittedChangeSink sink = feed;
        var key = new DataGridKey(new byte[keyBytes]);
        var capacity = Math.Min(
            DataGridChangeFeed.MaxChanges,
            DataGridChangeFeed.MaxBytes / (64 + keyBytes)
        );
        var start = feed.Read(Map, null, default).Cursor;
        for (var index = 1; index <= capacity; index++)
            sink.Record(index, Map, key);

        var first = feed.Read(Map, start, default);
        Assert.False(first.Gap);
        Assert.True(first.HasMore);
        Assert.Equal(DataGridChangePage.MaxChanges, first.Changes.Count);
        Assert.Equal(1, first.Changes[0].Index);
        sink.Record(capacity + 1, Map, key);

        var dropped = feed.Read(Map, start, default);
        Assert.True(dropped.Gap);
        Assert.Empty(dropped.Changes);
        Assert.Equal(capacity + 1, dropped.Cursor.Sequence);
        var resumed = feed.Read(Map, first.Cursor, default);
        Assert.False(resumed.Gap);
        Assert.Equal(first.Cursor.Sequence + 1, resumed.Changes[0].Index);
    }

    [Fact]
    public void HistoryAgesOutAndPositionsBelongToOneMapAndGeneration()
    {
        var clock = new ManualTimeProvider();
        var feed = new DataGridChangeFeed(clock);
        IDataGridCommittedChangeSink sink = feed;
        var otherMap = Guid.NewGuid();
        var cursor = feed.Read(Map, null, default).Cursor;
        sink.Record(1, otherMap, Key);
        sink.Record(2, Map, Key);
        var next = feed.Read(Map, cursor, new(20));
        Assert.False(next.HasMore);
        Assert.Equal(2, Assert.Single(next.Changes).Index);
        Assert.True(feed.Read(otherMap, next.Cursor, default).Gap);
        Assert.True(feed.Read(Map, next.Cursor with { Sequence = 3 }, default).Gap);

        clock.Advance(DataGridChangeFeed.MaxAge);
        var expired = feed.Read(Map, cursor, new(25));
        Assert.True(expired.Gap);
        Assert.Equal(next.Cursor, expired.Cursor);
        Assert.Empty(expired.Changes);
        Assert.True(new DataGridChangeFeed(clock).Read(Map, expired.Cursor, default).Gap);
        Assert.False(feed.Read(Map, expired.Cursor, default).Gap);
    }

    [Fact]
    public void FollowerFeedRecordsOnlyPersistedCommittedApplications()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var open = DataGridCommittedCommand.OpenSession(
            DataGridPeerFixture.Principal,
            Guid.NewGuid(),
            new(20),
            new(10)
        );
        var command = DataGridCommittedCommand.Mutate(
            DataGridPeerFixture.Principal,
            DataGridClientCodecTests.Mutation(),
            new(10)
        );
        var durable = new RaftDurableImage(
            config,
            term: 1,
            entries: [new(1, 1, DataGridCommittedCommandCodec.Encode(open))],
            commitIndex: 1
        );
        var append = new RaftMessage(
            RaftMessageKind.AppendRequest,
            0,
            1,
            1,
            new(1, 1),
            config.IncarnationId,
            config.ComputeDigest(),
            index: 1,
            previousTerm: 1,
            commit: 2,
            entries: [new(2, 1, DataGridCommittedCommandCodec.Encode(command))]
        );
        foreach (var succeeded in new[] { false, true })
        {
            var feed = new DataGridChangeFeed(new ManualTimeProvider());
            var follower = new RaftNode(config, 1, 1, durable, feed);
            var cursor = feed.Read(Map, null, default).Cursor;
            var write = Assert.Single(follower.Receive(append, default).OfType<RaftWrite>());
            Assert.Empty(feed.Read(Map, cursor, default).Changes);
            follower.Persisted(write.Token, succeeded);
            var result = feed.Read(Map, cursor, default);
            if (succeeded)
                Assert.Equal(2, Assert.Single(result.Changes).Index);
            else
                Assert.Empty(result.Changes);
        }
    }

    [Fact]
    public void DurableSnapshotInstallResetsFeedAndLocalCompactionPreservesIt()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var clock = new ManualTimeProvider();
        var feed = new DataGridChangeFeed(clock);
        var follower = new RaftNode(config, 1, 1, new(config, term: 1), feed);
        var cursor = feed.Read(Map, null, default).Cursor;
        var application = new DataGridLocalStateMachine(config);
        application.Apply(1, DataGridCommittedCommand.AdvanceTime(new(25)));
        var snapshot = application.SaveSnapshot(Guid.NewGuid(), 1);
        var message = new RaftMessage(
            RaftMessageKind.SnapshotRequest,
            0,
            1,
            1,
            new(1, 1),
            config.IncarnationId,
            config.ComputeDigest(),
            commit: 1,
            snapshot: snapshot
        );
        var write = Assert.Single(follower.Receive(message, default).OfType<RaftWrite>());
        Assert.False(feed.Read(Map, cursor, default).Gap);
        follower.Persisted(write.Token, true);
        Assert.Equal(1, follower.Machine.LastAppliedIndex);
        Assert.True(feed.Read(Map, cursor, follower.Machine.AppliedTime).Gap);

        var compactedFeed = new DataGridChangeFeed(clock);
        var entry = new RaftEntry(
            1,
            1,
            DataGridCommittedCommandCodec.Encode(DataGridCommittedCommand.AdvanceTime(new(25)))
        );
        var compacting = new RaftNode(
            config,
            1,
            2,
            new(config, term: 1, entries: [entry], commitIndex: 1),
            compactedFeed
        );
        var compactingCursor = compactedFeed.Read(Map, null, default).Cursor;
        var compact = Assert.Single(compacting.Compact(Guid.NewGuid()).OfType<RaftWrite>());
        compacting.Persisted(compact.Token, true);
        Assert.Equal(1, compacting.Durable.SnapshotIndex);
        Assert.False(compactedFeed.Read(Map, compactingCursor, compacting.Machine.AppliedTime).Gap);
    }
}
