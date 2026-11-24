using HostLoom.DataGrid;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridChangeFeedTlsTests
{
    [Fact(Timeout = 35000)]
    public async Task ReplicasPublishCommittedKeysInOrderAndGapOnMemberSwitch()
    {
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "DataGrid change feed replication requires Linux TLS 1.3."
        );
        var token = TestContext.Current.CancellationToken;
        await using var fixture = new DataGridClientTlsTests.ClientCluster(memberCount: 3);
        var leader = await fixture.Cluster.Leader(token);
        var primary = fixture.Cluster.Nodes[leader]!;
        var follower = fixture.Cluster.Nodes[(leader + 1) % 3]!;
        var map = DataGridPeerFixture.Map;
        var first = DataGridPeerFixture.Key;
        var second = DataGridKey.FromUtf8("catalog:us");
        var primaryStart = await primary.ReadChangesAsync(map, null, token);
        var followerStart = await follower.ReadChangesAsync(map, null, token);
        await using var client = fixture.Client(leader);
        var open = await client.ExecuteAsync(
            DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(4))
            ),
            token
        );
        Assert.Null(open.Reply.Failure);
        var session = open.Reply.Result!.SessionNumber;
        var put = DataGridClientRequest.Mutate(
            new(
                DataGridMutationKind.Put,
                map,
                session,
                1,
                DataGridExpiryKind.Never,
                0,
                [new(first, [1]), new(second, [2])]
            )
        );
        var applied = await client.ExecuteAsync(put, token);
        Assert.Null(applied.Reply.Failure);
        var putIndex = applied.Reply.Result!.GetItem(0).Version.LogIndex;

        var replay = await client.ExecuteAsync(put, token);
        Assert.Null(replay.Reply.Failure);
        Assert.Equal(applied.Reply.Result.GetItem(0), replay.Reply.Result!.GetItem(0));
        var rejected = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(session, 2, DataGridMutationKind.PutIfAbsent)
            ),
            token
        );
        Assert.Equal(
            DataGridMutationStatus.AlreadyExists,
            rejected.Reply.Result!.GetItem(0).Status
        );
        var removed = await client.ExecuteAsync(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(session, 3, DataGridMutationKind.Remove)
            ),
            token
        );
        Assert.Equal(DataGridMutationStatus.Removed, removed.Reply.Result!.GetItem(0).Status);
        var barrier = await primary.ReadAsync(map, first, token);
        await follower.WaitForAppliedAsync(barrier.Index, token);

        var primaryPage = await primary.ReadChangesAsync(map, primaryStart.Cursor, token);
        var followerPage = await follower.ReadChangesAsync(map, followerStart.Cursor, token);
        Assert.False(primaryPage.Gap);
        Assert.False(followerPage.Gap);
        Assert.Equal(primaryPage.Changes.ToArray(), followerPage.Changes.ToArray());
        Assert.Collection(
            primaryPage.Changes,
            change =>
            {
                Assert.Equal(putIndex, change.Index);
                Assert.Equal(first, change.Key);
            },
            change =>
            {
                Assert.Equal(putIndex, change.Index);
                Assert.Equal(second, change.Key);
            },
            change =>
            {
                Assert.True(change.Index > putIndex);
                Assert.Equal(first, change.Key);
            }
        );
        var switched = await follower.ReadChangesAsync(map, primaryPage.Cursor, token);
        Assert.True(switched.Gap);
        Assert.Equal(followerPage.Cursor, switched.Cursor);
        Assert.Empty(switched.Changes);
        fixture.AssertHealthy();
    }
}
