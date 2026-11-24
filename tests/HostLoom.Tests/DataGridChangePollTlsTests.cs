using HostLoom.DataGrid;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridChangePollTlsTests
{
    private static readonly Guid Map = DataGridPeerFixture.Map;

    [Fact(Timeout = 35000)]
    public async Task AuthorizedPollRedirectsCapturesCommittedChangesAndGapsOnFailover()
    {
        RequireLinux();
        var token = TestContext.Current.CancellationToken;
        await using var fixture = new DataGridClientTlsTests.ClientCluster(
            allowChangeObservation: true
        );
        var leader = await fixture.Cluster.Leader(token);
        var follower = (leader + 1) % 3;
        var barrier = await fixture.Cluster.Nodes[leader]!.ReadAsync(
            Map,
            DataGridPeerFixture.Key,
            token
        );
        await fixture.Cluster.Nodes[follower]!.WaitForAppliedAsync(barrier.Index, token);
        await using var observer = fixture.Client(follower);
        var initial = await observer.ExecuteAsync(DataGridClientRequest.PollChanges(Map), token);
        Assert.Equal(DataGridClientFailureCode.Redirect, initial.Attempts[0].Reply?.Failure?.Code);
        Assert.Equal(
            fixture.Peer.Configuration.GetMember(leader).MemberId,
            initial.Attempts[^1].MemberId
        );
        var start = Assert.IsType<DataGridChangePage>(initial.Reply.ChangePage);
        Assert.False(start.Gap);
        Assert.Empty(start.Changes);

        await using var writer = fixture.Client(leader, otherWriter: true);
        var session = await writer.ExecuteAsync(
            DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(4))
            ),
            token
        );
        Assert.Null(session.Reply.Failure);
        var written = await writer.ExecuteAsync(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(session.Reply.Result!.SessionNumber)
            ),
            token
        );
        Assert.Null(written.Reply.Failure);
        var writtenIndex = written.Reply.Result!.GetItem(0).Version.LogIndex;
        var poll = await observer.ExecuteAsync(
            DataGridClientRequest.PollChanges(Map, start.Cursor),
            token
        );
        var page = Assert.IsType<DataGridChangePage>(poll.Reply.ChangePage);
        Assert.False(page.Gap);
        Assert.Equal(
            new DataGridCommittedChange(writtenIndex, DataGridPeerFixture.Key),
            Assert.Single(page.Changes)
        );
        Assert.True(start.AppliedIndex < writtenIndex && writtenIndex < page.AppliedIndex);
        Assert.True(page.AppliedTime.UtcTicks >= start.AppliedTime.UtcTicks);

        fixture.AssertHealthy();
        await fixture.Cluster.Stop(leader);
        var replacement = await fixture.Cluster.Leader(token);
        Assert.NotEqual(leader, replacement);
        var resumed = await observer.ExecuteAsync(
            DataGridClientRequest.PollChanges(Map, page.Cursor),
            token
        );
        var gap = Assert.IsType<DataGridChangePage>(resumed.Reply.ChangePage);
        Assert.True(gap.Gap);
        Assert.Empty(gap.Changes);
        Assert.NotEqual(page.Cursor.Generation, gap.Cursor.Generation);
        Assert.True(gap.AppliedIndex > page.AppliedIndex);
        var again = await observer.ExecuteAsync(
            DataGridClientRequest.PollChanges(Map, gap.Cursor),
            token
        );
        Assert.False(Assert.IsType<DataGridChangePage>(again.Reply.ChangePage).Gap);
    }

    [Fact(Timeout = 30000)]
    public async Task ReadGrantAndUnrelatedMapAreDeniedBeforeActorAdmission()
    {
        RequireLinux();
        var token = TestContext.Current.CancellationToken;
        await using var fixture = new DataGridClientTlsTests.ClientCluster(
            memberCount: 1,
            allowChangeObservation: true
        );
        var leader = await fixture.Cluster.Leader(token);
        var entered = 0;
        fixture.Override = (_, _, _) =>
        {
            Interlocked.Increment(ref entered);
            throw new InvalidOperationException("Unauthorized poll reached the runtime.");
        };
        await using var reader = fixture.Client(leader, readOnly: true);
        var readGrant = await reader.ExecuteAsync(DataGridClientRequest.PollChanges(Map), token);
        await using var observer = fixture.Client(leader);
        var foreignMap = await observer.ExecuteAsync(
            DataGridClientRequest.PollChanges(Guid.NewGuid()),
            token
        );

        Assert.All(
            new[] { readGrant, foreignMap },
            outcome =>
            {
                Assert.Equal(DataGridClientFailureCode.Unauthorized, outcome.Reply.Failure!.Code);
                Assert.Null(outcome.Reply.ChangePage);
                Assert.False(outcome.MayHaveBeenAdmitted);
                Assert.Single(outcome.Attempts);
            }
        );
        Assert.Equal(0, Volatile.Read(ref entered));
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 25000)]
    public async Task LeaderWithoutQuorumCannotReturnItsLocalPage()
    {
        RequireLinux();
        var token = TestContext.Current.CancellationToken;
        await using var fixture = new DataGridClientTlsTests.ClientCluster(
            allowChangeObservation: true
        );
        var leader = await fixture.Cluster.Leader(token);
        await using var observer = fixture.Client(leader);
        var initial = await observer.ExecuteAsync(DataGridClientRequest.PollChanges(Map), token);
        var baseline = Assert.IsType<DataGridChangePage>(initial.Reply.ChangePage);
        for (var i = 0; i < 3; i++)
            if (i != leader)
                await fixture.Cluster.Stop(i);

        var poll = await observer.ExecuteAsync(
            DataGridClientRequest.PollChanges(Map, baseline.Cursor),
            token
        );
        Assert.Equal(DataGridClientFailureCode.QuorumUnavailable, poll.Reply.Failure!.Code);
        Assert.Null(poll.Reply.ChangePage);
        Assert.True(poll.MayHaveBeenAdmitted);
        Assert.Contains(poll.Attempts, attempt => attempt.RequestSubmitted);
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "DataGrid change polling requires Linux TLS 1.3."
        );
}
