using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridProgressTlsTests
{
    [Fact(Timeout = 30000)]
    public async Task PublishedSnapshotsPreserveMapUsageAndExposeReplicaCatchupAndStoragePublication()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        DataGridPeerRuntime runtime;
        DataGridRuntimeSnapshot before;
        await using (var cluster = new DataGridPeerTlsTests.LiveCluster(fixture))
        {
            cluster.StartAll();
            var leader = await cluster.Leader(timeout.Token);
            runtime = cluster.Nodes[leader]!;
            var opened = await runtime.ProposeAsync(
                DataGridCommittedCommand.OpenSession(
                    DataGridPeerFixture.Principal,
                    Guid.NewGuid(),
                    DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(4)),
                    DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow)
                ),
                timeout.Token
            );
            Assert.Equal(RaftCompletionKind.Applied, opened.Kind);
            var mutation = await runtime.ProposeAsync(
                DataGridCommittedCommand.Mutate(
                    DataGridPeerFixture.Principal,
                    DataGridClientCodecTests.Mutation(opened.Result!.SessionNumber),
                    DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow)
                ),
                timeout.Token
            );
            Assert.Equal(RaftCompletionKind.Applied, mutation.Kind);
            var read = await runtime.ReadAsync(
                DataGridPeerFixture.Map,
                DataGridPeerFixture.Key,
                timeout.Token
            );
            Assert.Equal(RaftCompletionKind.Read, read.Kind);
            await capture.WaitFor(
                _ =>
                    runtime.Diagnostics.LastSuccessfulReadBarrierIndex == read.Index
                    && runtime.Diagnostics.Replicas.All(r => r.Acknowledged && r.EntryLag == 0),
                timeout.Token
            );
            before = runtime.Diagnostics;
            Assert.Equal(1, Assert.Single(before.Maps).ChargedKeys);
            Assert.Equal(DataGridPeerFixture.Key.ByteCount + 1, before.Maps[0].ChargedBytes);
            Assert.True(before.LocalReadyHint);
            Assert.Equal(before.Local.Term, before.LastSuccessfulReadBarrierTerm);
            var mapView = Assert.IsAssignableFrom<IList<DataGridMapUsage>>(before.Maps);
            Assert.Throws<NotSupportedException>(() =>
                mapView[0] = before.Maps[0] with { ChargedKeys = 99 }
            );
            var replicaView = Assert.IsAssignableFrom<IList<DataGridReplicaProgress>>(
                before.Replicas
            );
            Assert.Throws<NotSupportedException>(() => replicaView.Clear());
            var follower = (leader + 1) % 3;
            await cluster.Stop(follower);
            var changed = await runtime.ProposeAsync(
                DataGridCommittedCommand.Mutate(
                    DataGridPeerFixture.Principal,
                    DataGridClientCodecTests.Mutation(opened.Result.SessionNumber, 2, value: 8),
                    DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow)
                ),
                timeout.Token
            );
            Assert.Equal(RaftCompletionKind.Applied, changed.Kind);
            await capture.WaitFor(
                _ =>
                    runtime.Diagnostics.Replicas.Any(r =>
                        r.PeerOrdinal == follower && r.EntryLag > 0
                    ),
                timeout.Token
            );
            Assert.Equal(0, before.Replicas.Single(r => r.PeerOrdinal == follower).EntryLag);
            await runtime.CompactAsync(timeout.Token);
            await capture.WaitFor(
                _ => runtime.Diagnostics.Storage.PublishedSnapshotBytes > 0,
                timeout.Token
            );
            Assert.True(runtime.Diagnostics.Storage.PublishedWalBytes > 0);
            cluster.Start(follower);
            await cluster.Nodes[follower]!.WaitForAppliedAsync(changed.Index, timeout.Token);
            await capture.WaitFor(
                _ =>
                    runtime.Diagnostics.Replicas.Single(r => r.PeerOrdinal == follower).EntryLag
                    == 0,
                timeout.Token
            );
            Assert.True(runtime.Diagnostics.CapturedTimestamp > before.CapturedTimestamp);
            Assert.Equal(1, before.Maps[0].ChargedKeys);
            Assert.Contains(
                capture.Measurements,
                m =>
                    m.Name == "hostloom.datagrid.runtime.map.keys"
                    && Equals(
                        m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.MemberOrdinalTag),
                        leader
                    )
                    && m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.MeasureTag) is "charged"
                    && m.Value == 1
            );
        }
        Assert.All(
            new[] { before, runtime.Diagnostics },
            snapshot => Assert.Equal(1, snapshot.Maps[0].ChargedKeys)
        );
        Assert.True(runtime.Diagnostics.Local.Stopped);
        Assert.True(runtime.Diagnostics.Storage.Closed);
        Assert.False(runtime.Diagnostics.LocalReadyHint);
        Assert.All(
            runtime.Diagnostics.Replicas,
            r =>
            {
                Assert.False(r.Acknowledged);
                Assert.Null(r.EntryLag);
            }
        );
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 30000)]
    public async Task LocalReadyHintAndOldReadEvidenceCannotClaimCurrentQuorumAfterMajorityLoss()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        DataGridPeerRuntime runtime;
        int leader;
        await using (var cluster = new DataGridPeerTlsTests.LiveCluster(fixture))
        {
            cluster.StartAll();
            leader = await cluster.Leader(timeout.Token);
            runtime = cluster.Nodes[leader]!;
            var confirmed = await runtime.ReadAsync(
                DataGridPeerFixture.Map,
                DataGridPeerFixture.Key,
                timeout.Token
            );
            Assert.Equal(RaftCompletionKind.Read, confirmed.Kind);
            await capture.WaitFor(
                _ => runtime.Diagnostics.LastSuccessfulReadBarrierIndex == confirmed.Index,
                timeout.Token
            );
            var prior = runtime.Diagnostics;
            foreach (var peer in Enumerable.Range(0, 3).Where(i => i != leader))
                await cluster.Stop(peer);
            var unavailable = await runtime.ReadAsync(
                DataGridPeerFixture.Map,
                DataGridPeerFixture.Key,
                timeout.Token
            );
            Assert.Equal(RaftCompletionKind.Unavailable, unavailable.Kind);
            var after = runtime.Diagnostics;
            Assert.Equal(RaftRole.Leader, after.Local.Role);
            Assert.True(after.LocalReadyHint); // This is deliberately a local hint, not successful availability.
            Assert.Equal(
                prior.LastSuccessfulReadBarrierIndex,
                after.LastSuccessfulReadBarrierIndex
            );
            Assert.Equal(prior.LastSuccessfulReadBarrierTerm, after.LastSuccessfulReadBarrierTerm);
        }
        Assert.False(runtime.Diagnostics.LocalReadyHint);
        var lastHint = capture.Measurements.Last(m =>
            m.Name == "hostloom.datagrid.runtime.state"
            && Equals(m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.MemberOrdinalTag), leader)
            && m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.StateTag) is "local_ready_hint"
        );
        Assert.Equal(0, lastHint.Value);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS 1.3 progress evidence requires Linux."
        );

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(25));
        return timeout;
    }
}
