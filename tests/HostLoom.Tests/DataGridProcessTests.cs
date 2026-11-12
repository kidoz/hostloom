using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using System.Threading.Channels;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.ProcessProbe;
using HostLoom.DataGrid.Storage;
using Xunit;
using GridClient = HostLoom.DataGrid.Client.DataGridClient;

namespace HostLoom.Tests;

public sealed class DataGridProcessTests
{
    [Theory(Timeout = 90000)]
    [InlineData(DataGridMutationKind.CompareExchange)]
    [InlineData(DataGridMutationKind.CompareRemove)]
    public async Task OverlappingConditionalsAndRecreationPreserveRetainedResultsAcrossReplyLossAndFailover(
        DataGridMutationKind firstKind
    )
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        await using var opener = cluster.Client(leader);
        var firstSession = await Open(cluster, opener, timeout.Token);
        var competingSession = await Open(cluster, opener, timeout.Token);
        var maintenanceSession = await Open(cluster, opener, timeout.Token);
        var history = new DataGridClientHistory();
        await using var first = cluster.Client(leader, history);
        await using var competing = cluster.Client(leader, history);
        await using var maintenance = cluster.Client(leader, history);
        await using var observer = cluster.Client(leader, history);
        var initial = Stored(
            await first.ExecuteAsync(
                DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(firstSession)),
                timeout.Token
            )
        );
        var baseline = await observer.ExecuteAsync(Read(), timeout.Token);
        var request = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(firstSession, 2, firstKind, initial, 8)
        );
        var otherRequest = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(
                competingSession,
                1,
                firstKind == DataGridMutationKind.CompareExchange
                    ? DataGridMutationKind.CompareRemove
                    : DataGridMutationKind.CompareExchange,
                initial,
                6
            )
        );
        await cluster.Nodes[leader]!.Arm(timeout.Token);
        var pending = first.ExecuteAsync(request, timeout.Token);
        await cluster.Nodes[leader]!.Paused.WaitAsync(timeout.Token);
        var rejected = await competing.ExecuteAsync(otherRequest, timeout.Token);
        Assert.Null(rejected.Reply.Failure);
        Assert.Equal(
            firstKind == DataGridMutationKind.CompareExchange
                ? DataGridMutationStatus.VersionMismatch
                : DataGridMutationStatus.Missing,
            rejected.Reply.Result!.GetItem(0).Status
        );
        var during = await observer.ExecuteAsync(Read(), timeout.Token);
        Assert.Null(during.Reply.Failure);
        if (firstKind == DataGridMutationKind.CompareRemove)
            Assert.Null(during.Reply.Read!.GetItem(0));
        else
            Assert.Equal(new byte[] { 8 }, during.Reply.Read!.GetItem(0)!.GetValue());
        var removed = await maintenance.ExecuteAsync(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(
                    maintenanceSession,
                    1,
                    DataGridMutationKind.Remove
                )
            ),
            timeout.Token
        );
        Assert.Null(removed.Reply.Failure);
        Assert.Equal(
            firstKind == DataGridMutationKind.CompareExchange
                ? DataGridMutationStatus.Removed
                : DataGridMutationStatus.Missing,
            removed.Reply.Result!.GetItem(0).Status
        );
        var recreated = Stored(
            await maintenance.ExecuteAsync(
                DataGridClientRequest.Mutate(
                    DataGridClientCodecTests.Mutation(
                        maintenanceSession,
                        2,
                        DataGridMutationKind.PutIfAbsent,
                        value: 9
                    )
                ),
                timeout.Token
            )
        );
        Assert.True(recreated.LogIndex > initial.LogIndex);
        var beforeLoss = await observer.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(recreated, beforeLoss.Reply.Read!.GetItem(0)!.Version);
        Assert.False(pending.IsCompleted);
        Assert.Equal("LOST", await cluster.Nodes[leader]!.Command("lose", timeout.Token));
        var replayed = await pending;
        Assert.Null(replayed.Reply.Failure);
        Assert.True(replayed.MayHaveBeenAdmitted);
        Assert.InRange(replayed.Attempts.Count, 2, 3);
        Assert.True(replayed.Attempts[0].RequestSubmitted);
        Assert.True(replayed.Attempts[^1].RequestSubmitted);
        Assert.Equal(
            replayed.Attempts.Count,
            replayed.Attempts.Select(attempt => attempt.AttemptId).Distinct().Count()
        );
        if (firstKind == DataGridMutationKind.CompareExchange)
            Assert.Equal(during.Reply.Read!.GetItem(0)!.Version, Stored(replayed));
        else
            Assert.Equal(DataGridMutationStatus.Removed, replayed.Reply.Result!.GetItem(0).Status);
        var overlap = history
            .GetOperations()
            .Single(operation => ReferenceEquals(operation.Request, request));
        var competingOperation = history
            .GetOperations()
            .Single(operation => ReferenceEquals(operation.Request, otherRequest));
        Assert.True(overlap.InvocationOrder < competingOperation.InvocationOrder);
        Assert.True(competingOperation.CompletionOrder < overlap.CompletionOrder);
        await cluster.AppliedEverywhere(recreated.LogIndex, timeout.Token);
        await cluster.Kill(leader);
        var replacement = await cluster.Leader(timeout.Token);
        await using var after = cluster.Client(replacement, history);
        var retained = await after.ExecuteAsync(request, timeout.Token);
        Assert.Null(retained.Reply.Failure);
        Assert.Equal(replayed.Reply.Result!.GetItem(0), retained.Reply.Result!.GetItem(0));
        var retainedRejection = await after.ExecuteAsync(otherRequest, timeout.Token);
        Assert.Null(retainedRejection.Reply.Failure);
        Assert.Equal(rejected.Reply.Result.GetItem(0), retainedRejection.Reply.Result!.GetItem(0));
        var final = await after.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(recreated, final.Reply.Read!.GetItem(0)!.Version);
        Assert.Equal(new byte[] { 9 }, final.Reply.Read.GetItem(0)!.GetValue());
        AssertHistoryRejectsStaleRead(history, baseline.Reply.Read!.GetItem(0)!);
        var negative = history.GetOperations().ToArray();
        var duplicateIndex = Array.FindLastIndex(
            negative,
            operation => ReferenceEquals(operation.Request, request)
        );
        var duplicate = negative[duplicateIndex].Outcome!;
        negative[duplicateIndex] = negative[duplicateIndex] with
        {
            Outcome = new(
                DataGridClientReply.Completed(
                    DataGridMessageKind.MutationResult,
                    new(
                        DataGridApplyStatus.Completed,
                        [
                            firstKind == DataGridMutationKind.CompareExchange
                                ? new(DataGridMutationStatus.Stored, recreated)
                                : new(DataGridMutationStatus.Missing),
                        ]
                    )
                ),
                duplicate.MayHaveBeenAdmitted,
                duplicate.Attempts
            ),
        };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check(negative).Status
        );
        await cluster.Start(leader, timeout.Token);
        await cluster.AppliedEverywhere(recreated.LogIndex, timeout.Token);
    }

    [Theory(Timeout = 90000)]
    [InlineData((int)DataGridStorageStep.SnapshotFlushed)]
    [InlineData((int)DataGridStorageStep.GenerationDirectoryFlushed)]
    [InlineData((int)DataGridStorageStep.AnchorDirectoryFlushed)]
    public async Task FollowerKilledDuringSnapshotPublicationRejoinsWithoutLosingClientHistory(
        int step
    )
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        await using var opener = cluster.Client(leader);
        var session = await Open(cluster, opener, timeout.Token);
        var history = new DataGridClientHistory();
        await using var client = cluster.Client(leader, history);
        var first = Stored(
            await client.ExecuteAsync(
                DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
                timeout.Token
            )
        );
        await cluster.AppliedEverywhere(first.LogIndex, timeout.Token);
        var baseline = await client.ExecuteAsync(Read(), timeout.Token);
        var follower = (leader + 1) % 3;
        await cluster.Kill(follower);
        var previous = cluster.RecoveredRoot(follower);
        var beforeSnapshot = Stored(
            await client.ExecuteAsync(
                DataGridClientRequest.Mutate(
                    DataGridClientCodecTests.Mutation(session, 2, value: 8)
                ),
                timeout.Token
            )
        );
        await cluster.AppliedEverywhere(beforeSnapshot.LogIndex, timeout.Token);
        var other = (leader + 2) % 3;
        Assert.Equal("COMPACTED", await cluster.Nodes[leader]!.Command("compact", timeout.Token));
        Assert.Equal("COMPACTED", await cluster.Nodes[other]!.Command("compact", timeout.Token));
        // Drop pre-compaction sender payloads: otherwise the follower may legitimately catch
        // up from an already captured append instead of taking the snapshot under test.
        await cluster.StopAll(timeout.Token);
        await cluster.Start(leader, timeout.Token);
        await cluster.Start(other, timeout.Token);
        leader = await cluster.Leader(timeout.Token);
        var snapshot = (await cluster.Nodes[leader]!.Status(timeout.Token)).Snapshot;
        Assert.True(snapshot >= beforeSnapshot.LogIndex);
        await cluster.Start(follower, timeout.Token, (DataGridStorageStep)step);
        Assert.Equal(step, await cluster.Nodes[follower]!.StoragePaused.WaitAsync(timeout.Token));
        // The other two members still form a quorum while publication is blocked.
        var request = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(session, 3, value: 9)
        );
        var latest = Stored(await client.ExecuteAsync(request, timeout.Token));
        Assert.Equal(
            latest,
            (await client.ExecuteAsync(Read(), timeout.Token)).Reply.Read!.GetItem(0)!.Version
        );
        await cluster.Kill(follower);
        var recovered = cluster.RecoveredRoot(follower);
        if (step == (int)DataGridStorageStep.AnchorDirectoryFlushed)
        {
            Assert.Equal(snapshot, recovered.Snapshot);
            Assert.NotEqual(previous.Generation, recovered.Generation);
        }
        else
            Assert.Equal(previous, recovered);
        await cluster.Start(follower, timeout.Token);
        await cluster.AppliedEverywhere(latest.LogIndex, timeout.Token);
        await using var rejoined = cluster.Client(follower, history);
        Assert.Equal(latest, Stored(await rejoined.ExecuteAsync(request, timeout.Token)));
        var observed = await rejoined.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(latest, observed.Reply.Read!.GetItem(0)!.Version);
        Assert.Equal(new byte[] { 9 }, observed.Reply.Read.GetItem(0)!.GetValue());
        AssertHistoryRejectsStaleRead(history, baseline.Reply.Read!.GetItem(0)!);
    }

    [Fact(Timeout = 90000)]
    public async Task AbruptWholeGroupKillPreservesUnknownCasAndRetainedIdentity()
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        await using var opener = cluster.Client(leader);
        var session = await Open(cluster, opener, timeout.Token);
        var history = new DataGridClientHistory();
        await using var client = cluster.Client(leader, history);
        var first = Stored(
            await client.ExecuteAsync(
                DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
                timeout.Token
            )
        );
        var baseline = await client.ExecuteAsync(Read(), timeout.Token);
        var request = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(
                session,
                2,
                DataGridMutationKind.CompareExchange,
                first,
                9
            )
        );
        await cluster.Nodes[leader]!.Arm(timeout.Token);
        var pending = client.ExecuteAsync(request, timeout.Token);
        await cluster.Nodes[leader]!.Paused.WaitAsync(timeout.Token);
        client.Disable();
        var unknown = await pending;
        Assert.Equal(DataGridClientFailureCode.OutcomeUnknown, unknown.Reply.Failure!.Code);
        Assert.True(unknown.MayHaveBeenAdmitted);
        // Internal progress is coordination only; the checker receives SDK observations.
        await cluster.AppliedEverywhere(
            (await cluster.Nodes[leader]!.Status(timeout.Token)).Commit,
            timeout.Token
        );
        await Task.WhenAll(Enumerable.Range(0, 3).Select(cluster.Kill));
        await cluster.StartAll(timeout.Token);
        var replacement = await cluster.Leader(timeout.Token);
        await using var restarted = cluster.Client(replacement, history);
        var observed = await restarted.ExecuteAsync(Read(), timeout.Token);
        var latest = observed.Reply.Read!.GetItem(0)!.Version;
        Assert.True(latest.LogIndex > first.LogIndex);
        Assert.Equal(new byte[] { 9 }, observed.Reply.Read.GetItem(0)!.GetValue());
        Assert.Equal(latest, Stored(await restarted.ExecuteAsync(request, timeout.Token)));
        Assert.Equal(
            latest,
            (await restarted.ExecuteAsync(Read(), timeout.Token)).Reply.Read!.GetItem(0)!.Version
        );
        await cluster.AppliedEverywhere(latest.LogIndex, timeout.Token);
        AssertHistoryRejectsStaleRead(history, baseline.Reply.Read!.GetItem(0)!);
    }

    [Fact(Timeout = 90000)]
    public async Task AcknowledgedStateAndRetainedIdentitySurviveLeaderKillAndWholeGroupRestart()
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        await using var opener = cluster.Client(leader);
        var session = await Open(cluster, opener, timeout.Token);
        var history = new DataGridClientHistory();
        await using var client = cluster.Client(leader, history);
        var request = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session));
        var stored = await client.ExecuteAsync(request, timeout.Token);
        var version = Stored(stored);
        await cluster.AppliedEverywhere(version.LogIndex, timeout.Token);
        await cluster.Kill(leader);
        var replacement = await cluster.Leader(timeout.Token);
        await using var routed = cluster.Client(replacement, history);
        Assert.Equal(version, Stored(await routed.ExecuteAsync(request, timeout.Token)));
        var read = await routed.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(version, read.Reply.Read!.GetItem(0)!.Version);
        await cluster.Start(leader, timeout.Token);
        await cluster.AppliedEverywhere(version.LogIndex, timeout.Token);
        await cluster.StopAll(timeout.Token);
        await cluster.StartAll(timeout.Token);
        var recovered = await cluster.Leader(timeout.Token);
        await using var restarted = cluster.Client(recovered, history);
        Assert.Equal(version, Stored(await restarted.ExecuteAsync(request, timeout.Token)));
        var cas = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(
                session,
                2,
                DataGridMutationKind.CompareExchange,
                version,
                9
            )
        );
        var updated = Stored(await restarted.ExecuteAsync(cas, timeout.Token));
        Assert.True(updated.LogIndex > version.LogIndex);
        Assert.Equal(
            updated,
            (await restarted.ExecuteAsync(Read(), timeout.Token)).Reply.Read!.GetItem(0)!.Version
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check(history.GetOperations()).Status
        );
        Assert.Equal(3, cluster.Nodes.Count(node => node is not null));
    }

    [Fact(Timeout = 90000)]
    public async Task KilledLeaderAfterCommitBeforeReplyLeavesUnknownThatLaterReadAndRetryResolve()
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        await using var opener = cluster.Client(leader);
        var session = await Open(cluster, opener, timeout.Token);
        var history = new DataGridClientHistory();
        await using var client = cluster.Client(leader, history);
        var request = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session));
        await cluster.Nodes[leader]!.Arm(timeout.Token);
        var pending = client.ExecuteAsync(request, timeout.Token);
        await cluster.Nodes[leader]!.Paused.WaitAsync(timeout.Token);
        client.Disable();
        var unknown = await pending;
        Assert.Equal(DataGridClientFailureCode.OutcomeUnknown, unknown.Reply.Failure!.Code);
        Assert.True(unknown.MayHaveBeenAdmitted);
        await cluster.Kill(leader);
        var replacement = await cluster.Leader(timeout.Token);
        await using var retry = cluster.Client(replacement, history);
        var observed = await retry.ExecuteAsync(Read(), timeout.Token);
        var version = observed.Reply.Read!.GetItem(0)!.Version;
        Assert.Equal(version, Stored(await retry.ExecuteAsync(request, timeout.Token)));
        await retry.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check(history.GetOperations()).Status
        );
        var negative = history.GetOperations().ToArray();
        negative[^1] = negative[^1] with
        {
            Outcome = new(
                DataGridClientReply.Completed(
                    DataGridMessageKind.ReadResult,
                    read: new(observed.Reply.Read.CapturedAt, [null])
                ),
                true,
                []
            ),
        };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check(negative).Status
        );
        await cluster.Start(leader, timeout.Token);
        await cluster.AppliedEverywhere(version.LogIndex, timeout.Token);
    }

    [Fact(Timeout = 90000)]
    public async Task MinorityCannotReturnStrongSuccessAndOriginalRequestCanResolveAfterRecovery()
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        await using var opener = cluster.Client(leader);
        var session = await Open(cluster, opener, timeout.Token);
        var barrier = (await cluster.Nodes[leader]!.Status(timeout.Token)).Commit;
        await cluster.AppliedEverywhere(barrier, timeout.Token);
        foreach (var member in Enumerable.Range(0, 3).Where(i => i != leader))
            await cluster.Kill(member);
        var history = new DataGridClientHistory();
        await using var minority = cluster.Client(leader, history);
        var request = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session));
        var write = await minority.ExecuteAsync(request, timeout.Token);
        Assert.Equal(DataGridClientFailureCode.OutcomeUnknown, write.Reply.Failure!.Code);
        var read = await minority.ExecuteAsync(Read(), timeout.Token);
        Assert.NotNull(read.Reply.Failure);
        Assert.Null(read.Reply.Read);
        foreach (var member in Enumerable.Range(0, 3).Where(i => i != leader))
            await cluster.Start(member, timeout.Token);
        var recovered = await cluster.Leader(timeout.Token);
        await using var client = cluster.Client(recovered, history);
        var version = Stored(await client.ExecuteAsync(request, timeout.Token));
        Assert.Equal(
            version,
            (await client.ExecuteAsync(Read(), timeout.Token)).Reply.Read!.GetItem(0)!.Version
        );
        await cluster.AppliedEverywhere(version.LogIndex, timeout.Token);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check(history.GetOperations()).Status
        );
    }

    [Fact(Timeout = 90000)]
    public async Task AbsoluteTtlAndRetainedReplaySurviveLeaderKillWithoutResurrection()
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        await using var opener = cluster.Client(leader);
        var session = await Open(cluster, opener, timeout.Token);
        var history = new DataGridClientHistory();
        await using var client = cluster.Client(leader, history);
        var request = DataGridClientRequest.Mutate(
            new(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                session,
                1,
                DataGridExpiryKind.Absolute,
                cluster.Clock + TimeSpan.FromSeconds(10).Ticks,
                [new(DataGridPeerFixture.Key, [7])]
            )
        );
        var version = Stored(await client.ExecuteAsync(request, timeout.Token));
        var live = await client.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(TimeSpan.FromSeconds(10), live.Reply.Read!.GetItem(0)!.RemainingTimeToLive);
        await cluster.AppliedEverywhere(version.LogIndex, timeout.Token);
        await cluster.Kill(leader);
        var replacement = await cluster.Leader(timeout.Token);
        await cluster.AdvanceClock(TimeSpan.FromSeconds(11), timeout.Token);
        await using var after = cluster.Client(replacement, history);
        var expired = await after.ExecuteAsync(Read(), timeout.Token);
        Assert.Null(expired.Reply.Read!.GetItem(0));
        Assert.Equal(version, Stored(await after.ExecuteAsync(request, timeout.Token)));
        await after.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check(history.GetOperations()).Status
        );
        var negative = history.GetOperations().ToArray();
        negative[^1] = negative[^1] with
        {
            Outcome = new(
                DataGridClientReply.Completed(
                    DataGridMessageKind.ReadResult,
                    read: new(
                        expired.Reply.Read.CapturedAt,
                        [new([7], version, TimeSpan.FromTicks(1))]
                    )
                ),
                true,
                []
            ),
        };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check(negative).Status
        );
        await cluster.Start(leader, timeout.Token);
        await cluster.AppliedEverywhere(version.LogIndex, timeout.Token);
    }

    [Fact(Timeout = 90000)]
    public async Task PeerPartitionKeepsOldLeaderUnavailableAndHealsWithoutChangingLogicalIdentity()
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        var history = new DataGridClientHistory();
        await using var old = cluster.Client(leader, history);
        var session = await Open(cluster, old, timeout.Token);
        var otherSession = await Open(cluster, old, timeout.Token);
        var first = Stored(
            await old.ExecuteAsync(
                DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
                timeout.Token
            )
        );
        await cluster.AppliedEverywhere(first.LogIndex, timeout.Token);
        await cluster.Partition(leader, timeout.Token);
        var logical = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(session, 2, value: 9)
        );
        var unknown = await old.ExecuteAsync(logical, timeout.Token);
        Assert.Equal(DataGridClientFailureCode.OutcomeUnknown, unknown.Reply.Failure!.Code);
        Assert.True(unknown.MayHaveBeenAdmitted);
        var unavailable = await old.ExecuteAsync(Read(), timeout.Token);
        Assert.NotNull(unavailable.Reply.Failure);
        Assert.Null(unavailable.Reply.Read);
        var replacement = await cluster.Leader(timeout.Token, excluded: leader);
        await using var majority = cluster.Client(replacement, history);
        var committed = Stored(
            await majority.ExecuteAsync(
                DataGridClientRequest.Mutate(
                    DataGridClientCodecTests.Mutation(otherSession, value: 8)
                ),
                timeout.Token
            )
        );
        cluster.Heal();
        await cluster.AppliedEverywhere(committed.LogIndex, timeout.Token);
        var recovered = await old.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(8, recovered.Reply.Read!.GetItem(0)!.GetValue()[0]);
        Assert.Equal(committed, recovered.Reply.Read.GetItem(0)!.Version);
        var retried = Stored(await old.ExecuteAsync(logical, timeout.Token));
        Assert.True(retried.LogIndex > committed.LogIndex);
        var final = await old.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(9, final.Reply.Read!.GetItem(0)!.GetValue()[0]);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker
                .Check(history.GetOperations(), startsWithEmptySessions: true)
                .Status
        );
        var negative = history.GetOperations().ToArray();
        negative[^1] = negative[^1] with { Outcome = recovered };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check(negative, startsWithEmptySessions: true).Status
        );
        await cluster.AppliedEverywhere(retried.LogIndex, timeout.Token);
    }

    [Fact(Timeout = 90000)]
    public async Task RelativeTtlReplayCloseAndNewSessionCheckThroughPublicClient()
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        var history = new DataGridClientHistory();
        await using var client = cluster.Client(leader, history);
        var session = await Open(cluster, client, timeout.Token);
        var request = DataGridClientRequest.Mutate(
            new(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                session,
                1,
                DataGridExpiryKind.Relative,
                TimeSpan.FromSeconds(10).Ticks,
                [new(DataGridPeerFixture.Key, [7])]
            )
        );
        var version = Stored(await client.ExecuteAsync(request, timeout.Token));
        Assert.Equal(
            TimeSpan.FromSeconds(10),
            (await client.ExecuteAsync(Read(), timeout.Token))
                .Reply.Read!.GetItem(0)!
                .RemainingTimeToLive
        );
        await cluster.AdvanceClock(TimeSpan.FromSeconds(5), timeout.Token);
        Assert.Equal(
            TimeSpan.FromSeconds(5),
            (await client.ExecuteAsync(Read(), timeout.Token))
                .Reply.Read!.GetItem(0)!
                .RemainingTimeToLive
        );
        Assert.Equal(version, Stored(await client.ExecuteAsync(request, timeout.Token)));
        await cluster.AdvanceClock(TimeSpan.FromSeconds(6), timeout.Token);
        Assert.Null((await client.ExecuteAsync(Read(), timeout.Token)).Reply.Read!.GetItem(0));
        var close = await client.ExecuteAsync(
            DataGridClientRequest.CloseSession(session, 2),
            timeout.Token
        );
        Assert.Equal(DataGridMessageKind.CloseSessionResult, close.Reply.Kind);
        var rejected = await client.ExecuteAsync(request, timeout.Token);
        Assert.Equal(DataGridClientFailureCode.SessionExpired, rejected.Reply.Failure!.Code);
        await client.ExecuteAsync(Read(), timeout.Token); // Retire the closed nonce tombstone.
        var next = await Open(cluster, client, timeout.Token);
        Assert.True(next > session);
        var stored = Stored(
            await client.ExecuteAsync(
                DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(next, value: 9)),
                timeout.Token
            )
        );
        await client.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker
                .Check(history.GetOperations(), startsWithEmptySessions: true)
                .Status
        );
        await cluster.AppliedEverywhere(stored.LogIndex, timeout.Token);
    }

    [Fact(Timeout = 90000)]
    public async Task ExpiredSessionCannotReplayRetainedMutationAndAllocatorDoesNotReuseNumber()
    {
        RequireFixture();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        await using var cluster = new ProcessCluster();
        await cluster.StartAll(timeout.Token);
        var leader = await cluster.Leader(timeout.Token);
        var history = new DataGridClientHistory();
        await using var client = cluster.Client(leader, history);
        var session = await Open(cluster, client, timeout.Token);
        var request = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session));
        var original = Stored(await client.ExecuteAsync(request, timeout.Token));
        await cluster.AdvanceClock(TimeSpan.FromMinutes(10) + TimeSpan.FromTicks(1), timeout.Token);
        Assert.Equal(
            original,
            (await client.ExecuteAsync(Read(), timeout.Token)).Reply.Read!.GetItem(0)!.Version
        );
        var expired = await client.ExecuteAsync(request, timeout.Token);
        Assert.Equal(DataGridClientFailureCode.SessionExpired, expired.Reply.Failure!.Code);
        var next = await Open(cluster, client, timeout.Token);
        Assert.True(next > session);
        var newVersion = Stored(
            await client.ExecuteAsync(
                DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(next, value: 9)),
                timeout.Token
            )
        );
        Assert.True(newVersion.LogIndex > original.LogIndex);
        await client.ExecuteAsync(Read(), timeout.Token);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker
                .Check(history.GetOperations(), startsWithEmptySessions: true)
                .Status
        );
        await cluster.AppliedEverywhere(newVersion.LogIndex, timeout.Token);
    }

    private static void AssertHistoryRejectsStaleRead(
        DataGridClientHistory history,
        DataGridLocalEntry stale
    )
    {
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check(history.GetOperations()).Status
        );
        var negative = history.GetOperations().ToArray();
        negative[^1] = negative[^1] with
        {
            Outcome = new(
                DataGridClientReply.Completed(
                    DataGridMessageKind.ReadResult,
                    read: new(negative[^1].Outcome!.Reply.Read!.CapturedAt, [stale])
                ),
                true,
                []
            ),
        };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check(negative).Status
        );
    }

    private static void RequireFixture() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux()
                && Environment.GetEnvironmentVariable("HOSTLOOM_DATA_GRID_PROCESSES") == "1",
            "Owned Linux member-process experiments require HOSTLOOM_DATA_GRID_PROCESSES=1."
        );

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(token);
        source.CancelAfter(TimeSpan.FromSeconds(75));
        return source;
    }

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

    private static async Task<long> Open(
        ProcessCluster cluster,
        GridClient client,
        CancellationToken token
    )
    {
        var result = await client.ExecuteAsync(
            DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                new(cluster.Clock + TimeSpan.FromSeconds(4).Ticks)
            ),
            token
        );
        Assert.Null(result.Reply.Failure);
        return result.Reply.Result!.SessionNumber;
    }

    private static DataGridEntryVersion Stored(DataGridClientOutcome result)
    {
        Assert.Null(result.Reply.Failure);
        Assert.Equal(DataGridMutationStatus.Stored, result.Reply.Result!.GetItem(0).Status);
        return result.Reply.Result.GetItem(0).Version;
    }

    private sealed class ProcessCluster : IAsyncDisposable
    {
        private readonly DataGridPeerFixture _fixture = new();
        private readonly X509Certificate2 _client;
        private readonly string _directory = Path.Combine(
            Path.GetTempPath(),
            "hostloom-grid-process-" + Guid.NewGuid().ToString("N")
        );
        private readonly int[] _ports;
        private readonly Dictionary<(int From, int To), DataGridPeerFaultProxy> _links = [];
        private readonly long[] _epochs = new long[3];
        private readonly DataGridBootstrapConfiguration _config;
        internal ProcessNode?[] Nodes { get; } = new ProcessNode?[3];
        internal long Clock { get; private set; } = DateTimeOffset.UtcNow.Ticks;

        internal ProcessCluster()
        {
            if (!OperatingSystem.IsLinux())
                throw new PlatformNotSupportedException();
            Directory.CreateDirectory(
                _directory,
                UnixFileMode.UserRead | UnixFileMode.UserWrite | UnixFileMode.UserExecute
            );
            _config = ProbeConfiguration.Create(
                Enumerable
                    .Range(0, 3)
                    .Select(i => (int)_fixture.Configuration.GetMember(i).Port)
                    .ToArray()
            );
            Assert.Equal(_fixture.Configuration.Encode(), _config.Encode());
            _client = _fixture.Issue("catalog-process-client.example", clientOnly: true);
            File.WriteAllBytes(
                Path.Combine(_directory, "root.cer"),
                _fixture.Root.Export(X509ContentType.Cert)
            );
            File.WriteAllBytes(
                Path.Combine(_directory, "client.cer"),
                _client.Export(X509ContentType.Cert)
            );
            for (var i = 0; i < 3; i++)
            {
                File.WriteAllBytes(
                    Path.Combine(_directory, $"member-{i}.pfx"),
                    _fixture.Certificates[i].Export(X509ContentType.Pkcs12)
                );
                File.WriteAllBytes(
                    Path.Combine(_directory, $"member-{i}.cer"),
                    _fixture.Certificates[i].Export(X509ContentType.Cert)
                );
                var node = Path.Combine(_directory, $"node-{i}");
                Directory.CreateDirectory(node);
                using var store = DataGridFileStore.Bootstrap(node, _config, i);
            }
            var listeners = Enumerable
                .Range(0, 3)
                .Select(_ => new TcpListener(IPAddress.Loopback, 0))
                .ToArray();
            try
            {
                foreach (var listener in listeners)
                    listener.Start();
                _ports = listeners
                    .Select(listener => ((IPEndPoint)listener.LocalEndpoint).Port)
                    .ToArray();
            }
            finally
            {
                foreach (var listener in listeners)
                    listener.Stop();
            }
            foreach (var listener in _fixture.Listeners)
                listener.Stop();
            for (var source = 0; source < 3; source++)
            for (var target = 0; target < 3; target++)
                if (source != target)
                    _links.Add(
                        (source, target),
                        new(source, target, _config.GetMember(target).Port)
                    );
        }

        internal async Task StartAll(CancellationToken token)
        {
            for (var i = 0; i < 3; i++)
                await Start(i, token);
        }

        internal async Task Start(
            int member,
            CancellationToken token,
            DataGridStorageStep? pause = null
        )
        {
            Assert.Null(Nodes[member]);
            var node = new ProcessNode(
                _directory,
                member,
                ++_epochs[member],
                _config,
                _ports[member],
                Clock,
                pause
            );
            try
            {
                await node.Ready.WaitAsync(token);
                Nodes[member] = node;
            }
            catch
            {
                await node.DisposeAsync();
                throw;
            }
        }

        internal async Task Kill(int member)
        {
            var node = Nodes[member]!;
            await node.DisposeAsync();
            Nodes[member] = null;
        }

        internal (Guid Generation, long Snapshot) RecoveredRoot(int member)
        {
            Assert.Null(Nodes[member]);
            using var store = DataGridFileStore.Open(
                Path.Combine(_directory, $"node-{member}"),
                _config,
                member
            );
            return (store.GenerationId, store.Image.SnapshotIndex);
        }

        internal async Task StopAll(CancellationToken token)
        {
            for (var i = 0; i < 3; i++)
                if (Nodes[i] is { } node)
                {
                    await node.Stop(token);
                    await node.DisposeAsync();
                    Nodes[i] = null;
                }
        }

        internal async Task<int> Leader(CancellationToken token, int excluded = -1)
        {
            using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
            do
            {
                for (var i = 0; i < 3; i++)
                    if (
                        i != excluded
                        && Nodes[i] is { } node
                        && (await node.Status(token)).Role == RaftRole.Leader
                    )
                        return i;
            } while (await poll.WaitForNextTickAsync(token));
            throw new InvalidOperationException("No leader.");
        }

        internal async Task AppliedEverywhere(long index, CancellationToken token)
        {
            using var poll = new PeriodicTimer(TimeSpan.FromMilliseconds(50));
            do
            {
                var applied = true;
                foreach (var node in Nodes.Where(node => node is not null))
                    applied &= (await node!.Status(token)).Applied >= index;
                if (applied)
                    return;
            } while (await poll.WaitForNextTickAsync(token));
        }

        internal async Task AdvanceClock(TimeSpan delta, CancellationToken token)
        {
            Clock += delta.Ticks;
            foreach (var node in Nodes.Where(node => node is not null))
                await node!.Command("clock " + Clock.ToString(CultureInfo.InvariantCulture), token);
        }

        internal async Task Partition(int member, CancellationToken token)
        {
            Assert.True(_links.Values.Sum(link => link.ForwardedBytes) > 0);
            foreach (var (route, link) in _links)
                if (route.From == member || route.To == member)
                    link.SetEnabled(false);
            await Task.WhenAll(
                    _links.Where(pair => pair.Key.From == member).Select(pair => pair.Value.Refused)
                )
                .WaitAsync(token);
        }

        internal void Heal()
        {
            foreach (var link in _links.Values)
            {
                Assert.Null(link.Failure);
                link.SetEnabled(true);
            }
        }

        internal GridClient Client(int member, DataGridClientHistory? history = null) =>
            new(
                new(
                    _config,
                    ProbeConfiguration.Principal,
                    _client,
                    _fixture.Root,
                    Enumerable
                        .Range(0, 3)
                        .Select(i => new DataGridClientEndpoint(
                            _config.GetMember(i).MemberId,
                            _config.GetMember(i).DnsName,
                            (ushort)_ports[i],
                            _fixture.Fingerprints[i],
                            IPAddress.Loopback
                        ))
                        .ToArray(),
                    SHA256.HashData("hostloom-process-probe-v1"u8),
                    member
                ),
                history
            );

        public async ValueTask DisposeAsync()
        {
            await Task.WhenAll(
                Nodes.Where(node => node is not null).Select(node => node!.DisposeAsync().AsTask())
            );
            await Task.WhenAll(_links.Values.Select(link => link.DisposeAsync().AsTask()));
            Assert.All(_links.Values, link => Assert.Null(link.Failure));
            _client.Dispose();
            _fixture.Dispose();
            Directory.Delete(_directory, recursive: true);
        }
    }

    private sealed record NodeStatus(RaftRole Role, long Commit, long Applied, long Snapshot);

    private sealed class ProcessNode : IAsyncDisposable
    {
        private readonly Process _process;
        private readonly CancellationTokenSource _io = new(TimeSpan.FromMinutes(3));
        private readonly Task _reader;
        private readonly Task<string> _errors;
        private readonly Channel<string> _replies = Channel.CreateBounded<string>(16);
        private readonly SemaphoreSlim _commands = new(1, 1);
        private readonly TaskCompletionSource _ready = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private TaskCompletionSource _paused = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private readonly TaskCompletionSource<int> _storagePaused = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private bool _disposed;
        internal Task Ready => _ready.Task;
        internal Task Paused => _paused.Task;
        internal Task<int> StoragePaused => _storagePaused.Task;

        internal ProcessNode(
            string directory,
            int member,
            long epoch,
            DataGridBootstrapConfiguration config,
            int clientPort,
            long clock,
            DataGridStorageStep? pause
        )
        {
            var start = new ProcessStartInfo("dotnet")
            {
                UseShellExecute = false,
                RedirectStandardInput = true,
                RedirectStandardOutput = true,
                RedirectStandardError = true,
            };
            start.ArgumentList.Add(
                Path.Combine(AppContext.BaseDirectory, "HostLoom.DataGrid.ProcessProbe.dll")
            );
            foreach (
                var argument in new[]
                {
                    directory,
                    member.ToString(CultureInfo.InvariantCulture),
                    epoch.ToString(CultureInfo.InvariantCulture),
                    config.GetMember(0).Port.ToString(CultureInfo.InvariantCulture),
                    config.GetMember(1).Port.ToString(CultureInfo.InvariantCulture),
                    config.GetMember(2).Port.ToString(CultureInfo.InvariantCulture),
                    clientPort.ToString(CultureInfo.InvariantCulture),
                    clock.ToString(CultureInfo.InvariantCulture),
                    "owned-test",
                }
            )
                start.ArgumentList.Add(argument);
            if (pause is { } step)
                start.ArgumentList.Add(((int)step).ToString(CultureInfo.InvariantCulture));
            _process = Process.Start(start)!;
            _errors = _process.StandardError.ReadToEndAsync(_io.Token);
            _reader = Pump();
        }

        private async Task Pump()
        {
            Exception? failure = null;
            try
            {
                while (await _process.StandardOutput.ReadLineAsync(_io.Token) is { } line)
                {
                    if (line == "READY")
                        _ready.TrySetResult();
                    else if (line == "PAUSED")
                        _paused.TrySetResult();
                    else if (line.StartsWith("STORAGEPAUSED ", StringComparison.Ordinal))
                        _storagePaused.TrySetResult(
                            int.Parse(line.AsSpan(14), CultureInfo.InvariantCulture)
                        );
                    else if (!_replies.Writer.TryWrite(line))
                        throw new InvalidOperationException("Probe control response capacity.");
                }
                if (!_ready.Task.IsCompleted)
                    failure = new IOException("Probe startup failed: " + await _errors);
            }
            catch (Exception exception)
            {
                failure = exception;
            }
            finally
            {
                if (failure is not null)
                {
                    _ready.TrySetException(failure);
                    _paused.TrySetException(failure);
                    _storagePaused.TrySetException(failure);
                }
                _replies.Writer.TryComplete(failure);
            }
        }

        internal async Task<string> Command(string command, CancellationToken token)
        {
            await _commands.WaitAsync(token);
            try
            {
                await _process.StandardInput.WriteLineAsync(command.AsMemory(), token);
                await _process.StandardInput.FlushAsync(token);
                return await _replies.Reader.ReadAsync(token);
            }
            finally
            {
                _commands.Release();
            }
        }

        internal async Task Arm(CancellationToken token)
        {
            _paused = new(TaskCreationOptions.RunContinuationsAsynchronously);
            Assert.Equal("ARMED", await Command("pause", token));
        }

        internal async Task<NodeStatus> Status(CancellationToken token)
        {
            var parts = (await Command("status", token)).Split(' ');
            Assert.Equal("STATUS", parts[0]);
            Assert.Equal("0", parts[5]);
            return new(
                (RaftRole)int.Parse(parts[1], CultureInfo.InvariantCulture),
                long.Parse(parts[3], CultureInfo.InvariantCulture),
                long.Parse(parts[4], CultureInfo.InvariantCulture),
                long.Parse(parts[6], CultureInfo.InvariantCulture)
            );
        }

        internal async Task Stop(CancellationToken token)
        {
            await _process.StandardInput.WriteLineAsync("stop".AsMemory(), token);
            await _process.StandardInput.FlushAsync(token);
            await _process.WaitForExitAsync(token);
            Assert.Equal(0, _process.ExitCode);
            Assert.Equal(string.Empty, await _errors);
        }

        public async ValueTask DisposeAsync()
        {
            if (_disposed)
                return;
            _disposed = true;
            if (!_process.HasExited)
                _process.Kill(entireProcessTree: true);
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(10));
            await _process.WaitForExitAsync(cleanup.Token);
            await _reader.WaitAsync(cleanup.Token);
            var errors = await _errors.WaitAsync(cleanup.Token);
            Assert.Equal(string.Empty, errors);
            _io.Dispose();
            _process.Dispose();
            _commands.Dispose();
        }
    }
}
