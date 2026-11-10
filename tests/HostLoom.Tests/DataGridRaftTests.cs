using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Testing;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridRaftTests
{
    private static readonly Guid Cluster = new("10112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Map = new("00112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Principal = new("20112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Nonce = new("40112233-4455-6677-8899-aabbccddeeff");
    private static readonly DataGridKey Key = DataGridKey.FromUtf8("catalog:eu");

    [Fact]
    public void ElectionSendsNothingUntilSelfVoteIsPersistedAndIgnoresStaleCompletions()
    {
        var config = Configuration();
        var node = new RaftNode(config, 0, 1, new(config));
        var timer = Assert.Single(node.Start().OfType<RaftTimer>());
        var election = node.Timeout(timer.Token, default);
        var write = Assert.Single(election.OfType<RaftWrite>());
        Assert.Empty(election.OfType<RaftSend>());
        Assert.Equal(0, node.Durable.Term);
        Assert.Empty(node.Persisted(new(9, 9), true));
        var votes = node.Persisted(write.Token, true).OfType<RaftSend>().ToArray();
        Assert.Equal(2, votes.Length);
        Assert.All(votes, vote => Assert.Equal(RaftMessageKind.VoteRequest, vote.Message.Kind));
        Assert.Equal(0, node.Durable.Vote);
        var restarted = new RaftNode(config, 0, 2, write.Image);
        var next = Assert.Single(restarted.Start().OfType<RaftTimer>());
        var nextWrite = Assert.Single(restarted.Timeout(next.Token, default).OfType<RaftWrite>());
        Assert.Empty(restarted.Persisted(write.Token, true));
        Assert.True(restarted.IsBusy);
        Assert.Equal(2, nextWrite.Image.Term);
    }

    [Fact]
    public void VoteGrantSurvivesRestartAndRefusesAnotherCandidateInSameTerm()
    {
        var config = Configuration();
        var node = new RaftNode(config, 0, 1, new(config));
        var persistTerm = Assert.Single(
            node.Receive(Message(RaftMessageKind.VoteRequest, 1, 0, 3), default).OfType<RaftWrite>()
        );
        var grantEffects = node.Persisted(persistTerm.Token, true);
        Assert.Empty(grantEffects.OfType<RaftSend>());
        var persistVote = Assert.Single(grantEffects.OfType<RaftWrite>());
        Assert.Equal(1, persistVote.Image.Vote);
        Assert.True(
            Assert
                .Single(node.Persisted(persistVote.Token, true).OfType<RaftSend>())
                .Message.Accepted
        );
        var restarted = new RaftNode(config, 0, 2, persistVote.Image);
        var rejection = Assert.Single(
            restarted
                .Receive(Message(RaftMessageKind.VoteRequest, 2, 0, 3), default)
                .OfType<RaftSend>()
        );
        Assert.False(rejection.Message.Accepted);
        Assert.Equal(1, restarted.Durable.Vote);
    }

    [Fact]
    public void ForeignMemberAndConfigurationCannotRaiseTerm()
    {
        var config = Configuration();
        var node = new RaftNode(config, 0, 1, new(config));
        Assert.Empty(node.Receive(Message(RaftMessageKind.VoteRequest, 3, 0, 99), default));
        Assert.Empty(
            node.Receive(
                new(
                    RaftMessageKind.VoteRequest,
                    1,
                    0,
                    99,
                    new(1, 1),
                    Guid.Empty,
                    config.ComputeDigest()
                ),
                default
            )
        );
        Assert.Empty(
            node.Receive(
                new(RaftMessageKind.VoteRequest, 1, 0, 99, new(1, 1), Cluster, new byte[32]),
                default
            )
        );
        Assert.Equal(0, node.CurrentTerm);
    }

    [Fact]
    public void MajorityPersistsWritesAndReadCapturesItsCommittedBarrier()
    {
        var sim = new DataGridRaftSimulation(Configuration(), 1);
        var leader = Leader(sim);
        var session = Open(sim, leader);
        var stored = Applied(sim, leader, Mutation(sim, session, 1, [7]));
        Assert.Equal(DataGridMutationStatus.Stored, stored.Result!.GetItem(0).Status);
        var read = sim.Read(leader, Map, Key);
        var value = Done(sim, read);
        Assert.Equal(DataGridRaftOutcomeKind.Read, value.Kind);
        Assert.True(value.Index > stored.Index);
        Assert.Equal(new byte[] { 7 }, value.Entry!.GetValue());
        Assert.Equal(stored.Result.GetItem(0).Version, value.Entry.Version);
        sim.RunUntil(s => Enumerable.Range(0, 3).All(n => s.Status(n).AppliedIndex >= value.Index));
        sim.CheckInvariants();
    }

    [Fact]
    public void IsolatedLeaderCannotCompleteWritesOrReadsAndItsSuffixIsReplaced()
    {
        var sim = new DataGridRaftSimulation(Configuration(), 7);
        var old = Leader(sim);
        var session = Open(sim, old);
        Applied(sim, old, Mutation(sim, session, 1, [1]));
        sim.Advance(200);
        sim.Isolate(old);
        var uncertainCommand = Mutation(sim, session, 2, [2]);
        var uncertain = sim.Propose(old, uncertainCommand);
        sim.Advance(2);
        var read = sim.Read(old, Map, Key);
        var replacement = Leader(sim, n => n != old);
        Applied(sim, replacement, Mutation(sim, session, 2, [3]));
        Assert.Equal(DataGridRaftOutcomeKind.OutcomeUnknown, Done(sim, uncertain).Kind);
        Assert.Equal(DataGridRaftOutcomeKind.Unavailable, Done(sim, read).Kind);
        sim.Isolate(old, false);
        sim.RunUntil(s =>
            s.Status(old).AppliedIndex == s.Status(replacement).CommitIndex
            && s.Status(old).Role == DataGridRaftRole.Follower
        );
        Assert.Equal(new byte[] { 3 }, sim.ReadLocal(old, Map, Key)!.GetValue());
        Assert.Equal(
            DataGridApplyStatus.RequestIdentityMismatch,
            Applied(sim, replacement, uncertainCommand).Result!.Status
        );
    }

    [Fact]
    public void LostReplyRetryAfterLeaderChangeReturnsOriginalVersion()
    {
        var sim = new DataGridRaftSimulation(Configuration(), 123456);
        var old = Leader(sim);
        var session = Open(sim, old);
        var command = Mutation(sim, session, 1, [9]);
        var request = sim.Propose(old, command);
        sim.LoseReply(request);
        sim.RunUntil(s => s.ReadLocal(old, Map, Key) is not null);
        var version = sim.ReadLocal(old, Map, Key)!.Version;
        sim.Stop(old);
        Assert.Equal(DataGridRaftOutcomeKind.OutcomeUnknown, sim.Outcome(request)!.Kind);
        var leader = Leader(sim);
        var retry = Applied(sim, leader, command);
        Assert.Equal(version, retry.Result!.GetItem(0).Version);
        sim.Restart(old);
        sim.RunUntil(s => s.Status(old).AppliedIndex >= retry.Index);
        Assert.Equal(version, sim.ReadLocal(old, Map, Key)!.Version);
    }

    [Fact]
    public void TwoUnavailableMembersBlockProgressAndFullRestartPreservesAcknowledgedPrefix()
    {
        var sim = new DataGridRaftSimulation(Configuration(), 8);
        var leader = Leader(sim);
        var session = Open(sim, leader);
        var command = Mutation(sim, session, 1, [4]);
        var stored = Applied(sim, leader, command);
        sim.Advance(300);
        foreach (var peer in Enumerable.Range(0, 3).Where(n => n != leader))
            sim.Stop(peer);
        var stalled = sim.Propose(leader, DataGridCommittedCommand.AdvanceTime(sim.UtcTime));
        Assert.Equal(DataGridRaftOutcomeKind.OutcomeUnknown, Done(sim, stalled).Kind);
        sim.Stop(leader);
        for (var i = 0; i < 3; i++)
            sim.Restart(i);
        leader = Leader(sim);
        var retry = Applied(sim, leader, command);
        Assert.Equal(stored.Result!.GetItem(0).Version, retry.Result!.GetItem(0).Version);
        Assert.Equal(new byte[] { 4 }, Done(sim, sim.Read(leader, Map, Key)).Entry!.GetValue());
    }

    [Theory]
    [InlineData(DataGridJournalFault.BeforeAppend)]
    [InlineData(DataGridJournalFault.AfterAppend)]
    public void UncertainPersistenceFaultsMemberAndRestartUsesActualStore(
        DataGridJournalFault fault
    )
    {
        var sim = new DataGridRaftSimulation(Configuration(), 17);
        var leader = Leader(sim);
        var session = Open(sim, leader);
        var previous = sim.Status(leader).LastIndex;
        sim.FailNextWrite(leader, fault);
        var command = Mutation(sim, session, 1, [6]);
        var request = sim.Propose(leader, command);
        sim.RunUntil(s => s.Status(leader).Role == DataGridRaftRole.Faulted);
        Assert.Equal(DataGridRaftOutcomeKind.OutcomeUnknown, sim.Outcome(request)!.Kind);
        Assert.Equal(
            previous + (fault == DataGridJournalFault.AfterAppend ? 1 : 0),
            sim.Status(leader).LastIndex
        );
        sim.Restart(leader);
        var newLeader = Leader(sim);
        Assert.Equal(
            DataGridMutationStatus.Stored,
            Applied(sim, newLeader, command).Result!.GetItem(0).Status
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void CrashBeforePersistenceAcknowledgementDoesNotCreateSuccess(bool persistPending)
    {
        var sim = new DataGridRaftSimulation(Configuration(), 19);
        var leader = Leader(sim);
        var session = Open(sim, leader);
        var previous = sim.Status(leader).LastIndex;
        sim.HoldWrites(leader, true);
        var command = Mutation(sim, session, 1, [8]);
        var request = sim.Propose(leader, command);
        sim.Advance(50);
        Assert.Null(sim.Outcome(request));
        sim.Stop(leader, persistPending);
        Assert.Equal(previous + (persistPending ? 1 : 0), sim.Status(leader).LastIndex);
        Assert.Equal(DataGridRaftOutcomeKind.OutcomeUnknown, sim.Outcome(request)!.Kind);
        sim.HoldWrites(leader, false);
        sim.Restart(leader);
        leader = Leader(sim);
        Assert.Equal(
            DataGridMutationStatus.Stored,
            Applied(sim, leader, command).Result!.GetItem(0).Status
        );
    }

    [Fact]
    public void SnapshotCatchesUpStoppedMemberAndPreservesItsOwnHardState()
    {
        var sim = new DataGridRaftSimulation(Configuration(), 24);
        var leader = Leader(sim);
        var lagging = (leader + 1) % 3;
        var session = Open(sim, leader);
        var ownVote = sim.Status(lagging).VotedFor;
        sim.Stop(lagging);
        for (ulong i = 1; i <= 12; i++)
            Applied(sim, leader, Mutation(sim, session, i, [(byte)i]));
        sim.Advance(300);
        var commit = sim.Status(leader).CommitIndex;
        sim.Compact(leader);
        sim.RunUntil(s => !s.Status(leader).Busy);
        Assert.Equal(commit, sim.StoredImage(leader).SnapshotIndex);
        sim.Restart(lagging);
        sim.RunUntil(s => s.Status(lagging).AppliedIndex >= commit);
        Assert.True(sim.StoredImage(lagging).SnapshotIndex > 0);
        Assert.Equal(ownVote, sim.StoredImage(lagging).Vote);
        Assert.Equal(sim.Status(leader).Term, sim.Status(lagging).DurableTerm);
        Assert.Equal(new byte[] { 12 }, sim.ReadLocal(lagging, Map, Key)!.GetValue());
        sim.Stop(lagging);
        sim.Restart(lagging);
        Assert.Equal(new byte[] { 12 }, sim.ReadLocal(lagging, Map, Key)!.GetValue());
    }

    [Fact]
    public void FollowerDoesNotCommitItsUnmatchedSuffixFromHeartbeat()
    {
        var config = Configuration();
        var commands = new[] { Entry(1, 1), Entry(2, 1), Entry(3, 2) };
        var node = new RaftNode(config, 1, 1, new(config, 3, 0, commands, 1));
        var effects = node.Receive(
            Message(RaftMessageKind.AppendRequest, 0, 1, 3, index: 1, previousTerm: 1, commit: 3),
            default
        );
        Assert.Empty(effects.OfType<RaftWrite>());
        Assert.Equal(1, node.Durable.CommitIndex);
        Assert.Equal(1, Assert.Single(effects.OfType<RaftSend>()).Message.Index);
    }

    [Fact]
    public void OnlyCurrentTermMajorityCanAdvanceLeaderCommit()
    {
        var config = Configuration();
        var node = new RaftNode(
            config,
            0,
            1,
            new(config, 1, entries: Enumerable.Range(1, 128).Select(i => Entry(i, 1)))
        );
        var election = Assert.Single(node.Start().OfType<RaftTimer>());
        var write = Assert.Single(node.Timeout(election.Token, default).OfType<RaftWrite>());
        var vote = node.Persisted(write.Token, true).OfType<RaftSend>().First().Message;
        var becomeLeader = node.Receive(
            Message(RaftMessageKind.VoteResponse, vote.To, 0, 2, token: vote.Token, accepted: true),
            default
        );
        var noop = Assert.Single(becomeLeader.OfType<RaftWrite>());
        var probe = node.Persisted(noop.Token, true).OfType<RaftSend>().First().Message;
        var resend = node.Receive(
            Message(
                RaftMessageKind.AppendResponse,
                probe.To,
                0,
                2,
                token: probe.Token,
                conflict: 1
            ),
            default
        );
        var firstBatch = Assert.Single(resend.OfType<RaftSend>()).Message;
        Assert.Equal(128, firstBatch.Entries.Count);
        Assert.Empty(
            node.Receive(
                Message(
                    RaftMessageKind.AppendResponse,
                    probe.To,
                    0,
                    2,
                    token: probe.Token,
                    accepted: true,
                    index: 129
                ),
                default
            )
        );
        var ack = node.Receive(
            Message(
                RaftMessageKind.AppendResponse,
                probe.To,
                0,
                2,
                token: firstBatch.Token,
                accepted: true,
                index: 128
            ),
            default
        );
        Assert.Empty(ack.OfType<RaftWrite>());
        Assert.Equal(0, node.Durable.CommitIndex);
        var barrier = Assert.Single(ack.OfType<RaftSend>()).Message;
        Assert.Equal(129, Assert.Single(barrier.Entries).Index);
        var commit = Assert.Single(
            node.Receive(
                    Message(
                        RaftMessageKind.AppendResponse,
                        probe.To,
                        0,
                        2,
                        token: barrier.Token,
                        accepted: true,
                        index: 129
                    ),
                    default
                )
                .OfType<RaftWrite>()
        );
        Assert.Equal(129, commit.Image.CommitIndex);
        Assert.Equal(0, node.Machine.LastAppliedIndex);
        node.Persisted(commit.Token, true);
        Assert.Equal(129, node.Machine.LastAppliedIndex);
    }

    [Theory]
    [InlineData(1u)]
    [InlineData(7u)]
    [InlineData(123456u)]
    public void SeededHistoriesSurviveNetworkFaultsCompactionAndLeaderRestarts(uint seed)
    {
        var sim = new DataGridRaftSimulation(Configuration(), seed);
        var leader = Leader(sim);
        var session = Open(sim, leader);
        for (ulong i = 1; i <= 50; i++)
        {
            if (i % 10 == 0)
            {
                sim.Advance(250);
                sim.Stop(leader);
                var previous = leader;
                leader = Leader(sim);
                sim.Restart(previous);
            }
            var current = sim.ReadLocal(leader, Map, Key);
            var kind = (i % 4) switch
            {
                0 => DataGridMutationKind.CompareRemove,
                1 => DataGridMutationKind.Put,
                2 => DataGridMutationKind.CompareExchange,
                _ => DataGridMutationKind.PutIfAbsent,
            };
            var version = current?.Version ?? new DataGridEntryVersion(Cluster, 1, 0);
            var command = Mutation(
                sim,
                session,
                i,
                kind == DataGridMutationKind.CompareRemove ? [] : [(byte)i],
                kind,
                version
            );
            var request = sim.Propose(leader, command);
            sim.RunUntil(s => s.PendingMessageCount > 0);
            if (i % 3 == 0)
                sim.DuplicateNextMessage();
            if (i % 5 == 0)
                sim.DelayNextMessage(230);
            if (i % 7 == 0)
                sim.DropNextMessage();
            Assert.Equal(DataGridRaftOutcomeKind.Applied, Done(sim, request).Kind);
            Assert.Equal(DataGridRaftOutcomeKind.Read, Done(sim, sim.Read(leader, Map, Key)).Kind);
            if (i % 9 == 0)
            {
                sim.Compact(leader);
                sim.RunUntil(s => !s.Status(leader).Busy);
            }
            sim.CheckInvariants();
        }
        Assert.False(sim.TraceTruncated);
    }

    [Fact]
    public void HistoryOracleRejectsSuccessBeforeQuorumAndStaleReadPrefix()
    {
        var sim = new DataGridRaftSimulation(Configuration(), 7);
        var leader = Leader(sim);
        var session = Open(sim, leader);
        sim.Isolate(leader);
        var request = sim.Propose(leader, Mutation(sim, session, 1, [1]));
        var exception = Assert.Throws<InvalidDataException>(() =>
            sim.ValidateInjectedCompletion(
                leader,
                new(request, RaftCompletionKind.Applied, sim.Status(leader).LastIndex + 1)
            )
        );
        Assert.Contains("seed=7", exception.Message, StringComparison.Ordinal);
        Assert.Contains("majority persistence", exception.Message, StringComparison.Ordinal);

        sim = new(Configuration(), 7);
        leader = Leader(sim);
        session = Open(sim, leader);
        Applied(sim, leader, Mutation(sim, session, 1, [1]));
        var read = sim.Read(leader, Map, Key);
        var index = sim.Status(leader).CommitIndex;
        Assert.Throws<InvalidDataException>(() =>
            sim.ValidateInjectedCompletion(leader, new(read, RaftCompletionKind.Read, index))
        );
    }

    [Fact]
    public void InvalidPersistentCommitAndLogGapsFailClosed()
    {
        var config = Configuration();
        Assert.Throws<InvalidDataException>(() =>
            new RaftDurableImage(config, 1, entries: [Entry(2, 1)])
        );
        Assert.Throws<InvalidDataException>(() =>
            new RaftDurableImage(config, 1, entries: [Entry(1, 1)], commitIndex: 2)
        );
        Assert.Throws<InvalidDataException>(() => new RaftDurableImage(config, 1, vote: 3));
        Assert.Throws<InvalidDataException>(() =>
            new RaftDurableImage(config, 1, entries: [Entry(1, 2)])
        );
    }

    [Fact]
    public void FollowerAcknowledgesAppendOnlyAfterPersistenceAndReplacesConflictingSuffix()
    {
        var config = Configuration();
        var node = new RaftNode(
            config,
            1,
            1,
            new(config, 3, 0, [Entry(1, 1), Entry(2, 2), Entry(3, 2)], 1)
        );
        var append = new RaftMessage(
            RaftMessageKind.AppendRequest,
            0,
            1,
            3,
            new(1, 2),
            Cluster,
            config.ComputeDigest(),
            index: 1,
            previousTerm: 1,
            commit: 3,
            entries: [Entry(2, 3), Entry(3, 3)]
        );
        var effects = node.Receive(append, default);
        Assert.Empty(effects.OfType<RaftSend>());
        var write = Assert.Single(effects.OfType<RaftWrite>());
        Assert.Equal(1, node.Machine.LastAppliedIndex);
        var ack = Assert.Single(node.Persisted(write.Token, true).OfType<RaftSend>()).Message;
        Assert.True(ack.Accepted);
        Assert.Equal(3, ack.Index);
        Assert.Equal(3, node.Machine.LastAppliedIndex);
        Assert.Equal(3, node.Durable.EntryAt(2).Term);
        var duplicate = node.Receive(append, default);
        Assert.Empty(duplicate.OfType<RaftWrite>());
        Assert.True(Assert.Single(duplicate.OfType<RaftSend>()).Message.Accepted);
    }

    [Fact]
    public void HigherTermDuringPendingWriteSuppressesItsOldTermContinuation()
    {
        var config = Configuration();
        var node = new RaftNode(config, 0, 1, new(config));
        var election = Assert.Single(node.Start().OfType<RaftTimer>());
        var pending = Assert.Single(node.Timeout(election.Token, default).OfType<RaftWrite>());
        Assert.Empty(
            node.Receive(Message(RaftMessageKind.VoteRequest, 1, 0, 5), default).OfType<RaftSend>()
        );
        Assert.Equal(RaftRole.Follower, node.Role);
        var effects = node.Persisted(pending.Token, true);
        Assert.Empty(effects.OfType<RaftSend>());
        var term = Assert.Single(effects.OfType<RaftWrite>());
        Assert.Equal(5, term.Image.Term);
        Assert.Null(term.Image.Vote);
        var vote = Assert.Single(node.Persisted(term.Token, true).OfType<RaftWrite>());
        var reply = Assert.Single(node.Persisted(vote.Token, true).OfType<RaftSend>()).Message;
        Assert.Equal(5, reply.Term);
        Assert.True(reply.Accepted);
        Assert.Equal(1, node.Durable.Vote);
    }

    [Fact]
    public void ConcurrentCandidatesCannotLeadInOneTermAndEventuallyRecover()
    {
        var sim = new DataGridRaftSimulation(Configuration(), 333);
        for (var node = 0; node < 3; node++)
            sim.ForceElection(node);
        sim.Advance(100);
        Assert.All(Enumerable.Range(0, 3), n => Assert.Equal(n, sim.Status(n).VotedFor));
        Assert.DoesNotContain(
            Enumerable.Range(0, 3),
            n => sim.Status(n).Role == DataGridRaftRole.Leader
        );
        var leader = Leader(sim);
        Assert.True(sim.Status(leader).Term > 1);
        sim.CheckInvariants();
    }

    [Fact]
    public void CompactionPreservesUncommittedSuffixAndLocalHardState()
    {
        var config = Configuration();
        var node = new RaftNode(
            config,
            1,
            1,
            new(config, 4, 2, [Entry(1, 1), Entry(2, 2), Entry(3, 3)], 2)
        );
        var write = Assert.Single(node.Compact(Nonce).OfType<RaftWrite>());
        Assert.Equal(4, write.Image.Term);
        Assert.Equal(2, write.Image.Vote);
        Assert.Equal(2, write.Image.SnapshotIndex);
        Assert.Equal(3, Assert.Single(write.Image.Entries).Index);
        node.Persisted(write.Token, true);
        var restarted = new RaftNode(config, 1, 2, write.Image);
        Assert.Equal(2, restarted.Machine.LastAppliedIndex);
        Assert.Equal(3, restarted.Durable.LastIndex);
    }

    [Fact]
    public void FollowerCompactionWithInFlightOldProbeStillAllowsCatchUp()
    {
        var sim = new DataGridRaftSimulation(Configuration(), 55);
        var leader = Leader(sim);
        var peer = (leader + 1) % 3;
        var session = Open(sim, leader);
        Applied(sim, leader, Mutation(sim, session, 1, [1]));
        sim.Advance(200);
        sim.Compact(peer);
        sim.RunUntil(s => !s.Status(peer).Busy);
        sim.Isolate(peer);
        var write = Applied(sim, leader, Mutation(sim, session, 2, [2]));
        sim.Advance(500);
        sim.Isolate(peer, false);
        sim.RunUntil(s => s.Status(peer).AppliedIndex >= write.Index);
        Assert.Equal(new byte[] { 2 }, sim.ReadLocal(peer, Map, Key)!.GetValue());
    }

    [Fact]
    public void RetainedConditionalRejectionAndDeleteCannotChangeAfterFailover()
    {
        var sim = new DataGridRaftSimulation(Configuration(), 56);
        var leader = Leader(sim);
        var session = Open(sim, leader);
        var first = Applied(sim, leader, Mutation(sim, session, 1, [1])).Result!.GetItem(0).Version;
        var rejection = Mutation(sim, session, 2, [2], DataGridMutationKind.PutIfAbsent);
        Assert.Equal(
            DataGridMutationStatus.AlreadyExists,
            Applied(sim, leader, rejection).Result!.GetItem(0).Status
        );
        sim.Stop(leader);
        var previous = leader;
        leader = Leader(sim);
        Assert.Equal(
            DataGridMutationStatus.AlreadyExists,
            Applied(sim, leader, rejection).Result!.GetItem(0).Status
        );
        var deletion = Mutation(sim, session, 3, [], DataGridMutationKind.CompareRemove, first);
        Assert.Equal(
            DataGridMutationStatus.Removed,
            Applied(sim, leader, deletion).Result!.GetItem(0).Status
        );
        sim.Restart(previous);
        sim.Stop(leader);
        leader = Leader(sim);
        Assert.Equal(
            DataGridMutationStatus.Removed,
            Applied(sim, leader, deletion).Result!.GetItem(0).Status
        );
        Assert.Null(Done(sim, sim.Read(leader, Map, Key)).Entry);
    }

    [Fact]
    public void MalformedCommitCannotTruncateAcknowledgedPrefix()
    {
        var config = Configuration();
        var node = new RaftNode(config, 1, 1, new(config, 3, 0, [Entry(1, 1), Entry(2, 2)], 2));
        var request = new RaftMessage(
            RaftMessageKind.AppendRequest,
            0,
            1,
            3,
            new(1, 1),
            Cluster,
            config.ComputeDigest(),
            index: 1,
            previousTerm: 1,
            entries: [Entry(2, 3)]
        );
        var effects = node.Receive(request, default);
        Assert.Single(effects.OfType<RaftFault>());
        Assert.Empty(effects.OfType<RaftWrite>());
        Assert.Empty(effects.OfType<RaftSend>());
        Assert.Equal(2, node.Durable.EntryAt(2).Term);
        Assert.Equal(2, node.Durable.CommitIndex);
    }

    [Fact]
    public void CompactedFollowerHintCanMoveReplicationProbeForward()
    {
        var config = Configuration();
        var node = new RaftNode(
            config,
            0,
            1,
            new(
                config,
                1,
                entries: Enumerable.Range(1, 128).Select(i => Entry(i, 1)),
                commitIndex: 128
            )
        );
        var timer = Assert.Single(node.Start().OfType<RaftTimer>());
        var election = Assert.Single(node.Timeout(timer.Token, default).OfType<RaftWrite>());
        var vote = node.Persisted(election.Token, true).OfType<RaftSend>().First().Message;
        var leader = Assert.Single(
            node.Receive(
                    Message(
                        RaftMessageKind.VoteResponse,
                        vote.To,
                        0,
                        2,
                        token: vote.Token,
                        accepted: true
                    ),
                    default
                )
                .OfType<RaftWrite>()
        );
        var probe = node.Persisted(leader.Token, true).OfType<RaftSend>().First().Message;
        var oldProbe = Assert
            .Single(
                node.Receive(
                        Message(
                            RaftMessageKind.AppendResponse,
                            probe.To,
                            0,
                            2,
                            token: probe.Token,
                            conflict: 1
                        ),
                        default
                    )
                    .OfType<RaftSend>()
            )
            .Message;
        Assert.Equal(0, oldProbe.Index);
        var forward = Assert
            .Single(
                node.Receive(
                        Message(
                            RaftMessageKind.AppendResponse,
                            probe.To,
                            0,
                            2,
                            token: oldProbe.Token,
                            conflict: 129
                        ),
                        default
                    )
                    .OfType<RaftSend>()
            )
            .Message;
        Assert.Equal(128, forward.Index);
        Assert.Equal(129, Assert.Single(forward.Entries).Index);
    }

    private static int Leader(DataGridRaftSimulation sim, Func<int, bool>? include = null)
    {
        sim.RunUntil(s =>
            Enumerable.Range(0, 3).Any(n => (include?.Invoke(n) ?? true) && s.Status(n).Ready)
        );
        return Enumerable
            .Range(0, 3)
            .First(n => (include?.Invoke(n) ?? true) && sim.Status(n).Ready);
    }

    private static DataGridRaftOutcome Done(DataGridRaftSimulation sim, Guid request)
    {
        sim.RunUntil(s => s.Outcome(request) is not null);
        return sim.Outcome(request)!;
    }

    private static DataGridRaftOutcome Applied(
        DataGridRaftSimulation sim,
        int node,
        DataGridCommittedCommand command
    )
    {
        // A preceding client may finish while an already-started replication checkpoint is flushing.
        sim.RunUntil(s => !s.Status(node).Busy);
        var outcome = Done(sim, sim.Propose(node, command));
        Assert.Equal(DataGridRaftOutcomeKind.Applied, outcome.Kind);
        return outcome;
    }

    private static long Open(DataGridRaftSimulation sim, int node) =>
        Applied(
            sim,
            node,
            DataGridCommittedCommand.OpenSession(
                Principal,
                Nonce,
                new(sim.UtcTime.UtcTicks + 5 * TimeSpan.TicksPerSecond),
                sim.UtcTime
            )
        ).Result!.SessionNumber;

    private static DataGridCommittedCommand Mutation(
        DataGridRaftSimulation sim,
        long session,
        ulong sequence,
        byte[] value,
        DataGridMutationKind kind = DataGridMutationKind.Put,
        DataGridEntryVersion version = default
    ) =>
        DataGridCommittedCommand.Mutate(
            Principal,
            new(
                kind,
                Map,
                session,
                sequence,
                DataGridExpiryKind.Never,
                0,
                [
                    new(
                        Key,
                        value,
                        kind
                            is DataGridMutationKind.CompareExchange
                                or DataGridMutationKind.CompareRemove
                            ? version
                            : default
                    ),
                ]
            ),
            sim.UtcTime
        );

    private static RaftEntry Entry(long index, long term) =>
        new(
            index,
            term,
            DataGridCommittedCommandCodec.Encode(DataGridCommittedCommand.AdvanceTime(default))
        );

    private static RaftMessage Message(
        RaftMessageKind kind,
        int from,
        int to,
        long term,
        RaftToken? token = null,
        long index = 0,
        long previousTerm = 0,
        long commit = 0,
        bool accepted = false,
        long conflict = 0
    ) =>
        new(
            kind,
            from,
            to,
            term,
            token ?? new(1, 1),
            Cluster,
            Configuration().ComputeDigest(),
            index,
            previousTerm,
            commit,
            accepted,
            conflict
        );

    private static DataGridBootstrapConfiguration Configuration() =>
        new(
            Cluster,
            [
                new(new Guid(1, 0, 0, new byte[8]), "node-a.example", 7400),
                new(new Guid(2, 0, 0, new byte[8]), "node-b.example", 7400),
                new(new Guid(3, 0, 0, new byte[8]), "node-c.example", 7400),
            ],
            [
                new(
                    Map,
                    "catalog",
                    DataGridMapOwnership.Derived,
                    "catalog-service",
                    "catalog-loader",
                    1000,
                    1048576,
                    new(1, 1, 2, 1, 1)
                ),
            ]
        );
}
