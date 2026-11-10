using System.Buffers.Binary;
using System.Security.Cryptography;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Testing;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridLocalStateMachineTests
{
    private static readonly Guid Cluster = new("10112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Map = new("00112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Principal = new("20112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Other = new("30112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Nonce = new("40112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid SnapshotId = new("50112233-4455-6677-8899-aabbccddeeff");

    [Fact]
    public void LostConditionalDeleteReplyCannotDeleteAnotherSessionsRecreatedEntry()
    {
        var state = new DataGridLocalStateMachine(Configuration());
        Assert.Equal(1, Apply(state, Open(Principal, Nonce)).SessionNumber);
        Assert.Equal(2, Apply(state, Open(Other, SnapshotId)).SessionNumber);
        var original = Apply(state, Mutation(1, 1, DataGridMutationKind.Put, "catalog:eu", [1]))
            .GetItem(0)
            .Version;
        var deletion = Mutation(
            1,
            2,
            DataGridMutationKind.CompareRemove,
            "catalog:eu",
            [],
            original
        );
        Assert.Equal(DataGridMutationStatus.Removed, Apply(state, deletion).GetItem(0).Status);
        var replacement = Apply(
                state,
                Mutation(2, 1, DataGridMutationKind.Put, "catalog:eu", [2], principal: Other)
            )
            .GetItem(0)
            .Version;
        Assert.Equal(DataGridMutationStatus.Removed, Apply(state, deletion).GetItem(0).Status);
        Assert.Equal(replacement, state.Read(Map, Key("catalog:eu"))!.Version);
        Assert.NotEqual(original, replacement);
        Assert.Equal(
            DataGridMutationStatus.VersionMismatch,
            Apply(
                    state,
                    Mutation(1, 3, DataGridMutationKind.CompareRemove, "catalog:eu", [], original)
                )
                .GetItem(0)
                .Status
        );
        Assert.Equal(new byte[] { 2 }, state.Read(Map, Key("catalog:eu"))!.GetValue());
    }

    [Fact]
    public void ConditionalRejectionRemainsStableAcrossOtherWritesAndSnapshot()
    {
        var config = Configuration();
        var state = new DataGridLocalStateMachine(config);
        Apply(state, Open(Principal, Nonce));
        Apply(state, Open(Other, SnapshotId));
        Apply(state, Mutation(2, 1, DataGridMutationKind.Put, "catalog:eu", [1], principal: Other));
        var rejected = Mutation(1, 1, DataGridMutationKind.PutIfAbsent, "catalog:eu", [2]);
        Assert.Equal(
            DataGridMutationStatus.AlreadyExists,
            Apply(state, rejected).GetItem(0).Status
        );
        Apply(
            state,
            Mutation(2, 2, DataGridMutationKind.Remove, "catalog:eu", [], principal: Other)
        );
        var snapshot = state.SaveSnapshot(SnapshotId, 1);
        state = DataGridLocalStateMachine.Restore(
            config,
            snapshot.Descriptor,
            snapshot.GetContent()
        );
        Assert.Equal(
            DataGridMutationStatus.AlreadyExists,
            Apply(state, rejected).GetItem(0).Status
        );
        Assert.Null(state.Read(Map, Key("catalog:eu")));
        Assert.Equal(
            DataGridApplyStatus.RequestIdentityMismatch,
            Apply(state, Mutation(1, 1, DataGridMutationKind.PutIfAbsent, "catalog:eu", [3])).Status
        );
        Assert.Equal(
            DataGridMutationStatus.Stored,
            Apply(state, Mutation(1, 2, DataGridMutationKind.PutIfAbsent, "catalog:eu", [3]))
                .GetItem(0)
                .Status
        );
        Assert.Equal(DataGridApplyStatus.RequestRetired, Apply(state, rejected).Status);
    }

    [Fact]
    public void ExpiredStateStaysChargedUntilBoundedReclamationAndRejectedResultIsRetained()
    {
        var state = new DataGridLocalStateMachine(Configuration(keys: 1, bytes: 3));
        Apply(state, Open(Principal, Nonce));
        Apply(state, Mutation(1, 1, DataGridMutationKind.Put, "k1", [1], ttl: 1));
        Apply(state, DataGridCommittedCommand.AdvanceTime(new(1)));
        Assert.Null(state.Read(Map, Key("k1")));
        Assert.Equal(1, state.StoredKeyCount);
        var rejected = Mutation(1, 2, DataGridMutationKind.Put, "k2", [2], time: 1);
        Assert.Equal(
            DataGridMutationStatus.CapacityExceeded,
            Apply(state, rejected).GetItem(0).Status
        );
        Apply(state, DataGridCommittedCommand.Reclaim(Map, new(1)));
        Assert.Equal(0, state.StoredDataByteCount);
        Assert.Equal(
            DataGridMutationStatus.CapacityExceeded,
            Apply(state, rejected).GetItem(0).Status
        );
        Assert.Equal(
            DataGridMutationStatus.Stored,
            Apply(state, Mutation(1, 3, DataGridMutationKind.PutIfAbsent, "k2", [3], time: 1))
                .GetItem(0)
                .Status
        );
        Assert.Equal(3, state.StoredDataByteCount);
    }

    [Fact]
    public void BatchesReportPartialCapacityAndReplacementUsesOnlyItsByteDelta()
    {
        var state = new DataGridLocalStateMachine(Configuration(keys: 2, bytes: 6));
        Apply(state, Open(Principal, Nonce));
        var batch = new DataGridMutationCommand(
            DataGridMutationKind.Put,
            Map,
            1,
            1,
            DataGridExpiryKind.Never,
            0,
            [new(Key("k1"), [1]), new(Key("k2"), [2]), new(Key("k3"), [3])]
        );
        var result = Apply(state, DataGridCommittedCommand.Mutate(Principal, batch, default));
        Assert.Equal(DataGridMutationStatus.Stored, result.GetItem(0).Status);
        Assert.Equal(1, result.GetItem(1).Version.ItemIndex);
        Assert.Equal(DataGridMutationStatus.CapacityExceeded, result.GetItem(2).Status);
        var replacement = Apply(state, Mutation(1, 2, DataGridMutationKind.Put, "k1", []));
        Assert.Equal(DataGridMutationStatus.Stored, replacement.GetItem(0).Status);
        Assert.Equal(5, state.StoredDataByteCount);
        Assert.Equal(2, state.StoredKeyCount);
        Assert.Empty(state.Read(Map, Key("k1"))!.GetValue());
    }

    [Fact]
    public void ReadsAndSnapshotPreserveTicksOwnershipAndClockHighWater()
    {
        var config = Configuration();
        var state = new DataGridLocalStateMachine(config);
        Apply(state, Open(Principal, Nonce));
        byte[] input = [1, 2];
        var write = Mutation(
            1,
            1,
            DataGridMutationKind.Put,
            "catalog:eu",
            input,
            ttl: 10,
            time: 100
        );
        input[0] = 99;
        Apply(state, write);
        var read = state.Read(Map, Key("catalog:eu"))!;
        read.GetValue()[0] = 99;
        Assert.Equal(new byte[] { 1, 2 }, state.Read(Map, Key("catalog:eu"))!.GetValue());
        Apply(state, DataGridCommittedCommand.AdvanceTime(new(105)));
        var snapshot = state.SaveSnapshot(SnapshotId, 1);
        var bytes = snapshot.GetContent();
        var restored = DataGridLocalStateMachine.Restore(config, snapshot.Descriptor, bytes);
        bytes[0] = 0;
        Assert.Equal(
            TimeSpan.FromTicks(5),
            restored.Read(Map, Key("catalog:eu"))!.RemainingTimeToLive
        );
        Apply(restored, DataGridCommittedCommand.AdvanceTime(new(90)));
        Assert.Equal(105, restored.AppliedTime.UtcTicks);
        Apply(restored, DataGridCommittedCommand.AdvanceTime(new(110)));
        Assert.Null(restored.Read(Map, Key("catalog:eu")));
        var replay = Apply(restored, write);
        Assert.Equal(read.Version, replay.GetItem(0).Version);
        Assert.Null(restored.Read(Map, Key("catalog:eu")));
        Assert.Single(restored.ReadMany(Map, [Key("catalog:eu")]));
    }

    [Fact]
    public void InvalidPolicyAndOverflowAreRetainedRejectionsAndDoNotChangeValues()
    {
        var state = new DataGridLocalStateMachine(Configuration(neverOnly: true));
        Apply(state, Open(Principal, Nonce));
        var invalid = Mutation(1, 1, DataGridMutationKind.Put, "catalog:eu", [1], ttl: 1);
        Assert.Equal(
            DataGridMutationStatus.InvalidRequest,
            Apply(state, invalid).GetItem(0).Status
        );
        Assert.Equal(
            DataGridMutationStatus.InvalidRequest,
            Apply(state, invalid).GetItem(0).Status
        );
        Assert.Null(state.Read(Map, Key("catalog:eu")));
        var max = DateTimeOffset.MaxValue.UtcTicks;
        var nearEnd = new DataGridLocalStateMachine(Configuration());
        var openTime = max - TimeSpan.FromMinutes(11).Ticks;
        Apply(
            nearEnd,
            DataGridCommittedCommand.OpenSession(Principal, Nonce, new(openTime + 1), new(openTime))
        );
        var overflow = Mutation(
            1,
            1,
            DataGridMutationKind.Put,
            "catalog:eu",
            [1],
            ttl: DateTimeOffset.MaxValue.UtcTicks,
            time: openTime
        );
        Assert.Equal(
            DataGridMutationStatus.InvalidRequest,
            Apply(nearEnd, overflow).GetItem(0).Status
        );
    }

    [Fact]
    public void SessionCapacityNonceTombstoneRetirementAndPrincipalOwnershipAreEnforced()
    {
        var state = new DataGridLocalStateMachine(Configuration());
        var opened = Apply(state, Open(Principal, Nonce));
        Assert.Equal(opened.SessionNumber, Apply(state, Open(Principal, Nonce)).SessionNumber);
        Assert.Equal(
            DataGridApplyStatus.RequestIdentityMismatch,
            Apply(
                state,
                DataGridCommittedCommand.OpenSession(Principal, Nonce, new(2), default)
            ).Status
        );
        Assert.Equal(
            DataGridApplyStatus.SessionExpired,
            Apply(
                state,
                Mutation(1, 1, DataGridMutationKind.Put, "k", [1], principal: Other)
            ).Status
        );
        Apply(state, DataGridCommittedCommand.CloseSession(Principal, 1, 1, default));
        Assert.Equal(
            DataGridApplyStatus.SessionExpired,
            Apply(state, Open(Principal, Nonce)).Status
        );
        for (var i = 1; i < 64; i++)
            Apply(state, Open(Principal, NumberId(i)));
        Assert.Equal(
            DataGridApplyStatus.CapacityExceeded,
            Apply(state, Open(Principal, NumberId(1000))).Status
        );
        Assert.Equal(64, state.SessionHighWater);
        Apply(state, DataGridCommittedCommand.AdvanceTime(new(TimeSpan.TicksPerSecond * 6)));
        Assert.Equal(63, state.SessionCount);
        var future = TimeSpan.TicksPerSecond * 6;
        var next = Apply(
            state,
            DataGridCommittedCommand.OpenSession(
                Principal,
                NumberId(1001),
                new(future + 1),
                new(future)
            )
        );
        Assert.Equal(65, next.SessionNumber);
        Assert.Equal(
            DataGridApplyStatus.SessionExpired,
            Apply(state, Open(Principal, Nonce)).Status
        );
        Apply(state, DataGridCommittedCommand.AdvanceTime(new(TimeSpan.FromMinutes(10).Ticks + 1)));
        Assert.Equal(
            DataGridApplyStatus.SessionExpired,
            Apply(state, Mutation(2, 1, DataGridMutationKind.Put, "k", [1])).Status
        );
    }

    [Fact]
    public void ReclamationIsBoundedAndItsCursorSurvivesRestore()
    {
        var config = Configuration(keys: 300);
        var state = new DataGridLocalStateMachine(config);
        Apply(state, Open(Principal, Nonce));
        for (var n = 0; n < 3; n++)
        {
            var entries = Enumerable
                .Range(n * 100, 100)
                .Select(i => new DataGridMutationItem(Key($"catalog:{i:000}"), [1]))
                .ToArray();
            Apply(
                state,
                DataGridCommittedCommand.Mutate(
                    Principal,
                    new(
                        DataGridMutationKind.Put,
                        Map,
                        1,
                        (ulong)n + 1,
                        DataGridExpiryKind.Relative,
                        1,
                        entries
                    ),
                    default
                )
            );
        }
        Apply(state, DataGridCommittedCommand.Reclaim(Map, new(1)));
        Assert.Equal(172, state.StoredKeyCount);
        var snapshot = state.SaveSnapshot(SnapshotId, 1);
        state = DataGridLocalStateMachine.Restore(
            config,
            snapshot.Descriptor,
            snapshot.GetContent()
        );
        Apply(
            state,
            Mutation(1, 4, DataGridMutationKind.Put, "catalog:000", [2], ttl: 100, time: 1)
        );
        Apply(state, DataGridCommittedCommand.Reclaim(Map, new(1)));
        Assert.Equal(45, state.StoredKeyCount);
        Apply(state, DataGridCommittedCommand.Reclaim(Map, new(1)));
        Assert.Equal(1, state.StoredKeyCount);
        Assert.Equal(new byte[] { 2 }, state.Read(Map, Key("catalog:000"))!.GetValue());
    }

    [Fact]
    public void SnapshotRejectsCorruptionAndSemanticallyInvalidRehashedImagesWithoutChangingOwner()
    {
        var config = Configuration();
        var state = new DataGridLocalStateMachine(config);
        Apply(state, Open(Principal, Nonce));
        Apply(state, Mutation(1, 1, DataGridMutationKind.Put, "k1", [1]));
        Apply(state, Mutation(1, 2, DataGridMutationKind.Put, "k2", [2]));
        var snapshot = state.SaveSnapshot(SnapshotId, 1);
        var bytes = snapshot.GetContent();
        bytes[^1] ^= 1;
        Assert.Throws<InvalidDataException>(() =>
            DataGridLocalStateMachine.Restore(config, snapshot.Descriptor, bytes)
        );
        foreach (var offset in new[] { 0, 4, 7, 24, 56, 92 })
        {
            var changed = snapshot.GetContent();
            changed[offset] = 0xff;
            Assert.Throws<InvalidDataException>(() =>
                RestoreRehashed(config, snapshot.Descriptor, changed)
            );
        }
        var entries = FindSection(snapshot.GetContent(), 3);
        var invalidBoolean = snapshot.GetContent();
        var keyLength = BinaryPrimitives.ReadUInt16BigEndian(invalidBoolean.AsSpan(entries + 16));
        invalidBoolean[entries + 18 + keyLength + 28] = 2;
        Assert.Throws<InvalidDataException>(() =>
            RestoreRehashed(config, snapshot.Descriptor, invalidBoolean)
        );
        var impossibleQuota = Configuration(keys: 1);
        var quotaImage = snapshot.GetContent();
        impossibleQuota.ComputeDigest().CopyTo(quotaImage, 24);
        Assert.Throws<InvalidDataException>(() =>
            RestoreRehashed(impossibleQuota, snapshot.Descriptor, quotaImage)
        );
        Assert.Equal(2, state.StoredKeyCount);
        Assert.Equal(new byte[] { 1 }, state.Read(Map, Key("k1"))!.GetValue());
    }

    [Fact]
    public void WrapperCodecRejectsMalformedControlAndMutationPayloads()
    {
        DataGridCommittedCommand[] commands =
        [
            DataGridCommittedCommand.AdvanceTime(new(5)),
            Open(Principal, Nonce),
            DataGridCommittedCommand.CloseSession(Principal, 1, 1, default),
            DataGridCommittedCommand.Reclaim(Map, default),
            Mutation(1, 1, DataGridMutationKind.CompareRemove, "k", [], new(Cluster, 1)),
        ];
        foreach (var command in commands)
        {
            var bytes = DataGridCommittedCommandCodec.Encode(command);
            Assert.Equal(
                bytes,
                DataGridCommittedCommandCodec.Encode(DataGridCommittedCommandCodec.ReadFrom(bytes))
            );
            for (var length = 0; length < bytes.Length; length++)
            {
                var prefix = bytes[..length];
                Assert.Throws<InvalidDataException>(() =>
                    DataGridCommittedCommandCodec.ReadFrom(prefix)
                );
            }
            Assert.Throws<InvalidDataException>(() =>
                DataGridCommittedCommandCodec.ReadFrom([.. bytes, 0])
            );
        }
        var control = DataGridCommittedCommandCodec.Encode(commands[0]);
        control[16] = 1;
        Assert.Throws<InvalidDataException>(() => DataGridCommittedCommandCodec.ReadFrom(control));
        var state = new DataGridLocalStateMachine(Configuration());
        Assert.Throws<InvalidOperationException>(() => state.Apply(2, commands[0]));
        Assert.Equal(0, state.LastAppliedIndex);
        Assert.Equal(0, state.AppliedTime.UtcTicks);
    }

    [Theory]
    [InlineData(DataGridJournalFault.BeforeAppend)]
    [InlineData(DataGridJournalFault.AfterAppend)]
    public void UnknownJournalCompletionStopsOwnerAndRestartUsesOnlyModeledPersistence(
        DataGridJournalFault fault
    )
    {
        var journal = new InMemoryDataGridJournal();
        var simulation = new DataGridSimulation(Configuration(), 7, journal);
        simulation.Schedule(Open(Principal, Nonce));
        simulation.Step();
        simulation.Schedule(Mutation(1, 1, DataGridMutationKind.Put, "catalog:eu", [1]));
        journal.NextFault = fault;
        Assert.Throws<IOException>(() => simulation.Step());
        Assert.False(simulation.IsRunning);
        simulation.Restart();
        Assert.Equal(
            fault == DataGridJournalFault.AfterAppend,
            simulation.State.Read(Map, Key("catalog:eu")) is not null
        );
        simulation.Schedule(Mutation(1, 1, DataGridMutationKind.Put, "catalog:eu", [1]));
        Assert.Equal(DataGridMutationStatus.Stored, simulation.Step()!.GetItem(0).Status);
    }

    [Fact]
    public void FakeNetworkDropDuplicationDelayAndPersistedUnappliedRestartAreReplayable()
    {
        var network = new DataGridSimulatedTransport();
        var simulation = new DataGridSimulation(Configuration(), 17, transport: network);
        simulation.Schedule(Open(Principal, Nonce));
        simulation.Step();
        simulation.Schedule(Mutation(1, 1, DataGridMutationKind.Put, "dropped", [1]));
        simulation.DropNext();
        Assert.Equal(1, simulation.State.LastAppliedIndex);
        simulation.Schedule(Mutation(1, 1, DataGridMutationKind.Put, "catalog:eu", [2]));
        simulation.DuplicateNext();
        simulation.DelayNext(10);
        Assert.Null(simulation.Step(crashAfterPersistence: true));
        simulation.Restart();
        var version = simulation.State.Read(Map, Key("catalog:eu"))!.Version;
        Assert.Equal(version, simulation.Step()!.GetItem(0).Version);
        var snapshot = simulation.Checkpoint();
        simulation.Stop();
        simulation.Restart();
        Assert.Equal(snapshot.Descriptor.LastIncludedIndex, simulation.State.LastAppliedIndex);
        Assert.Equal(version, simulation.State.Read(Map, Key("catalog:eu"))!.Version);
    }

    [Theory]
    [InlineData(1u)]
    [InlineData(7u)]
    [InlineData(123456u)]
    public void SequentialModelChecksSeededFaultAndRecoveryHistories(uint seed) =>
        RunModel(seed, injectInvalidObservation: false);

    [Fact]
    public void CheckerDetectsDeliberatelyInvalidObservationWithReproducibleFailingSeed()
    {
        var first = Assert.Throws<InvalidDataException>(() =>
            RunModel(7, injectInvalidObservation: true)
        );
        var second = Assert.Throws<InvalidDataException>(() =>
            RunModel(7, injectInvalidObservation: true)
        );
        Assert.Equal(first.Message, second.Message);
        Assert.Contains("seed=7", first.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void SmallMapReclamationWrapsWithinTheSameInspectionBudget()
    {
        var state = new DataGridLocalStateMachine(Configuration());
        Apply(state, Open(Principal, Nonce));
        Apply(state, Mutation(1, 1, DataGridMutationKind.Put, "k4", [1]));
        Apply(state, DataGridCommittedCommand.Reclaim(Map, default));
        Apply(state, Mutation(1, 2, DataGridMutationKind.Put, "k1", [2], ttl: 1));
        Apply(state, Mutation(1, 3, DataGridMutationKind.Put, "k7", [3]));
        Apply(state, DataGridCommittedCommand.Reclaim(Map, new(1)));
        Assert.Equal(2, state.StoredKeyCount);
        Assert.Null(state.Read(Map, Key("k1")));
    }

    [Fact]
    public void EmptySnapshotAndTimeWrapperMatchIndependentBinaryFixtures()
    {
        var state = new DataGridLocalStateMachine(Configuration());
        var command = DataGridCommittedCommand.AdvanceTime(new(5));
        Apply(state, command);
        Assert.Equal(
            "484C47520001000100000000000000050000000000000000000000000000000000000000",
            Convert.ToHexString(DataGridCommittedCommandCodec.Encode(command))
        );
        var image = state.SaveSnapshot(SnapshotId, 1);
        // Independent Python struct/uuid/hashlib encoding of HLGF, HLGB and HLGS.
        Assert.Equal(181, image.Descriptor.ByteCount);
        Assert.Equal(
            "8EB50F45DDDCF1A4C478771B98053BD24738D791800901DEDA169024E78BF089",
            Convert.ToHexString(image.Descriptor.GetDigest())
        );
    }

    [Fact]
    public void SnapshotRejectsMixedPositionsInOneRetainedBatchAndInvalidSessionFlags()
    {
        var config = Configuration();
        var state = new DataGridLocalStateMachine(config);
        Apply(state, Open(Principal, Nonce));
        Apply(
            state,
            DataGridCommittedCommand.Mutate(
                Principal,
                new(
                    DataGridMutationKind.Put,
                    Map,
                    1,
                    1,
                    DataGridExpiryKind.Never,
                    0,
                    [new(Key("k1"), [1]), new(Key("k2"), [2])]
                ),
                default
            )
        );
        var snapshot = state.SaveSnapshot(SnapshotId, 1);
        var start = FindSection(snapshot.GetContent(), 4);
        var mixed = snapshot.GetContent();
        BinaryPrimitives.WriteInt64BigEndian(mixed.AsSpan(start + 150), 1);
        Assert.Throws<InvalidDataException>(() =>
            RestoreRehashed(config, snapshot.Descriptor, mixed)
        );
        var badFlag = snapshot.GetContent();
        badFlag[start + 65] = 2;
        Assert.Throws<InvalidDataException>(() =>
            RestoreRehashed(config, snapshot.Descriptor, badFlag)
        );
        var impossibleSequence = snapshot.GetContent();
        BinaryPrimitives.WriteUInt64BigEndian(
            impossibleSequence.AsSpan(start + 57),
            ulong.MaxValue
        );
        Assert.Throws<InvalidDataException>(() =>
            RestoreRehashed(config, snapshot.Descriptor, impossibleSequence)
        );
        var impossibleAllocator = snapshot.GetContent();
        BinaryPrimitives.WriteInt64BigEndian(
            impossibleAllocator.AsSpan(FindSection(impossibleAllocator, 1)),
            long.MaxValue
        );
        Assert.Throws<InvalidDataException>(() =>
            RestoreRehashed(config, snapshot.Descriptor, impossibleAllocator)
        );
    }

    [Fact]
    public void GlobalSessionAdmissionAndBoundedRetirementPreserveTheAllocator()
    {
        var state = new DataGridLocalStateMachine(Configuration());
        for (var principal = 0; principal < 64; principal++)
        for (var i = 0; i < 64; i++)
            Apply(state, Open(NumberId(10000 + principal), NumberId(20000 + principal * 64 + i)));
        Assert.Equal(4096, state.SessionCount);
        Assert.Equal(DataGridApplyStatus.CapacityExceeded, Apply(state, Open(Other, Nonce)).Status);
        Apply(state, DataGridCommittedCommand.AdvanceTime(new(TimeSpan.FromMinutes(10).Ticks)));
        Assert.Equal(3968, state.SessionCount);
        Assert.Equal(4096, state.SessionHighWater);
        var now = state.AppliedTime;
        var opened = Apply(
            state,
            DataGridCommittedCommand.OpenSession(Other, Nonce, new(now.UtcTicks + 1), now)
        );
        Assert.Equal(4097, opened.SessionNumber);
    }

    [Fact]
    public void LocalReadVectorRejectsDuplicatesAndOversizedResponsesWithoutMutatingState()
    {
        var state = new DataGridLocalStateMachine(Configuration(bytes: 2 * 1024 * 1024));
        Apply(state, Open(Principal, Nonce));
        var keys = Enumerable.Range(0, 16).Select(i => $"k{i:00}").ToArray();
        for (var i = 0; i < keys.Length; i++)
            Apply(
                state,
                Mutation(1, (ulong)i + 1, DataGridMutationKind.Put, keys[i], new byte[65536])
            );
        Assert.Throws<InvalidOperationException>(() =>
            state.ReadMany(Map, keys.Select(Key).ToArray())
        );
        Assert.Throws<ArgumentException>(() => state.ReadMany(Map, [Key(keys[0]), Key(keys[0])]));
        Assert.Equal(16, state.StoredKeyCount);
        var captured = state.ReadMany(Map, [Key(keys[0]), Key("missing")]);
        Assert.NotNull(captured[0]);
        Assert.Null(captured[1]);
    }

    [Fact]
    public void SimulationTransportHasFiniteAdmissionAndOverflowDoesNotLosePackets()
    {
        var network = new DataGridSimulatedTransport();
        network.Enqueue([1], long.MaxValue);
        Assert.Equal(new byte[] { 1 }, network.Receive());
        network.Enqueue([2], 0);
        Assert.Throws<OverflowException>(() => network.DelayFirst(1));
        Assert.Equal(1, network.PendingCount);
        Assert.Equal(new byte[] { 2 }, network.Receive());
        for (var i = 0; i < 4096; i++)
            network.Enqueue([], 0);
        Assert.Throws<InvalidOperationException>(() => network.Enqueue([], 0));
        network.DropFirst();
        network.Enqueue([], 0);
        Assert.Equal(4096, network.PendingCount);
    }

    [Fact]
    public void MapIsolationAndPerMapQuotaSurviveOneGroupSnapshot()
    {
        var baseline = Configuration(keys: 1, bytes: 10);
        var config = new DataGridBootstrapConfiguration(
            Cluster,
            [baseline.GetMember(0), baseline.GetMember(1), baseline.GetMember(2)],
            [
                baseline.GetMap(0),
                new(
                    Other,
                    "inventory",
                    DataGridMapOwnership.Authoritative,
                    "inventory-service",
                    "",
                    1,
                    10,
                    new(1, 1, 2, 1, 1)
                ),
            ]
        );
        var state = new DataGridLocalStateMachine(config);
        Apply(state, Open(Principal, Nonce));
        Apply(state, Mutation(1, 1, DataGridMutationKind.Put, "k", [1]));
        Apply(
            state,
            DataGridCommittedCommand.Mutate(
                Principal,
                new(
                    DataGridMutationKind.Put,
                    Other,
                    1,
                    2,
                    DataGridExpiryKind.Never,
                    0,
                    [new(Key("k"), [2])]
                ),
                default
            )
        );
        Assert.Equal(
            DataGridMutationStatus.CapacityExceeded,
            Apply(state, Mutation(1, 3, DataGridMutationKind.Put, "k", new byte[10]))
                .GetItem(0)
                .Status
        );
        var image = state.SaveSnapshot(SnapshotId, 1);
        state = DataGridLocalStateMachine.Restore(config, image.Descriptor, image.GetContent());
        Assert.Equal(2, state.StoredKeyCount);
        Assert.Equal(4, state.StoredDataByteCount);
        Assert.Equal(new byte[] { 1 }, state.Read(Map, Key("k"))!.GetValue());
        Assert.Equal(new byte[] { 2 }, state.Read(Other, Key("k"))!.GetValue());
    }

    [Fact]
    public void ReorderedDistinctRequestsCannotBypassTheSequentialSessionContract()
    {
        var simulation = new DataGridSimulation(Configuration(), 19);
        simulation.Schedule(Open(Principal, Nonce));
        simulation.Step();
        simulation.Schedule(Mutation(1, 1, DataGridMutationKind.Put, "k1", [1]));
        var second = Mutation(1, 2, DataGridMutationKind.Put, "k2", [2]);
        simulation.Schedule(second);
        simulation.DelayNext(10);
        Assert.Equal(DataGridApplyStatus.InvalidRequest, simulation.Step()!.Status);
        Assert.Null(simulation.State.Read(Map, Key("k2")));
        Assert.Equal(DataGridMutationStatus.Stored, simulation.Step()!.GetItem(0).Status);
        simulation.Schedule(second);
        Assert.Equal(DataGridMutationStatus.Stored, simulation.Step()!.GetItem(0).Status);
        Assert.Equal(new byte[] { 1 }, simulation.State.Read(Map, Key("k1"))!.GetValue());
        Assert.Equal(new byte[] { 2 }, simulation.State.Read(Map, Key("k2"))!.GetValue());
    }

    [Fact]
    public void JournalAndTraceCapacityRequireCheckpointWithoutResettingApplicationPositions()
    {
        var simulation = new DataGridSimulation(Configuration(), 23);
        for (var i = 0; i < 4096; i++)
        {
            simulation.Schedule(DataGridCommittedCommand.AdvanceTime(default));
            simulation.Step();
        }
        Assert.True(simulation.TraceTruncated);
        Assert.True(simulation.Trace.Count <= 8192);
        Assert.True(simulation.Trace.Sum(line => line.Length) <= 4 * 1024 * 1024);
        simulation.Schedule(DataGridCommittedCommand.AdvanceTime(default));
        Assert.Throws<IOException>(() => simulation.Step());
        Assert.False(simulation.IsRunning);
        simulation.Restart();
        Assert.Equal(4096, simulation.State.LastAppliedIndex);
        simulation.Checkpoint();
        simulation.Schedule(DataGridCommittedCommand.AdvanceTime(new(1)));
        simulation.Step();
        Assert.Equal(4097, simulation.State.LastAppliedIndex);
    }

    private static void RunModel(uint seed, bool injectInvalidObservation)
    {
        var simulation = new DataGridSimulation(Configuration(keys: 8, bytes: 64), seed);
        var model = new Dictionary<string, ModelEntry>(StringComparer.Ordinal);
        uint random = seed;
        long index = 0,
            time = 0;
        ulong sequence = 0;
        simulation.Schedule(Open(Principal, Nonce));
        simulation.Step();
        index++;
        for (var step = 0; step < 250; step++)
        {
            random = unchecked(random * 1664525u + 1013904223u);
            var key = $"catalog:{random % 8}";
            var kind = (DataGridMutationKind)(1 + ((random >> 8) % 5));
            var proposed = (long)((random >> 16) % 1000);
            time = Math.Max(time, proposed);
            var conditional =
                kind is DataGridMutationKind.CompareExchange or DataGridMutationKind.CompareRemove;
            var removal = kind is DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove;
            var version = conditional
                ? model.TryGetValue(key, out var previous) && (random & 1) != 0
                    ? previous.Version
                    : new DataGridEntryVersion(Cluster, 1)
                : default;
            byte[] value = removal ? [] : [(byte)random, (byte)step];
            var ttl = !removal && (random & 2) != 0 ? 5L : 0;
            var command = Mutation(1, ++sequence, kind, key, value, version, ttl, proposed);
            simulation.ScheduleRandom(command, 20);
            var crash = step % 17 == 0;
            var lost = step % 11 == 0;
            var result = simulation.Step(crashAfterPersistence: crash, loseReply: lost);
            index++;
            var expected = ModelApply(
                model,
                key,
                kind,
                value,
                version,
                ttl == 0 ? null : time + ttl,
                time,
                index
            );
            if (crash)
                simulation.Restart();
            if (result is not null)
                CheckResult(result, expected, seed, step, simulation);
            if (crash || lost)
            {
                simulation.Schedule(command);
                result = simulation.Step();
                index++;
                CheckResult(result!, expected, seed, step, simulation);
            }
            if (step % 13 == 0)
            {
                simulation.Schedule(DataGridCommittedCommand.Reclaim(Map, new(time)));
                simulation.Step();
                index++;
                foreach (
                    var expired in model
                        .Where(pair => pair.Value.Deadline <= time)
                        .Select(pair => pair.Key)
                        .ToArray()
                )
                    model.Remove(expired);
            }
            if (step % 25 == 0)
            {
                simulation.Checkpoint();
                simulation.Stop();
                simulation.Restart();
            }
            if (
                simulation.State.LastAppliedIndex != index
                || simulation.State.AppliedTime.UtcTicks != time
            )
                Fail(seed, step, simulation, "position/time");
            foreach (var i in Enumerable.Range(0, 8))
            {
                var name = $"catalog:{i}";
                var actual = simulation.State.Read(Map, Key(name));
                var present =
                    model.TryGetValue(name, out var entry)
                    && (entry.Deadline is null || entry.Deadline > time);
                var observedPresent = actual is not null;
                if (injectInvalidObservation && step == 37 && i == 0)
                    observedPresent = !observedPresent;
                if (
                    present != observedPresent
                    || (
                        present
                        && (
                            actual!.Version != entry.Version
                            || !actual.GetValue().AsSpan().SequenceEqual(entry.Value)
                            || actual.RemainingTimeToLive
                                != (
                                    entry.Deadline is { } deadline
                                        ? TimeSpan.FromTicks(deadline - time)
                                        : null
                                )
                        )
                    )
                )
                    Fail(seed, step, simulation, "entry observation");
            }
            if (
                simulation.State.StoredKeyCount != model.Count
                || simulation.State.StoredDataByteCount
                    != model.Sum(pair => Key(pair.Key).ByteCount + pair.Value.Value.Length)
            )
                Fail(seed, step, simulation, "capacity accounting");
        }
    }

    private static DataGridMutationResult ModelApply(
        Dictionary<string, ModelEntry> entries,
        string key,
        DataGridMutationKind kind,
        byte[] value,
        DataGridEntryVersion expected,
        long? deadline,
        long time,
        long index
    )
    {
        var stored = entries.TryGetValue(key, out var old);
        var live = stored && (old.Deadline is null || old.Deadline > time);
        if (kind is DataGridMutationKind.CompareExchange or DataGridMutationKind.CompareRemove)
        {
            if (!live)
                return new(DataGridMutationStatus.Missing);
            if (old.Version != expected)
                return new(DataGridMutationStatus.VersionMismatch);
        }
        if (kind == DataGridMutationKind.PutIfAbsent && live)
            return new(DataGridMutationStatus.AlreadyExists);
        if (kind is DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove)
        {
            if (!live)
                return new(DataGridMutationStatus.Missing);
            entries.Remove(key);
            return new(DataGridMutationStatus.Removed);
        }
        var newBytes =
            entries.Sum(pair => Key(pair.Key).ByteCount + pair.Value.Value.Length)
            + Key(key).ByteCount
            + value.Length
            - (stored ? Key(key).ByteCount + old.Value.Length : 0);
        if (entries.Count + (stored ? 0 : 1) > 8 || newBytes > 64)
            return new(DataGridMutationStatus.CapacityExceeded);
        var version = new DataGridEntryVersion(Cluster, index);
        entries[key] = new(value, version, deadline);
        return new(DataGridMutationStatus.Stored, version);
    }

    private static void CheckResult(
        DataGridApplyResult actual,
        DataGridMutationResult expected,
        uint seed,
        int step,
        DataGridSimulation simulation
    )
    {
        if (
            actual.Status != DataGridApplyStatus.Completed
            || actual.ItemCount != 1
            || actual.GetItem(0) != expected
        )
            Fail(seed, step, simulation, "logical result");
    }

    private static void Fail(uint seed, int step, DataGridSimulation simulation, string reason) =>
        throw new InvalidDataException(
            $"seed={seed} step={step} {reason}\n{string.Join('\n', simulation.Trace)}"
        );

    private readonly record struct ModelEntry(
        byte[] Value,
        DataGridEntryVersion Version,
        long? Deadline
    );

    private static int FindSection(byte[] image, int id)
    {
        var offset = 96;
        for (var i = 0; i < 4; i++)
        {
            var section = BinaryPrimitives.ReadUInt16BigEndian(image.AsSpan(offset));
            var length = BinaryPrimitives.ReadUInt32BigEndian(image.AsSpan(offset + 8));
            if (section == id)
                return offset + 12;
            offset += 12 + (int)length;
        }
        throw new InvalidOperationException("Missing test section.");
    }

    private static DataGridLocalStateMachine RestoreRehashed(
        DataGridBootstrapConfiguration configuration,
        DataGridSnapshotDescriptor descriptor,
        byte[] bytes
    ) =>
        DataGridLocalStateMachine.Restore(
            configuration,
            new(
                descriptor.SnapshotId,
                descriptor.LastIncludedIndex,
                descriptor.LastIncludedTerm,
                bytes.Length,
                SHA256.HashData(bytes)
            ),
            bytes
        );

    private static DataGridApplyResult Apply(
        DataGridLocalStateMachine state,
        DataGridCommittedCommand command
    ) => state.Apply(state.LastAppliedIndex + 1, command);

    private static DataGridKey Key(string key) => DataGridKey.FromUtf8(key);

    private static Guid NumberId(int n) => new(n, 0, 0, new byte[8]);

    private static DataGridCommittedCommand Open(Guid principal, Guid nonce) =>
        DataGridCommittedCommand.OpenSession(
            principal,
            nonce,
            new(TimeSpan.TicksPerSecond * 5),
            default
        );

    private static DataGridCommittedCommand Mutation(
        long session,
        ulong sequence,
        DataGridMutationKind kind,
        string key,
        byte[] value,
        DataGridEntryVersion version = default,
        long ttl = 0,
        long time = 0,
        Guid principal = default
    ) =>
        DataGridCommittedCommand.Mutate(
            principal == Guid.Empty ? Principal : principal,
            new(
                kind,
                Map,
                session,
                sequence,
                ttl == 0 ? DataGridExpiryKind.Never : DataGridExpiryKind.Relative,
                ttl,
                [new(Key(key), value, version)]
            ),
            new(time)
        );

    private static DataGridBootstrapConfiguration Configuration(
        int keys = 1000,
        long bytes = 1048576,
        bool neverOnly = false
    ) =>
        new(
            Cluster,
            [
                new(NumberId(1), "node-a.example", 7400),
                new(NumberId(2), "node-b.example", 7400),
                new(NumberId(3), "node-c.example", 7400),
            ],
            [
                new(
                    Map,
                    "catalog",
                    DataGridMapOwnership.Derived,
                    "catalog-service",
                    "catalog-loader",
                    keys,
                    bytes,
                    new(1, 1, neverOnly ? (byte)1 : (byte)2, 1, 1)
                ),
            ]
        );
}
