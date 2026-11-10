using System.Buffers.Binary;
using System.Diagnostics;
using System.Globalization;
using System.Security.Cryptography;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Storage;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridFileStorageTests
{
    private static readonly Guid Cluster = new("10112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Map = new("00112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Principal = new("20112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Generation = new("30112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Nonce = new("40112233-4455-6677-8899-aabbccddeeff");
    private static readonly DataGridKey Key = DataGridKey.FromUtf8("catalog:eu");

    [Fact]
    public void CurrentAnchorMatchesIndependentBytesAndRejectsEveryCorruptedByte()
    {
        var anchor = new DataGridCurrentAnchor(Cluster, Member(0), Generation);
        var expected = new byte[88];
        Convert
            .FromHexString("484C47430001000010112233445566778899AABBCCDDEEFF")
            .CopyTo(expected, 0);
        Convert.FromHexString("00000001000000000000000000000000").CopyTo(expected, 24);
        Convert.FromHexString("30112233445566778899AABBCCDDEEFF").CopyTo(expected, 40);
        SHA256.HashData(expected.AsSpan(0, 56)).CopyTo(expected, 56);
        Assert.Equal(expected, anchor.Encode());
        Assert.Equal(Generation, DataGridCurrentAnchor.ReadFrom(expected).GenerationId);
        for (var i = 0; i < expected.Length; i++)
        {
            var bytes = (byte[])expected.Clone();
            bytes[i] ^= 1;
            Assert.Throws<InvalidDataException>(() => DataGridCurrentAnchor.ReadFrom(bytes));
        }
        for (var i = 0; i < expected.Length; i++)
            Assert.Throws<InvalidDataException>(() =>
                DataGridCurrentAnchor.ReadFrom(expected.AsSpan(0, i))
            );
        var empty = (byte[])expected.Clone();
        empty.AsSpan(40, 16).Clear();
        SHA256.HashData(empty.AsSpan(0, 56)).CopyTo(empty, 56);
        Assert.Throws<InvalidDataException>(() => DataGridCurrentAnchor.ReadFrom(empty));
    }

    [Fact]
    public void BootstrapIsExplicitExclusiveAndDoesNotReinitializeExistingState()
    {
        using var directory = new OwnedDirectory();
        Assert.Throws<FileNotFoundException>(() =>
            DataGridFileStore.Open(directory.Path, Configuration(), 0)
        );
        using var store = DataGridFileStore.Bootstrap(directory.Path, Configuration(), 0);
        Assert.Equal(0, store.Image.CommitIndex);
        Assert.Throws<IOException>(() =>
            DataGridFileStore.Open(directory.Path, Configuration(), 0)
        );
        store.Dispose();
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Bootstrap(directory.Path, Configuration(), 0)
        );
        using var recovered = DataGridFileStore.Open(directory.Path, Configuration(), 0);
        Assert.Equal(0, recovered.Image.Term);
        Assert.Null(recovered.Image.Vote);
    }

    [Fact]
    public void RecoveryRetainsUncommittedTailWithoutApplyingIt()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var store = DataGridFileStore.Bootstrap(directory.Path, config, 0))
            store.Persist(Image(config, commit: 2), TestContext.Current.CancellationToken);
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(4, recovered.Image.LastIndex);
        Assert.Equal(2, recovered.RestoreApplication().LastAppliedIndex);
        Assert.Equal(new byte[] { 1 }, recovered.RestoreApplication().Read(Map, Key)!.GetValue());
        var application = recovered.RestoreApplication();
        var node = new RaftNode(config, 0, 2, recovered.Image);
        Assert.Equal(application.LastAppliedIndex, node.Machine.LastAppliedIndex);
        Assert.Equal(2, node.Durable.Vote);
    }

    [Fact]
    public void ConflictReplacementPreservesCommittedPrefixAndReplaysOnlyNewCheckpoint()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var store = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            store.Persist(Image(config, commit: 2), TestContext.Current.CancellationToken);
            var replacement = new RaftDurableImage(
                config,
                4,
                1,
                store.Image.Entries.Take(2).Append(Entry(3, 4, Mutation(2, [9]))),
                3
            );
            store.Persist(replacement, TestContext.Current.CancellationToken);
        }
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(4, recovered.Image.Term);
        Assert.Equal(1, recovered.Image.Vote);
        Assert.Equal(3, recovered.Image.LastIndex);
        Assert.Equal(new byte[] { 9 }, recovered.RestoreApplication().Read(Map, Key)!.GetValue());
    }

    [Fact]
    public void SnapshotPublicationPreservesVoteUncommittedSuffixAndRetryResult()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        Guid previous;
        DataGridEntryVersion version;
        using (var store = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            store.Persist(Image(config, commit: 2), TestContext.Current.CancellationToken);
            previous = store.GenerationId;
            var state = store.RestoreApplication();
            version = state.Read(Map, Key)!.Version;
            var image = state.SaveSnapshot(Nonce, 1);
            store.Persist(
                new(config, 3, 2, store.Image.Entries.Skip(2), 2, image),
                TestContext.Current.CancellationToken
            );
            Assert.NotEqual(previous, store.GenerationId);
            Assert.True(File.Exists(directory.GenerationPath(previous)));
        }
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(3, recovered.Image.Term);
        Assert.Equal(2, recovered.Image.Vote);
        Assert.Equal(2, recovered.Image.SnapshotIndex);
        Assert.Equal(4, recovered.Image.LastIndex);
        var stateAfter = recovered.RestoreApplication();
        Assert.Equal(version, stateAfter.Read(Map, Key)!.Version);
        Assert.Equal(version, stateAfter.Apply(3, Mutation(1, [1])).GetItem(0).Version);
    }

    [Theory]
    [InlineData((int)DataGridStorageStep.SnapshotFlushed)]
    [InlineData((int)DataGridStorageStep.SnapshotRenamed)]
    [InlineData((int)DataGridStorageStep.SnapshotDirectoryFlushed)]
    [InlineData((int)DataGridStorageStep.GenerationFlushed)]
    [InlineData((int)DataGridStorageStep.GenerationRenamed)]
    [InlineData((int)DataGridStorageStep.GenerationDirectoryFlushed)]
    [InlineData((int)DataGridStorageStep.AnchorFlushed)]
    [InlineData((int)DataGridStorageStep.AnchorRenamed)]
    [InlineData((int)DataGridStorageStep.AnchorDirectoryFlushed)]
    public void InterruptedPublicationRecoversOnlySelectedCompleteGeneration(int interruptionValue)
    {
        var interruption = (DataGridStorageStep)interruptionValue;
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0))
            initial.Persist(Image(config, 2), TestContext.Current.CancellationToken);
        Guid previous;
        var reached = new List<DataGridStorageStep>();
        using (
            var store = DataGridFileStore.Open(
                directory.Path,
                config,
                0,
                observe: step =>
                {
                    reached.Add(step);
                    if (step == interruption)
                        throw new IOException("Injected interruption.");
                }
            )
        )
        {
            previous = store.GenerationId;
            var snapshot = store.RestoreApplication().SaveSnapshot(Nonce, 1);
            Assert.Throws<IOException>(() =>
                store.Persist(
                    new(config, 3, 2, store.Image.Entries.Skip(2), 2, snapshot),
                    TestContext.Current.CancellationToken
                )
            );
            Assert.True(store.IsFaulted);
            Assert.Throws<InvalidOperationException>(() =>
                store.RollGeneration(TestContext.Current.CancellationToken)
            );
        }
        Assert.Equal(interruption, reached[^1]);
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(
            interruption >= DataGridStorageStep.AnchorRenamed,
            recovered.GenerationId != previous
        );
        Assert.Equal(2, recovered.Image.CommitIndex);
        Assert.Equal(4, recovered.Image.LastIndex);
        Assert.Equal(2, recovered.Image.Vote);
        Assert.Equal(new byte[] { 1 }, recovered.RestoreApplication().Read(Map, Key)!.GetValue());
    }

    [Theory]
    [InlineData((int)DataGridStorageStep.WalWritten)]
    [InlineData((int)DataGridStorageStep.WalFlushed)]
    public void UncertainAppendFaultsOwnerAndDoesNotPermitReuse(int interruptionValue)
    {
        var interruption = (DataGridStorageStep)interruptionValue;
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0)) { }
        using (
            var store = DataGridFileStore.Open(
                directory.Path,
                config,
                0,
                observe: step =>
                {
                    if (step == interruption)
                        throw new IOException("Injected I/O failure.");
                }
            )
        )
        {
            Assert.Throws<IOException>(() =>
                store.Persist(Image(config, 2), TestContext.Current.CancellationToken)
            );
            Assert.True(store.IsFaulted);
            Assert.Throws<InvalidOperationException>(() =>
                store.Persist(Image(config, 2), TestContext.Current.CancellationToken)
            );
        }
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(2, recovered.Image.CommitIndex);
        Assert.Equal(4, recovered.Image.LastIndex);
    }

    [Fact]
    public void CancellationBeforeSubmissionDoesNotWriteAndAfterFlushRequiresRestart()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using var cancellation = new CancellationTokenSource();
        using (var store = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            cancellation.Cancel();
            Assert.Throws<OperationCanceledException>(() =>
                store.Persist(Image(config, 2), cancellation.Token)
            );
            Assert.False(store.IsFaulted);
            Assert.Equal(0, store.Image.LastIndex);
        }
        using var afterWrite = new CancellationTokenSource();
        using (
            var store = DataGridFileStore.Open(
                directory.Path,
                config,
                0,
                observe: step =>
                {
                    if (step == DataGridStorageStep.WalFlushed)
                        afterWrite.Cancel();
                }
            )
        )
        {
            Assert.Throws<OperationCanceledException>(() =>
                store.Persist(Image(config, 2), afterWrite.Token)
            );
            Assert.True(store.IsFaulted);
        }
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(2, recovered.Image.CommitIndex);
    }

    [Fact]
    public void TornAndCorruptUncommittedTailsRefuseStartupWithoutModifyingFiles()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        Guid generation;
        using (var store = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            store.Persist(Image(config, 2), TestContext.Current.CancellationToken);
            generation = store.GenerationId;
        }
        var path = directory.GenerationPath(generation);
        var original = File.ReadAllBytes(path);
        var final = RecordOffsets(original)[^1];
        // Last record is the checkpoint. Append an uncommitted entry and then tear/corrupt it.
        var lastNumber = BinaryPrimitives.ReadInt64BigEndian(original.AsSpan(final + 12));
        var appended = new DataGridWalRecord(
            DataGridWalRecordKind.LogAppend,
            lastNumber + 1,
            DataGridWalRecovery.Append(Entry(5, 3, DataGridCommittedCommand.AdvanceTime(default)))
        ).Encode();
        for (var n = 1; n < appended.Length; n++)
        {
            var bytes = original.Concat(appended.AsSpan(0, n).ToArray()).ToArray();
            File.WriteAllBytes(path, bytes);
            Assert.Throws<InvalidDataException>(() =>
                DataGridFileStore.Open(directory.Path, config, 0)
            );
            Assert.Equal(bytes, File.ReadAllBytes(path));
        }
        appended[^1] ^= 1;
        File.WriteAllBytes(path, original.Concat(appended).ToArray());
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Open(directory.Path, config, 0)
        );
    }

    [Fact]
    public void CompleteRecordPrefixesOfInterruptedBatchesKeepAcknowledgedState()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        Guid generation;
        long originalLength;
        using (var store = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            store.Persist(Image(config, 2), TestContext.Current.CancellationToken);
            generation = store.GenerationId;
            originalLength = new FileInfo(directory.GenerationPath(generation)).Length;
            var replacement = new RaftDurableImage(
                config,
                4,
                1,
                store.Image.Entries.Take(2).Append(Entry(3, 4, Mutation(2, [9]))),
                3
            );
            store.Persist(replacement, TestContext.Current.CancellationToken);
        }
        var path = directory.GenerationPath(generation);
        var complete = File.ReadAllBytes(path);
        var boundaries = RecordOffsets(complete)
            .Where(offset => offset >= originalLength)
            .Append(complete.Length)
            .ToArray();
        foreach (var boundary in boundaries)
        {
            File.WriteAllBytes(path, complete.AsSpan(0, boundary).ToArray());
            using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
            Assert.Equal(boundary == complete.Length ? 3 : 2, recovered.Image.CommitIndex);
            Assert.Equal(
                new byte[] { boundary == complete.Length ? (byte)9 : (byte)1 },
                recovered.RestoreApplication().Read(Map, Key)!.GetValue()
            );
            Assert.Equal(boundary, new FileInfo(path).Length);
        }
    }

    [Fact]
    public void SelectedCorruptGenerationNeverFallsBackToOldOrUnreferencedFiles()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        Guid selected;
        using (var store = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            store.Persist(Image(config, 2), TestContext.Current.CancellationToken);
            store.RollGeneration(TestContext.Current.CancellationToken);
            selected = store.GenerationId;
        }
        var bytes = File.ReadAllBytes(directory.GenerationPath(selected));
        bytes[^1] ^= 1;
        File.WriteAllBytes(directory.GenerationPath(selected), bytes);
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Open(directory.Path, config, 0)
        );
        File.Delete(directory.GenerationPath(selected));
        Assert.Throws<FileNotFoundException>(() =>
            DataGridFileStore.Open(directory.Path, config, 0)
        );
    }

    [Fact]
    public void MissingOrCorruptSnapshotAndForeignMemberRefuseStartup()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var store = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            store.Persist(Image(config, 2), TestContext.Current.CancellationToken);
            var snapshot = store.RestoreApplication().SaveSnapshot(Nonce, 1);
            store.Persist(
                new(config, 3, 2, store.Image.Entries.Skip(2), 2, snapshot),
                TestContext.Current.CancellationToken
            );
        }
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Open(directory.Path, config, 1)
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Open(directory.Path, Configuration(999), 0)
        );
        var path = System.IO.Path.Combine(directory.Path, $"snapshot-{Nonce:N}.bin");
        var bytes = File.ReadAllBytes(path);
        bytes[^1] ^= 1;
        File.WriteAllBytes(path, bytes);
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Open(directory.Path, config, 0)
        );
        File.Delete(path);
        Assert.Throws<FileNotFoundException>(() =>
            DataGridFileStore.Open(directory.Path, config, 0)
        );
    }

    [Fact]
    public void SymlinksAndForeignFilesRejectBeforeServingState()
    {
        using var directory = new OwnedDirectory();
        using (var store = DataGridFileStore.Bootstrap(directory.Path, Configuration(), 0)) { }
        var foreign = System.IO.Path.Combine(directory.Path, "foreign.txt");
        File.WriteAllText(foreign, "catalog");
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Open(directory.Path, Configuration(), 0)
        );
        File.Delete(foreign);
        var anchor = System.IO.Path.Combine(directory.Path, "CURRENT");
        var saved = File.ReadAllBytes(anchor);
        File.Delete(anchor);
        using var target = new OwnedDirectory();
        var targetFile = System.IO.Path.Combine(target.Path, "anchor");
        File.WriteAllBytes(targetFile, saved);
        File.CreateSymbolicLink(anchor, targetFile);
        Assert.Throws<IOException>(() =>
            DataGridFileStore.Open(directory.Path, Configuration(), 0)
        );
        Assert.Equal(saved, File.ReadAllBytes(targetFile));
    }

    [Fact]
    public void CapacityReservationsIncludeOldAndTemporaryFiles()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        Guid generation;
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            generation = initial.GenerationId;
        }
        var used = Directory.GetFiles(directory.Path).Sum(path => new FileInfo(path).Length);
        using var store = DataGridFileStore.Open(
            directory.Path,
            config,
            0,
            new(DirectoryBytes: used + 1)
        );
        var before = File.ReadAllBytes(directory.GenerationPath(generation));
        Assert.Throws<IOException>(() =>
            store.RollGeneration(TestContext.Current.CancellationToken)
        );
        Assert.False(store.IsFaulted);
        Assert.Equal(before, File.ReadAllBytes(directory.GenerationPath(generation)));
        Assert.Equal(
            2,
            Directory
                .GetFiles(directory.Path)
                .Count(path => !path.EndsWith("LOCK", StringComparison.Ordinal))
        );
    }

    [Fact]
    public void LoweredWalAndFreeSpaceBudgetsRejectBeforeWriting()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0)) { }
        var wal = Directory.GetFiles(directory.Path, "*.wal").Single();
        var original = File.ReadAllBytes(wal);
        using (
            var limited = DataGridFileStore.Open(
                directory.Path,
                config,
                0,
                new(WalBytes: original.Length + 1)
            )
        )
        {
            Assert.Throws<IOException>(() =>
                limited.Persist(Image(config, 2), TestContext.Current.CancellationToken)
            );
            Assert.False(limited.IsFaulted);
        }
        using (
            var limited = DataGridFileStore.Open(
                directory.Path,
                config,
                0,
                new(FreeReserveBytes: long.MaxValue)
            )
        )
            Assert.Throws<IOException>(() =>
                limited.Persist(Image(config, 2), TestContext.Current.CancellationToken)
            );
        Assert.Equal(original, File.ReadAllBytes(wal));
    }

    [Fact]
    public void InvalidTransitionsAreRejectedWithoutFaultingOrChangingFiles()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using var store = DataGridFileStore.Bootstrap(directory.Path, config, 0);
        store.Persist(Image(config, 2), TestContext.Current.CancellationToken);
        Assert.Throws<InvalidDataException>(() =>
            store.Persist(
                new(config, 2, 2, store.Image.Entries, 2),
                TestContext.Current.CancellationToken
            )
        );
        Assert.Throws<InvalidDataException>(() =>
            store.Persist(
                new(config, 3, 1, store.Image.Entries, 2),
                TestContext.Current.CancellationToken
            )
        );
        Assert.Throws<InvalidDataException>(() =>
            store.Persist(
                new(config, 3, 2, store.Image.Entries, 1),
                TestContext.Current.CancellationToken
            )
        );
        Assert.Throws<InvalidDataException>(() =>
            store.Persist(
                new(
                    config,
                    3,
                    2,
                    [
                        Entry(1, 1, DataGridCommittedCommand.AdvanceTime(default)),
                        .. store.Image.Entries.Skip(1),
                    ],
                    2
                ),
                TestContext.Current.CancellationToken
            )
        );
        Assert.False(store.IsFaulted);
        Assert.Equal(4, store.Image.LastIndex);
    }

    [Fact]
    public void LocalSnapshotCannotDropSuffixOrChangeKnownApplicationState()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using var store = DataGridFileStore.Bootstrap(directory.Path, config, 0);
        store.Persist(Image(config, 2), TestContext.Current.CancellationToken);
        var snapshot = store.RestoreApplication().SaveSnapshot(Nonce, 1);
        Assert.Throws<InvalidDataException>(() =>
            store.Persist(new(config, 3, 2, [], 2, snapshot), TestContext.Current.CancellationToken)
        );
        var changed = new DataGridLocalStateMachine(config);
        changed.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                Principal,
                Nonce,
                new(5 * TimeSpan.TicksPerSecond),
                default
            )
        );
        changed.Apply(2, Mutation(1, [8]));
        var foreignState = changed.SaveSnapshot(Nonce, 1);
        Assert.Throws<InvalidDataException>(() =>
            store.Persist(
                new(config, 3, 2, store.Image.Entries.Skip(2), 2, foreignState),
                TestContext.Current.CancellationToken
            )
        );
        Assert.False(store.IsFaulted);
        Assert.Single(Directory.GetFiles(directory.Path, "*.wal"));
        Assert.Empty(Directory.GetFiles(directory.Path, "*.bin"));
    }

    [Fact]
    public void RehashedInvalidWalTransitionsAreRejectedSemantically()
    {
        var config = Configuration();
        var seed = new[]
        {
            new DataGridWalRecord(
                DataGridWalRecordKind.Identity,
                1,
                DataGridWalRecovery.Identity(config, Member(0), Generation)
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.HardState,
                2,
                DataGridWalRecovery.HardState(config, new(config, 3, 2))
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.LogAppend,
                3,
                DataGridWalRecovery.Append(
                    Entry(1, 1, DataGridCommittedCommand.AdvanceTime(default))
                )
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.CommitCheckpoint,
                4,
                DataGridWalRecovery.Position(1, 1)
            ),
        };
        var invalid = new[]
        {
            new DataGridWalRecord(
                DataGridWalRecordKind.HardState,
                5,
                DataGridWalRecovery.HardState(config, new(config, 3, 1))
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.HardState,
                5,
                DataGridWalRecovery.HardState(config, new(config, 2, 2))
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.LogTruncate,
                5,
                DataGridWalRecovery.Position(1)
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.LogTruncate,
                5,
                DataGridWalRecovery.Position(3)
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.LogAppend,
                5,
                DataGridWalRecovery.Append(
                    Entry(3, 2, DataGridCommittedCommand.AdvanceTime(default))
                )
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.LogAppend,
                5,
                DataGridWalRecovery.Append(
                    Entry(2, 4, DataGridCommittedCommand.AdvanceTime(default))
                )
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.CommitCheckpoint,
                5,
                DataGridWalRecovery.Position(1, 2)
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.CommitCheckpoint,
                5,
                DataGridWalRecovery.Position(2, 1)
            ),
            new DataGridWalRecord(
                DataGridWalRecordKind.Identity,
                5,
                DataGridWalRecovery.Identity(config, Member(0), Generation)
            ),
        };
        var valid = seed.SelectMany(record => record.Encode()).ToArray();
        foreach (var record in invalid)
        {
            using var stream = new MemoryStream(valid.Concat(record.Encode()).ToArray());
            Assert.Throws<InvalidDataException>(() =>
                DataGridWalRecovery.Read(
                    stream,
                    config,
                    Member(0),
                    Generation,
                    _ => throw new InvalidOperationException()
                )
            );
        }
        using var emptySeed = new MemoryStream(
            seed.Take(3).SelectMany(record => record.Encode()).ToArray()
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridWalRecovery.Read(
                emptySeed,
                config,
                Member(0),
                Generation,
                _ => throw new InvalidOperationException()
            )
        );
    }

    [Fact]
    public void WalStreamValidatesHeaderBeforeAllocatingDeclaredPayload()
    {
        var header = new byte[20];
        "HLGW"u8.CopyTo(header);
        BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(4), 1);
        BinaryPrimitives.WriteUInt16BigEndian(header.AsSpan(6), 3);
        BinaryPrimitives.WriteUInt32BigEndian(header.AsSpan(8), uint.MaxValue);
        BinaryPrimitives.WriteInt64BigEndian(header.AsSpan(12), 1);
        using var stream = new MemoryStream(header);
        Assert.Throws<InvalidDataException>(() => DataGridWalRecovery.ReadRecord(stream, 1));
        Assert.Equal(20, stream.Position);
    }

    [Fact]
    public void FilePersistenceCompletesFollowerEffectBeforeItCanAcknowledgeAppend()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using var store = DataGridFileStore.Bootstrap(directory.Path, config, 1);
        var node = new RaftNode(config, 1, 1, store.Image);
        var request = new RaftMessage(
            RaftMessageKind.AppendRequest,
            0,
            1,
            1,
            new(1, 1),
            Cluster,
            config.ComputeDigest(),
            commit: 1,
            entries: [Entry(1, 1, DataGridCommittedCommand.AdvanceTime(default))]
        );
        var effects = node.Receive(request, default);
        Assert.Empty(effects.OfType<RaftSend>());
        while (effects.OfType<RaftWrite>().SingleOrDefault() is { } write)
        {
            store.Persist(write.Image, TestContext.Current.CancellationToken);
            effects = node.Persisted(write.Token, true);
        }
        Assert.True(Assert.Single(effects.OfType<RaftSend>()).Message.Accepted);
        Assert.Equal(1, store.Image.CommitIndex);
        Assert.Equal(1, node.Machine.LastAppliedIndex);
    }

    [Theory(Timeout = 30_000)]
    [InlineData("append", (int)DataGridStorageStep.WalWritten)]
    [InlineData("append", (int)DataGridStorageStep.WalFlushed)]
    [InlineData("snapshot", (int)DataGridStorageStep.SnapshotFlushed)]
    [InlineData("snapshot", (int)DataGridStorageStep.GenerationDirectoryFlushed)]
    [InlineData("snapshot", (int)DataGridStorageStep.AnchorDirectoryFlushed)]
    public async Task KilledProcessRecoversFromFlushedWalOrSelectedGeneration(
        string operation,
        int stage
    )
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        Guid previous;
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            initial.Persist(Image(config, 2), TestContext.Current.CancellationToken);
            previous = initial.GenerationId;
        }
        await KillProbe(
            directory.Path,
            operation,
            stage,
            () => { },
            TestContext.Current.CancellationToken
        );
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        if (operation == "append")
        {
            if (stage == (int)DataGridStorageStep.WalFlushed)
                Assert.Equal(5, recovered.Image.CommitIndex);
            else
                Assert.Contains(recovered.Image.CommitIndex, new long[] { 2, 5 });
            Assert.Equal(previous, recovered.GenerationId);
        }
        else
        {
            Assert.Equal(2, recovered.Image.CommitIndex);
            Assert.Equal(4, recovered.Image.LastIndex);
            Assert.Equal(
                stage == (int)DataGridStorageStep.AnchorDirectoryFlushed,
                recovered.GenerationId != previous
            );
            Assert.Equal(2, recovered.Image.Vote);
        }
    }

    [Fact(Timeout = 30_000)]
    public async Task DirectoryOwnershipExcludesAnotherProcessAndIsReleasedByProcessDeath()
    {
        using var directory = new OwnedDirectory();
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, Configuration(), 0)) { }
        await KillProbe(
            directory.Path,
            "hold",
            -1,
            () =>
                Assert.Throws<IOException>(() =>
                    DataGridFileStore.Open(directory.Path, Configuration(), 0)
                ),
            TestContext.Current.CancellationToken
        );
        using var recovered = DataGridFileStore.Open(directory.Path, Configuration(), 0);
        Assert.Equal(0, recovered.Image.CommitIndex);
    }

    private static async Task KillProbe(
        string directory,
        string operation,
        int stage,
        Action whileOwned,
        CancellationToken cancellationToken
    )
    {
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        var start = new ProcessStartInfo("dotnet")
        {
            UseShellExecute = false,
            RedirectStandardOutput = true,
            RedirectStandardError = true,
            RedirectStandardInput = true,
        };
        start.ArgumentList.Add(
            Path.Combine(AppContext.BaseDirectory, "HostLoom.DataGrid.StorageProbe.dll")
        );
        start.ArgumentList.Add(directory);
        start.ArgumentList.Add(operation);
        start.ArgumentList.Add(stage.ToString(CultureInfo.InvariantCulture));
        using var process = Process.Start(start)!;
        var errors = process.StandardError.ReadToEndAsync(timeout.Token);
        try
        {
            var line = await process.StandardOutput.ReadLineAsync(timeout.Token);
            if (line != "READY")
            {
                await process.WaitForExitAsync(timeout.Token);
                Assert.Fail($"Storage probe did not reach its pause: {line}; {await errors}");
            }
            whileOwned();
        }
        finally
        {
            if (!process.HasExited)
                process.Kill(entireProcessTree: true);
            using var cleanup = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            await process.WaitForExitAsync(cleanup.Token);
        }
        Assert.Equal(string.Empty, await errors);
    }

    private static List<int> RecordOffsets(byte[] bytes)
    {
        var offsets = new List<int>();
        for (var offset = 0; offset < bytes.Length; )
        {
            offsets.Add(offset);
            offset += 52 + (int)BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(offset + 8));
        }
        return offsets;
    }

    private static RaftEntry Entry(long index, long term, DataGridCommittedCommand command) =>
        new(index, term, DataGridCommittedCommandCodec.Encode(command));

    private static DataGridCommittedCommand Mutation(ulong sequence, byte[] value) =>
        DataGridCommittedCommand.Mutate(
            Principal,
            new(
                DataGridMutationKind.Put,
                Map,
                1,
                sequence,
                DataGridExpiryKind.Never,
                0,
                [new(Key, value)]
            ),
            default
        );

    private static RaftDurableImage Image(DataGridBootstrapConfiguration config, long commit) =>
        new(
            config,
            3,
            2,
            [
                Entry(
                    1,
                    1,
                    DataGridCommittedCommand.OpenSession(
                        Principal,
                        Nonce,
                        new(5 * TimeSpan.TicksPerSecond),
                        default
                    )
                ),
                Entry(2, 1, Mutation(1, [1])),
                Entry(3, 2, Mutation(2, [2])),
                Entry(4, 3, DataGridCommittedCommand.AdvanceTime(default)),
            ],
            commit
        );

    private static Guid Member(int index) => new(index + 1, 0, 0, new byte[8]);

    private static DataGridBootstrapConfiguration Configuration(int keys = 1000) =>
        new(
            Cluster,
            [
                new(Member(0), "node-a.example", 7400),
                new(Member(1), "node-b.example", 7400),
                new(Member(2), "node-c.example", 7400),
            ],
            [
                new(
                    Map,
                    "catalog",
                    DataGridMapOwnership.Derived,
                    "catalog-service",
                    "catalog-loader",
                    keys,
                    1048576,
                    new(1, 1, 2, 1, 1)
                ),
            ]
        );

    private sealed class OwnedDirectory : IDisposable
    {
        internal OwnedDirectory()
        {
            Assert.SkipUnless(
                OperatingSystem.IsLinux() || OperatingSystem.IsMacOS(),
                "Unix storage experiment."
            );
            Path = System.IO.Path.Combine(
                System.IO.Path.GetTempPath(),
                "hostloom-storage-" + Guid.NewGuid().ToString("N")
            );
            Directory.CreateDirectory(Path);
        }

        internal string Path { get; }

        internal string GenerationPath(Guid id) =>
            System.IO.Path.Combine(Path, $"generation-{id:N}.wal");

        public void Dispose() => Directory.Delete(Path, recursive: true);
    }
}
