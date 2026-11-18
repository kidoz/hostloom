using System.Collections;
using System.Reflection;
using System.Security.Cryptography;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Storage;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridSnapshotAllocationTests
{
    [Fact]
    public void RaftSharesPublishedSnapshotValuesAfterAcknowledgementAndRestart()
    {
        using var fixture = new DataGridPeerFixture();
        var configuration = fixture.Configuration;
        var value = Enumerable.Repeat((byte)42, 1024).ToArray();
        var open = DataGridCommittedCommand.OpenSession(
            DataGridPeerFixture.Principal,
            DataGridPeerFixture.Nonce,
            new(4 * TimeSpan.TicksPerSecond),
            default
        );
        var node = new RaftNode(
            configuration,
            0,
            1,
            new(
                configuration,
                1,
                null,
                [
                    new RaftEntry(1, 1, DataGridCommittedCommandCodec.Encode(open)),
                    new RaftEntry(
                        2,
                        1,
                        DataGridCommittedCommandCodec.Encode(
                            DataGridFileStorageTests.Mutation(1, value)
                        )
                    ),
                ],
                2
            )
        );
        var previousValue = ReadEntryStorage(node.Machine);
        var write = Assert.Single(node.Compact(Guid.NewGuid()).OfType<RaftWrite>());
        var snapshot = write.Image.Snapshot!;
        var publishedValue = snapshot.Content.ReadValue(
            snapshot.GetContent().AsSpan().IndexOf(value),
            value.Length,
            borrow: true
        );
        Assert.True(previousValue.Equals(ReadEntryStorage(node.Machine)));
        Assert.False(publishedValue.Equals(previousValue));

        node.Persisted(write.Token, true);
        Assert.True(publishedValue.Equals(ReadEntryStorage(node.Machine)));
        var restarted = new RaftNode(configuration, 0, 2, write.Image);
        Assert.True(publishedValue.Equals(ReadEntryStorage(restarted.Machine)));

        var ordinary = DataGridLocalStateMachine.RestoreSnapshot(configuration, snapshot);
        Assert.False(publishedValue.Equals(ReadEntryStorage(ordinary)));
        var replacement = new byte[] { 7, 8, 9 };
        var applied = restarted.Machine.Apply(3, DataGridFileStorageTests.Mutation(2, replacement));
        Assert.Equal(DataGridMutationStatus.Stored, applied.GetItem(0).Status);
        Assert.Equal(replacement, ReadEntryStorage(restarted.Machine).ToArray());
        Assert.Equal(value, publishedValue.ToArray());
        Assert.Equal(
            value,
            node.Machine.Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key)!.GetValue()
        );
    }

    private static ReadOnlyMemory<byte> ReadEntryStorage(DataGridLocalStateMachine machine)
    {
        const BindingFlags flags =
            BindingFlags.Instance | BindingFlags.Public | BindingFlags.NonPublic;
        var maps = (Array)machine.GetType().GetField("_maps", flags)!.GetValue(machine)!;
        var map = maps.GetValue(0)!;
        var entries = (IEnumerable)map.GetType().GetProperty("Entries", flags)!.GetValue(map)!;
        var entry = Assert.Single(entries.Cast<object>());
        return (ReadOnlyMemory<byte>)entry.GetType().GetProperty("Value", flags)!.GetValue(entry)!;
    }

    [Fact]
    public void FirstSnapshotValidationDoesNotAllocateTheRestoredApplication()
    {
        using var fixture = new DataGridPeerFixture(10000, 16 * 1024 * 1024);
        var configuration = fixture.Configuration;
        var machine = Populate(configuration, DataGridFeasibilityCase.Key);
        machine.SaveSnapshot(Guid.NewGuid(), 1).Validate(configuration);
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);

        var before = GC.GetAllocatedBytesForCurrentThread();
        snapshot.Validate(configuration);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;

        TestContext.Current.TestOutputHelper!.WriteLine(
            $"First validation allocated {allocated} bytes for an image of {snapshot.Descriptor.ByteCount} bytes."
        );
        // Allow room for the compact version index and fields crossing a content block.
        Assert.True(allocated < 512 * 1024, $"Validation allocated {allocated} bytes.");
    }

    [Fact]
    public void SnapshotBackingArraysStayBoundedWhenTheCanonicalImageGrows()
    {
        using var fixture = new DataGridPeerFixture(10000, 16 * 1024 * 1024);
        var machine = Populate(fixture.Configuration, DataGridFeasibilityCase.Key);
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var seen = new HashSet<object>(ReferenceEqualityComparer.Instance);
        var largest = 0;
        void Visit(object? value)
        {
            if (value is null || !seen.Add(value))
                return;
            if (value is byte[] bytes)
                largest = Math.Max(largest, bytes.Length);
            else if (value is Array array)
                foreach (var child in array)
                    Visit(child);
            else if (value.GetType().Namespace == "HostLoom.DataGrid")
                foreach (
                    var field in value
                        .GetType()
                        .GetFields(
                            System.Reflection.BindingFlags.Instance
                                | System.Reflection.BindingFlags.Public
                                | System.Reflection.BindingFlags.NonPublic
                        )
                )
                    Visit(field.GetValue(value));
        }
        Visit(snapshot);
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Canonical image {snapshot.Descriptor.ByteCount} bytes; largest retained byte array {largest} bytes."
        );
        Assert.True(snapshot.Descriptor.ByteCount > 8 * 1024 * 1024);
        Assert.InRange(largest, 1, 65536);
    }

    [Theory]
    [InlineData(0)]
    [InlineData(1024)]
    public void BorrowedVerifierRebuildsStateAndReplacesValuesWithoutChangingItsSnapshot(int length)
    {
        using var fixture = new DataGridPeerFixture();
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                DataGridPeerFixture.Nonce,
                new(4 * TimeSpan.TicksPerSecond),
                default
            )
        );
        var originalValue = Enumerable.Repeat((byte)42, length).ToArray();
        machine.Apply(2, DataGridFileStorageTests.Mutation(1, originalValue));
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var originalBytes = snapshot.GetContent();
        var borrowed = DataGridLocalStateMachine.RestoreForVerification(
            fixture.Configuration,
            snapshot
        );
        var copiedInput = snapshot.GetContent();
        var copied = DataGridLocalStateMachine.Restore(
            fixture.Configuration,
            snapshot.Descriptor,
            copiedInput
        );
        borrowed.VerifyCanonicalSnapshot(snapshot);
        Assert.Equal(machine.StoredDataByteCount, borrowed.StoredDataByteCount);
        var replay = borrowed.Apply(3, DataGridFileStorageTests.Mutation(1, originalValue));
        Assert.Equal(2, replay.GetItem(0).Version.LogIndex);
        var replacement = new byte[] { 7, 8, 9 };
        borrowed.Apply(4, DataGridFileStorageTests.Mutation(2, replacement));
        var result = borrowed.Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key)!;
        Assert.Equal(replacement, result.GetValue());
        Assert.Equal(4, result.Version.LogIndex);
        Array.Fill(copiedInput, (byte)255);
        Assert.Equal(
            originalValue,
            copied.Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key)!.GetValue()
        );
        Assert.Equal(
            originalValue,
            machine.Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key)!.GetValue()
        );
        Assert.Equal(originalBytes, snapshot.GetContent());
    }

    [Fact]
    public void BorrowedVerifierStillChecksDigestAndConfigurationAfterPriorValidation()
    {
        using var fixture = new DataGridPeerFixture();
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        machine.Apply(1, DataGridCommittedCommand.AdvanceTime(default));
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        snapshot.Validate(fixture.Configuration);
        var damaged = snapshot.GetContent();
        damaged[^1] ^= 1;
        Assert.Throws<InvalidDataException>(() =>
            DataGridLocalStateMachine.RestoreForVerification(
                fixture.Configuration,
                new(snapshot.Descriptor, damaged)
            )
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridLocalStateMachine.RestoreForVerification(
                DataGridFileStorageTests.Configuration(1001),
                snapshot
            )
        );
    }

    [Fact]
    public void ComparisonWriterChecksEveryByteAndCompletionWithoutChangingBorrowedInput()
    {
        var expected = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
        var original = (byte[])expected.Clone();
        using (var exact = GridWriter.Comparing(expected))
        {
            exact.Raw(expected.AsSpan(0, 7));
            exact.Raw(expected.AsSpan(7));
            exact.CompleteComparison();
            Assert.Throws<InvalidOperationException>(() => exact.ToArray());
            Assert.Throws<InvalidOperationException>(() => exact.DetachExactContent());
        }
        for (var i = 0; i < expected.Length; i++)
        {
            var changed = (byte[])expected.Clone();
            changed[i] ^= 1;
            using var comparison = GridWriter.Comparing(expected);
            Assert.Throws<InvalidDataException>(() =>
            {
                comparison.Raw(changed.AsSpan(0, 7));
                comparison.Raw(changed.AsSpan(7));
                comparison.CompleteComparison();
            });
        }
        using (var incomplete = GridWriter.Comparing(expected))
        {
            incomplete.Raw(expected.AsSpan(0, 31));
            Assert.Throws<InvalidDataException>(() => incomplete.CompleteComparison());
        }
        Assert.Equal(original, expected);
    }

    [Fact]
    public void KnownPrefixComparisonDoesNotAllocateAnotherFullEncodedImage()
    {
        var token = TestContext.Current.CancellationToken;
        using var directory = new DataGridFileStorageTests.OwnedDirectory();
        using var fixture = new DataGridPeerFixture(10000, 16 * 1024 * 1024);
        var config = fixture.Configuration;
        var machine = Populate(config, DataGridFeasibilityCase.Key);
        var oldSnapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var nextSnapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var target = new RaftDurableImage(
            config,
            1,
            commitIndex: machine.LastAppliedIndex,
            snapshot: nextSnapshot
        );
        using var store = DataGridFileStore.Bootstrap(directory.Path, config, 0);
        store.Persist(
            new(config, 1, commitIndex: machine.LastAppliedIndex, snapshot: oldSnapshot),
            token
        );
        _ = store.RestoreApplication();
        var before = GC.GetAllocatedBytesForCurrentThread();
        var independent = store.RestoreApplication();
        var reconstruction = GC.GetAllocatedBytesForCurrentThread() - before;
        before = GC.GetAllocatedBytesForCurrentThread();
        store.Persist(target, token);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        GC.KeepAlive(independent);
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Known prefix: {allocated} allocated bytes, reconstruction {reconstruction} bytes; snapshot {nextSnapshot.Descriptor.ByteCount} bytes."
        );
        // Independent reconstruction is required. Its canonical comparison must not add an image-sized output buffer.
        Assert.True(allocated - reconstruction < nextSnapshot.Descriptor.ByteCount);
        // Prefix verification may borrow immutable durable values while rebuilding its own
        // indexes. It must avoid copying most of this 10000 x 1024-byte payload again.
        Assert.True(reconstruction - allocated > 10000L * 1024 * 3 / 4);
        Assert.Equal(
            nextSnapshot.Descriptor.SnapshotId,
            store.Image.Snapshot!.Descriptor.SnapshotId
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void GenerationReadbackUsesBoundedBuffersForNewAndExistingSnapshots(bool rollExisting)
    {
        using var directory = new DataGridFileStorageTests.OwnedDirectory();
        using var fixture = new DataGridPeerFixture(10000, 16 * 1024 * 1024);
        var config = fixture.Configuration;
        var machine = Populate(config, DataGridFeasibilityCase.Key);
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var target = new RaftDurableImage(
            config,
            1,
            commitIndex: machine.LastAppliedIndex,
            snapshot: snapshot
        );
        using (var store = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            if (rollExisting)
                store.Persist(target, TestContext.Current.CancellationToken);
            var before = GC.GetAllocatedBytesForCurrentThread();
            if (rollExisting)
                store.RollGeneration(TestContext.Current.CancellationToken);
            else
                store.Persist(target, TestContext.Current.CancellationToken);
            var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
            TestContext.Current.TestOutputHelper!.WriteLine(
                $"Generation readback (existing={rollExisting}): {allocated} allocated bytes; snapshot {snapshot.Descriptor.ByteCount} bytes."
            );
            // Verification must not allocate even one image or another reconstructed application.
            Assert.True(
                allocated < snapshot.Descriptor.ByteCount,
                $"Generation readback allocated {allocated} bytes for {snapshot.Descriptor.ByteCount} snapshot bytes."
            );
            Assert.Same(snapshot, store.Image.Snapshot);
        }
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(snapshot.GetContent(), recovered.Image.Snapshot!.GetContent());
        for (var offset = 0; offset < recovered.Image.Snapshot.Content.Length; )
        {
            var block = recovered.Image.Snapshot.Content.BlockAt(offset);
            Assert.InRange(block.Length, 1, 65536);
            Assert.True(
                System.Runtime.InteropServices.MemoryMarshal.TryGetArray(block, out var storage)
            );
            Assert.InRange(storage.Array!.Length, 1, 65536);
            offset += block.Length;
        }
        var restored = recovered.RestoreApplication();
        Assert.Equal(
            new byte[1024],
            restored.Read(DataGridPeerFixture.Map, DataGridFeasibilityCase.Key(0))!.GetValue()
        );
        var replay = restored.Apply(
            restored.LastAppliedIndex + 1,
            DataGridCommittedCommand.Mutate(
                DataGridPeerFixture.Principal,
                new(
                    DataGridMutationKind.Put,
                    DataGridPeerFixture.Map,
                    1,
                    79,
                    DataGridExpiryKind.Never,
                    0,
                    Enumerable
                        .Range(9984, 16)
                        .Select(i => new DataGridMutationItem(
                            DataGridFeasibilityCase.Key(i),
                            new byte[1024]
                        ))
                        .ToArray()
                ),
                default
            )
        );
        Assert.Equal(snapshot.Descriptor.LastIncludedIndex, replay.GetItem(0).Version.LogIndex);
    }

    [Fact]
    public void SnapshotEncodingStaysV1WithoutMaterializingSeparateBodyCopies()
    {
        var config = new DataGridBootstrapConfiguration(
            DataGridPeerFixture.Cluster,
            Enumerable
                .Range(0, 3)
                .Select(i => new DataGridMemberConfiguration(
                    new Guid(i + 1, 0, 0, new byte[8]),
                    $"node-{(char)('a' + i)}.example",
                    (ushort)(14001 + i)
                ))
                .ToArray(),
            [
                new(
                    DataGridPeerFixture.Map,
                    "catalog",
                    DataGridMapOwnership.Authoritative,
                    "catalog",
                    "",
                    10000,
                    16 * 1024 * 1024,
                    new(1, 1, 2, 1, 1)
                ),
            ]
        );
        var machine = Populate(
            config,
            i => DataGridKey.FromUtf8(FormattableString.Invariant($"catalog:{i:D8}"))
        );
        var id = Guid.Parse("50112233-4455-6677-8899-aabbccddeeff");
        _ = machine.SaveSnapshot(id, 1);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var snapshot = machine.SaveSnapshot(id, 1);
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Snapshot capture: {allocated} allocated bytes; snapshot {snapshot.Descriptor.ByteCount} bytes."
        );
        // Captured from the prior v1 encoder with this fixed input; protects byte compatibility.
        Assert.Equal(
            "DF475C01C5AB6F79BE2E26D1A9DB6760071F76BD4D9B25BCB5ED283B1FEC9BBA",
            Convert.ToHexString(SHA256.HashData(snapshot.GetContent()))
        );
        // One exact owned buffer plus small per-key metadata, rather than full-image copies.
        Assert.True(allocated < 2 * snapshot.Descriptor.ByteCount);
    }

    [Fact]
    public void ValidationReuseRemainsBoundToSnapshotAndConfigurationIdentity()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var machine = new DataGridLocalStateMachine(config);
        machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                Guid.NewGuid(),
                new(TimeSpan.TicksPerSecond * 4),
                default
            )
        );
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        Assert.Throws<ArgumentNullException>(() =>
            new RaftDurableImage(null!, 1, commitIndex: 1, snapshot: snapshot)
        );
        void Validate(
            DataGridBootstrapConfiguration configuration,
            DataGridLocalSnapshot candidate
        ) => _ = new RaftDurableImage(configuration, 1, commitIndex: 1, snapshot: candidate);
        Validate(config, snapshot);
        var members = Enumerable.Range(0, 3).Select(config.GetMember).ToArray();
        var maps = Enumerable.Range(0, config.MapCount).Select(config.GetMap).ToArray();
        var other = new DataGridBootstrapConfiguration(Guid.NewGuid(), members, maps);
        Assert.Throws<InvalidDataException>(() => Validate(other, snapshot));
        Validate(new(config.IncarnationId, members, maps), snapshot);
        Validate(config, snapshot);
        var damaged = snapshot.GetContent();
        damaged[0] ^= 1;
        Assert.Throws<InvalidDataException>(() =>
            Validate(config, new(snapshot.Descriptor, damaged))
        );
        var consistentDigest = new DataGridSnapshotDescriptor(
            snapshot.Descriptor.SnapshotId,
            1,
            1,
            damaged.Length,
            SHA256.HashData(damaged)
        );
        Assert.Throws<InvalidDataException>(() => Validate(config, new(consistentDigest, damaged)));
        Validate(config, snapshot);
    }

    [Fact]
    public void ConcurrentFirstValidationPreservesSnapshotPublicCopyIsolation()
    {
        using var fixture = new DataGridPeerFixture();
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                Guid.NewGuid(),
                new(TimeSpan.TicksPerSecond * 4),
                default
            )
        );
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        var original = snapshot.GetContent();
        Parallel.For(
            0,
            32,
            _ =>
            {
                snapshot.GetContent()[0] ^= 1;
                var image = new RaftDurableImage(
                    fixture.Configuration,
                    1,
                    commitIndex: 1,
                    snapshot: snapshot
                );
                Assert.Same(snapshot, image.Snapshot);
            }
        );
        Assert.Equal(original, snapshot.GetContent());
    }

    [Fact]
    public void ReusingValidatedSnapshotDoesNotRebuildItsApplicationForEveryDurableImage()
    {
        using var fixture = new DataGridPeerFixture(10000, 16 * 1024 * 1024);
        var machine = Populate(fixture.Configuration, DataGridFeasibilityCase.Key);
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        _ = new RaftDurableImage(
            fixture.Configuration,
            1,
            commitIndex: machine.LastAppliedIndex,
            snapshot: snapshot
        );
        var before = GC.GetAllocatedBytesForCurrentThread();
        for (var i = 0; i < 8; i++)
            _ = new RaftDurableImage(
                fixture.Configuration,
                1,
                commitIndex: machine.LastAppliedIndex,
                snapshot: snapshot
            );
        var allocated = GC.GetAllocatedBytesForCurrentThread() - before;
        TestContext.Current.TestOutputHelper!.WriteLine(
            $"Eight same-snapshot durable images: {allocated} allocated bytes; snapshot {snapshot.Descriptor.ByteCount} bytes."
        );
        // An algorithmic allocation bound, not a service memory/latency target: unchanged
        // durable metadata must not deserialize or copy a complete application image.
        Assert.True(
            allocated < snapshot.Descriptor.ByteCount,
            $"Reusing a snapshot allocated {allocated} bytes for {snapshot.Descriptor.ByteCount} bytes of state."
        );
    }

    private static DataGridLocalStateMachine Populate(
        DataGridBootstrapConfiguration config,
        Func<int, DataGridKey> key
    )
    {
        var machine = new DataGridLocalStateMachine(config);
        var opened = machine.Apply(
            1,
            DataGridCommittedCommand.OpenSession(
                DataGridPeerFixture.Principal,
                DataGridPeerFixture.Nonce,
                new(TimeSpan.TicksPerSecond * 4),
                default
            )
        );
        ulong sequence = 1;
        for (var first = 0; first < 10000; first += 128)
        {
            var items = Enumerable
                .Range(first, Math.Min(128, 10000 - first))
                .Select(i => new DataGridMutationItem(key(i), new byte[1024]))
                .ToArray();
            var result = machine.Apply(
                machine.LastAppliedIndex + 1,
                DataGridCommittedCommand.Mutate(
                    DataGridPeerFixture.Principal,
                    new(
                        DataGridMutationKind.Put,
                        DataGridPeerFixture.Map,
                        opened.SessionNumber,
                        sequence++,
                        DataGridExpiryKind.Never,
                        0,
                        items
                    ),
                    default
                )
            );
            Assert.All(
                Enumerable.Range(0, result.ItemCount),
                i => Assert.Equal(DataGridMutationStatus.Stored, result.GetItem(i).Status)
            );
        }
        return machine;
    }
}
