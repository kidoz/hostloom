using System.Buffers.Binary;
using System.Security.Cryptography;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Storage;
using Xunit;
using static HostLoom.Tests.DataGridFileStorageTests;

namespace HostLoom.Tests;

public sealed class DataGridWalFrontierTests
{
    [Fact]
    public void V2AnchorHasIndependentLayoutChecksumBoundsAndDefensiveDigest()
    {
        var cluster = new Guid("10112233-4455-6677-8899-aabbccddeeff");
        var member = new Guid(1, 0, 0, new byte[8]);
        var generation = new Guid("30112233-4455-6677-8899-aabbccddeeff");
        var digest = Enumerable.Range(0, 32).Select(i => (byte)i).ToArray();
        var anchor = new DataGridCurrentAnchor(cluster, member, generation, 512, 3, digest);
        var expected = Convert.FromHexString(
            "484C47430002000010112233445566778899AABBCCDDEEFF"
                + "0000000100000000000000000000000030112233445566778899AABBCCDDEEFF"
                + "00000000000002000000000000000003000102030405060708090A0B0C0D0E0F"
                + "101112131415161718191A1B1C1D1E1F"
        );
        expected = expected.Concat(SHA256.HashData(expected)).ToArray();
        Assert.Equal(136, expected.Length);
        Assert.Equal(expected, anchor.Encode());
        digest[0] ^= 1;
        anchor.GetWalDigest()[0] ^= 1;
        Assert.Equal(expected, anchor.Encode());
        var decoded = DataGridCurrentAnchor.ReadFrom(expected);
        Assert.Equal(2, decoded.FormatVersion);
        Assert.Equal(512, decoded.PublishedWalBytes);
        Assert.Equal(3, decoded.PublishedRecordCount);
        Assert.Equal(expected, decoded.Encode());
        for (var i = 0; i < expected.Length; i++)
        {
            var corrupt = (byte[])expected.Clone();
            corrupt[i] ^= 1;
            Assert.Throws<InvalidDataException>(() => DataGridCurrentAnchor.ReadFrom(corrupt));
            Assert.Throws<InvalidDataException>(() =>
                DataGridCurrentAnchor.ReadFrom(expected.AsSpan(0, i))
            );
        }
        foreach (
            var (offset, value) in new (int, long)[]
            {
                (56, 0),
                (56, 1073741825),
                (64, 2),
                (64, 1000001),
            }
        )
        {
            var invalid = (byte[])expected.Clone();
            BinaryPrimitives.WriteInt64BigEndian(invalid.AsSpan(offset), value);
            SHA256.HashData(invalid.AsSpan(0, 104), invalid.AsSpan(104));
            Assert.Throws<InvalidDataException>(() => DataGridCurrentAnchor.ReadFrom(invalid));
        }
    }

    [Theory]
    [InlineData((int)DataGridStorageStep.WalWritten)]
    [InlineData((int)DataGridStorageStep.WalFlushed)]
    [InlineData((int)DataGridStorageStep.AnchorWriteStarting)]
    [InlineData((int)DataGridStorageStep.AnchorFlushed)]
    [InlineData((int)DataGridStorageStep.AnchorRenamed)]
    [InlineData((int)DataGridStorageStep.AnchorDirectoryFlushed)]
    public void AppendPublishesHardStateLogAndCommitOnlyAtSelectedFrontier(int stage)
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0))
            initial.Persist(Image(config, 2), TestContext.Current.CancellationToken);
        using (
            var store = DataGridFileStore.Open(
                directory.Path,
                config,
                0,
                observe: step =>
                {
                    if ((int)step == stage)
                        throw new IOException("Interrupted publication.");
                }
            )
        )
        {
            Assert.Throws<IOException>(() =>
                store.Persist(
                    Replacement(store.Image, config),
                    TestContext.Current.CancellationToken
                )
            );
            Assert.True(store.IsFaulted);
            Assert.Equal(2, store.Progress.DurableCommitIndex);
        }
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        var published =
            stage
            is (int)DataGridStorageStep.AnchorRenamed
                or (int)DataGridStorageStep.AnchorDirectoryFlushed;
        Assert.Equal(published ? 4 : 3, recovered.Image.Term);
        Assert.Equal(published ? 1 : 2, recovered.Image.Vote);
        Assert.Equal(published ? 3 : 2, recovered.Image.CommitIndex);
        Assert.Equal(published ? 3 : 4, recovered.Image.LastIndex);
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void UnpublishedCompleteOrTornBytesStayUntouchedUntilFreshGenerationPublication(
        bool torn
    )
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0))
            initial.Persist(Image(config, 2), TestContext.Current.CancellationToken);
        var path = DataGridDiskFullTests.Selected(directory.Path);
        var anchor = DataGridCurrentAnchor.ReadFrom(
            File.ReadAllBytes(Path.Combine(directory.Path, "CURRENT"))
        );
        var suffix = new DataGridWalRecord(
            DataGridWalRecordKind.HardState,
            anchor.PublishedRecordCount!.Value + 1,
            DataGridWalRecovery.HardState(config, new(config, 4, 1, Image(config, 2).Entries, 2))
        ).Encode();
        using (var file = new FileStream(path, FileMode.Append))
            file.Write(suffix.AsSpan(0, torn ? suffix.Length - 1 : suffix.Length));
        var preserved = DataGridDiskFullTests.Files(directory.Path);
        using (var store = DataGridFileStore.Open(directory.Path, config, 0))
        {
            Assert.Equal(preserved, DataGridDiskFullTests.Files(directory.Path));
            Assert.Equal(3, store.Image.Term);
            Assert.Equal(2, store.Image.Vote);
            Assert.Equal(2, store.Image.CommitIndex);
            Assert.Equal(4, store.Image.LastIndex);
            var previous = File.ReadAllBytes(path);
            store.Persist(Replacement(store.Image, config), TestContext.Current.CancellationToken);
            Assert.NotEqual(path, DataGridDiskFullTests.Selected(directory.Path));
            Assert.Equal(previous, File.ReadAllBytes(path));
        }
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(4, recovered.Image.Term);
        Assert.Equal(1, recovered.Image.Vote);
        Assert.Equal(3, recovered.Image.CommitIndex);
    }

    [Theory]
    [InlineData((int)DataGridStorageStep.GenerationFlushed)]
    [InlineData((int)DataGridStorageStep.AnchorFlushed)]
    [InlineData((int)DataGridStorageStep.AnchorRenamed)]
    [InlineData((int)DataGridStorageStep.AnchorDirectoryFlushed)]
    public void InterruptedTailRolloverNeverChangesPublishedHardStateOrCommittedPrefix(int stage)
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0))
            initial.Persist(Image(config, 2), TestContext.Current.CancellationToken);
        var path = DataGridDiskFullTests.Selected(directory.Path);
        using (var file = new FileStream(path, FileMode.Append))
            file.WriteByte(255);
        var before = File.ReadAllBytes(path);
        using (
            var store = DataGridFileStore.Open(
                directory.Path,
                config,
                0,
                observe: step =>
                {
                    if ((int)step == stage)
                        throw new IOException("Interrupted rollover.");
                }
            )
        )
            Assert.Throws<IOException>(() =>
                store.RollGeneration(TestContext.Current.CancellationToken)
            );
        Assert.Equal(before, File.ReadAllBytes(path));
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(3, recovered.Image.Term);
        Assert.Equal(2, recovered.Image.Vote);
        Assert.Equal(2, recovered.Image.CommitIndex);
        Assert.Equal(4, recovered.Image.LastIndex);
        recovered.RollGeneration(TestContext.Current.CancellationToken);
        Assert.NotEqual(path, DataGridDiskFullTests.Selected(directory.Path));
    }

    [Theory]
    [InlineData("short")]
    [InlineData("corrupt")]
    [InlineData("rehashed-hard-state")]
    [InlineData("count")]
    [InlineData("digest")]
    public void PublishedPrefixOrFrontierDamageRefusesWithoutFallbackOrFileChanges(string damage)
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0))
        {
            initial.Persist(Image(config, 2), TestContext.Current.CancellationToken);
            initial.RollGeneration(TestContext.Current.CancellationToken);
        }
        var path = DataGridDiskFullTests.Selected(directory.Path);
        var anchorPath = Path.Combine(directory.Path, "CURRENT");
        var anchor = DataGridCurrentAnchor.ReadFrom(File.ReadAllBytes(anchorPath));
        var bytes = File.ReadAllBytes(path);
        if (damage == "short")
            File.WriteAllBytes(path, bytes[..^1]);
        else if (damage == "corrupt")
        {
            bytes[^1] ^= 1;
            File.WriteAllBytes(path, bytes);
        }
        else if (damage == "rehashed-hard-state")
        {
            var offset = 52 + (int)BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(8));
            var validButChanged = new DataGridWalRecord(
                DataGridWalRecordKind.HardState,
                2,
                DataGridWalRecovery.HardState(
                    config,
                    new(config, 4, 2, Image(config, 2).Entries, 2)
                )
            ).Encode();
            validButChanged.CopyTo(bytes, offset);
            File.WriteAllBytes(path, bytes);
        }
        else
        {
            var digest = anchor.GetWalDigest();
            if (damage == "digest")
                digest[0] ^= 1;
            File.WriteAllBytes(
                anchorPath,
                new DataGridCurrentAnchor(
                    anchor.IncarnationId,
                    anchor.MemberId,
                    anchor.GenerationId,
                    anchor.PublishedWalBytes!.Value,
                    anchor.PublishedRecordCount!.Value + (damage == "count" ? 1 : 0),
                    digest
                ).Encode()
            );
        }
        var preserved = DataGridDiskFullTests.Files(directory.Path);
        Assert.Throws<InvalidDataException>(() =>
            DataGridFileStore.Open(directory.Path, config, 0)
        );
        Assert.Equal(preserved, DataGridDiskFullTests.Files(directory.Path));
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LegacyV1ValidatesWholeWalAndUpgradesOnlyThroughFreshPublication(bool torn)
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using (var initial = DataGridFileStore.Bootstrap(directory.Path, config, 0))
            initial.Persist(Image(config, 2), TestContext.Current.CancellationToken);
        var path = DataGridDiskFullTests.Selected(directory.Path);
        var anchorPath = Path.Combine(directory.Path, "CURRENT");
        var selected = DataGridCurrentAnchor.ReadFrom(File.ReadAllBytes(anchorPath));
        File.WriteAllBytes(
            anchorPath,
            new DataGridCurrentAnchor(
                selected.IncarnationId,
                selected.MemberId,
                selected.GenerationId
            ).Encode()
        );
        if (torn)
        {
            using (var file = new FileStream(path, FileMode.Append))
                file.WriteByte(255);
            var preserved = DataGridDiskFullTests.Files(directory.Path);
            Assert.Throws<InvalidDataException>(() =>
                DataGridFileStore.Open(directory.Path, config, 0)
            );
            Assert.Equal(preserved, DataGridDiskFullTests.Files(directory.Path));
            return;
        }
        var before = DataGridDiskFullTests.Files(directory.Path);
        using var recovered = DataGridFileStore.Open(directory.Path, config, 0);
        Assert.Equal(before, DataGridDiskFullTests.Files(directory.Path));
        Assert.Equal(3, recovered.Image.Term);
        Assert.Equal(2, recovered.Image.Vote);
        recovered.Persist(
            Replacement(recovered.Image, config),
            TestContext.Current.CancellationToken
        );
        Assert.NotEqual(selected.GenerationId, recovered.GenerationId);
        Assert.Equal(
            2,
            DataGridCurrentAnchor.ReadFrom(File.ReadAllBytes(anchorPath)).FormatVersion
        );
    }

    [Fact]
    public void CollectorRejectsSameGenerationFrontierRollbackBeforeDeletingFiles()
    {
        using var directory = new OwnedDirectory();
        var config = Configuration();
        using var store = DataGridFileStore.Bootstrap(directory.Path, config, 0);
        var anchorPath = Path.Combine(directory.Path, "CURRENT");
        var original = File.ReadAllBytes(anchorPath);
        store.Persist(Image(config, 2), TestContext.Current.CancellationToken);
        File.WriteAllBytes(anchorPath, original);
        var preserved = DataGridDiskFullTests.Files(directory.Path);
        Assert.Throws<InvalidDataException>(() =>
            store.CollectObsoleteFiles(TestContext.Current.CancellationToken)
        );
        Assert.Equal(preserved, DataGridDiskFullTests.Files(directory.Path));
    }

    private static RaftDurableImage Replacement(
        RaftDurableImage image,
        DataGridBootstrapConfiguration config
    ) => new(config, 4, 1, image.Entries.Take(2).Append(Entry(3, 4, Mutation(2, [9]))), 3);
}
