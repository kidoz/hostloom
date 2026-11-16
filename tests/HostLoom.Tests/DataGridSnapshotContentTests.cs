using System.Security.Cryptography;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridSnapshotContentTests
{
    [Theory]
    [InlineData(1)]
    [InlineData(65535)]
    [InlineData(65536)]
    [InlineData(65537)]
    [InlineData(131085)]
    public void ArbitraryWritesKeepExactBytesAndHashAcrossBoundaries(int length)
    {
        var expected = Enumerable.Range(0, length).Select(i => (byte)(i * 31)).ToArray();
        using var builder = new DataGridSnapshotContent.Builder(length);
        Assert.Throws<InvalidDataException>(() => builder.Complete());
        for (var offset = 0; offset < length; )
        {
            var count = Math.Min(777, length - offset);
            builder.Write(expected.AsSpan(offset, count));
            offset += count;
        }
        Assert.Throws<InvalidDataException>(() => builder.Write([1]));
        var content = builder.Complete();
        builder.Dispose();
        builder.Dispose();
        Assert.Throws<InvalidOperationException>(() => builder.Write([1]));
        Assert.Throws<InvalidOperationException>(() => builder.Complete());
        Assert.Equal(expected, content.ToArray());
        Assert.Equal(SHA256.HashData(expected), content.ComputeHash());
        var copy = content.ToArray();
        copy[0] ^= 1;
        Assert.Equal(expected, content.ToArray());
        Assert.Equal((length - 1) / 65536 + 1, content.BlockCount);
        for (var offset = 0; offset < length; )
        {
            var block = content.BlockAt(offset);
            Assert.InRange(block.Length, 1, 65536);
            Assert.True(
                System.Runtime.InteropServices.MemoryMarshal.TryGetArray(block, out var storage)
            );
            Assert.InRange(storage.Array!.Length, 1, 65536);
            offset += block.Length;
        }
        if (length > 65536)
        {
            Assert.True(
                content.Matches(65530, expected.AsSpan(65530, Math.Min(20, length - 65530)))
            );
            Assert.Equal(
                expected.AsSpan(65530, Math.Min(20, length - 65530)).ToArray(),
                content.ReadValue(65530, Math.Min(20, length - 65530), true).ToArray()
            );
        }
    }

    [Fact]
    public void BuilderAndLogicalReadersRejectInvalidRangesWithoutExposingPartialContent()
    {
        Assert.Throws<InvalidDataException>(() => new DataGridSnapshotContent.Builder(0));
        Assert.Throws<InvalidDataException>(() => new DataGridSnapshotContent.Builder(-1));
        Assert.Throws<InvalidDataException>(() =>
            new DataGridSnapshotContent.Builder(
                checked((int)DataGridSnapshotDescriptor.MaxSnapshotByteCount + 1)
            )
        );
        using (
            var largest = new DataGridSnapshotContent.Builder(
                (int)DataGridSnapshotDescriptor.MaxSnapshotByteCount
            )
        )
        {
            Assert.Equal(0, largest.Length); // Only bounded index metadata exists before data arrives.
            Assert.Throws<InvalidDataException>(() => largest.Complete());
        }
        var stopped = new DataGridSnapshotContent.Builder(4);
        stopped.Write([1, 2]);
        Assert.Throws<InvalidDataException>(() => stopped.Write([3, 4, 5]));
        Assert.Equal(2, stopped.Length);
        stopped.Dispose();
        Assert.Throws<ObjectDisposedException>(() => stopped.Complete());
        var content = DataGridSnapshotContent.CopyFrom(new byte[131085]);
        var before = GC.GetAllocatedBytesForCurrentThread();
        var reader = new GridReader(content);
        var body = reader.Subreader(content.Length);
        reader.End();
        var section = body.Subreader(content.Length);
        body.End();
        Assert.Equal(before, GC.GetAllocatedBytesForCurrentThread());
        Assert.Equal(new byte[8], section.Take(8).ToArray());
        Assert.Throws<InvalidDataException>(() => content.ReadValue(0, 65537, false));
        Assert.Throws<InvalidDataException>(() => content.ReadField(0, 65537).ToArray());
        Assert.Throws<InvalidDataException>(() => content.Matches(content.Length, [1]));
        Assert.Throws<InvalidDataException>(() => content.CopyTo(-1, new byte[1]));
    }

    [Fact]
    public void PrimitiveAndCanonicalComparisonCrossBlocksWithoutSkippingBytes()
    {
        var id = Guid.NewGuid();
        for (var remaining = 1; remaining < 28; remaining++)
        {
            var prefix = new byte[65536 - remaining];
            using var writer = GridWriter.Segmented(prefix.Length + 16 + 8 + 3);
            writer.Raw(prefix);
            writer.Id(id);
            writer.Number(0xfedcba9876543210, 8);
            writer.Raw([1, 2, 3]);
            var content = writer.DetachSnapshotContent();
            var reader = new GridReader(content);
            var skipped = reader.Subreader(prefix.Length);
            var skippedTail = skipped.Subreader(prefix.Length);
            skipped.End();
            Assert.Equal(id, reader.Id());
            Assert.Equal(0xfedcba9876543210, reader.Number(8));
            Assert.Equal(new byte[] { 1, 2, 3 }, reader.Take(3).ToArray());
            reader.End();
            using var compare = GridWriter.Comparing(content);
            compare.Raw(prefix);
            compare.Id(id);
            compare.Number(0xfedcba9876543210, 8);
            Assert.Throws<InvalidDataException>(() => compare.Raw([1, 2, 4]));
            compare.Raw([1, 2, 3]);
            compare.CompleteComparison();
            _ = skippedTail.Subreader(prefix.Length);
            skippedTail.End();
        }
    }

    [Fact]
    public void MaximumValueCrossesBlocksWithOrdinaryAndBorrowedRestoreAndPublicCopyIsolation()
    {
        using var fixture = new DataGridPeerFixture(2, 200000);
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
        var value = Enumerable
            .Range(0, DataGridMutationItem.MaxValueByteCount)
            .Select(i => (byte)(i * 17))
            .ToArray();
        machine.Apply(2, DataGridFileStorageTests.Mutation(1, value));
        var snapshot = machine.SaveSnapshot(Guid.NewGuid(), 1);
        Assert.True(snapshot.Content.BlockCount > 1);
        snapshot.Validate(fixture.Configuration);
        foreach (
            var restored in new[]
            {
                DataGridLocalStateMachine.RestoreSnapshot(fixture.Configuration, snapshot),
                DataGridLocalStateMachine.RestoreForVerification(fixture.Configuration, snapshot),
            }
        )
        {
            restored.VerifyCanonicalSnapshot(snapshot);
            Assert.Equal(
                value,
                restored.Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key)!.GetValue()
            );
            var replay = restored.Apply(3, DataGridFileStorageTests.Mutation(1, value));
            Assert.Equal(
                new DataGridEntryVersion(fixture.Configuration.IncarnationId, 2),
                replay.GetItem(0).Version
            );
            restored.Apply(4, DataGridFileStorageTests.Mutation(2, [9]));
        }
        var bytes = snapshot.GetContent();
        bytes[^1] ^= 1;
        Assert.Equal(
            value,
            DataGridLocalStateMachine
                .RestoreSnapshot(fixture.Configuration, snapshot)
                .Read(DataGridPeerFixture.Map, DataGridPeerFixture.Key)!
                .GetValue()
        );
        var corrupted = new DataGridLocalSnapshot(snapshot.Descriptor, bytes);
        Assert.Throws<InvalidDataException>(() => corrupted.Validate(fixture.Configuration));
    }
}
