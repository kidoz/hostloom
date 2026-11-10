using System.Buffers.Binary;
using System.Security.Cryptography;
using HostLoom.DataGrid;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridStorageFormatTests
{
    private static readonly Guid SnapshotId = new("00112233-4455-6677-8899-aabbccddeeff");

    [Fact]
    public void WalEncodingMatchesIndependentSha256Fixture()
    {
        var frame = new DataGridWalRecord(DataGridWalRecordKind.LogAppend, 1, "abc"u8).Encode();
        // Fixture computed independently with Python hashlib, not the codec under test.
        Assert.Equal(
            "484C475700010003000000030000000000000001616263D734FF2E220D31A86184981E1389AC5A17AEE7CC35D147C692269222F848D394",
            Convert.ToHexString(frame)
        );
        var read = DataGridWalRecord.ReadFrom(frame, 1);
        Assert.Equal(DataGridWalRecordKind.LogAppend, read.Kind);
        Assert.Equal(1, read.Number);
        Assert.Equal("abc"u8.ToArray(), read.GetPayload());
    }

    [Fact]
    public void EverySingleByteMutationAndTruncatedPrefixIsRejected()
    {
        var frame = new DataGridWalRecord(
            DataGridWalRecordKind.HardState,
            12,
            new byte[24]
        ).Encode();
        for (var i = 0; i < frame.Length; i++)
        {
            var changed = (byte[])frame.Clone();
            changed[i] ^= 1;
            Assert.Throws<InvalidDataException>(() => DataGridWalRecord.ReadFrom(changed, 12));
            var prefix = frame[..i];
            Assert.Throws<InvalidDataException>(() => DataGridWalRecord.ReadFrom(prefix, 12));
        }
    }

    [Fact]
    public void SequenceGapsDuplicatesReorderingAndTrailingBytesAreRejected()
    {
        var first = new DataGridWalRecord(DataGridWalRecordKind.LogAppend, 1, []).Encode();
        var second = new DataGridWalRecord(DataGridWalRecordKind.LogAppend, 2, []).Encode();
        Assert.Throws<InvalidDataException>(() => DataGridWalRecord.ReadFrom(first, 2));
        Assert.Throws<InvalidDataException>(() => DataGridWalRecord.ReadFrom(second, 1));
        Assert.Throws<InvalidDataException>(() =>
            DataGridWalRecord.ReadFrom([.. first, .. second], 1)
        );
        Assert.Throws<InvalidDataException>(() => DataGridWalRecord.ReadFrom([.. first, 0], 1));
        Assert.Throws<ArgumentOutOfRangeException>(() => DataGridWalRecord.ReadFrom(first, 0));
        Assert.Equal(
            long.MaxValue,
            DataGridWalRecord
                .ReadFrom(
                    new DataGridWalRecord(
                        DataGridWalRecordKind.HardState,
                        long.MaxValue,
                        []
                    ).Encode(),
                    long.MaxValue
                )
                .Number
        );
    }

    [Theory]
    [InlineData(4)]
    [InlineData(6)]
    [InlineData(8)]
    [InlineData(12)]
    public void ValidChecksumDoesNotBypassVersionKindLengthOrSequenceValidation(int offset)
    {
        var frame = new DataGridWalRecord(DataGridWalRecordKind.Identity, 1, []).Encode();
        frame[offset] = 0xff;
        Rehash(frame);
        Assert.Throws<InvalidDataException>(() => DataGridWalRecord.ReadFrom(frame, 1));
    }

    [Fact]
    public void FramePayloadIsOwnedAcrossConstructionEncodingAndDecoding()
    {
        byte[] input = [1, 2, 3];
        var record = new DataGridWalRecord(DataGridWalRecordKind.LogTruncate, 1, input);
        input[0] = 99;
        var frame = record.Encode();
        var restored = DataGridWalRecord.ReadFrom(frame, 1);
        frame[20] = 99;
        var exported = restored.GetPayload();
        exported[0] = 99;
        Assert.Equal(new byte[] { 1, 2, 3 }, restored.GetPayload());
        Assert.Equal(new byte[] { 1, 2, 3 }, record.GetPayload());
    }

    [Fact]
    public void FrameSizeLimitIncludesHeaderAndChecksum()
    {
        var payload = new byte[DataGridWalRecord.MaxPayloadByteCount];
        var frame = new DataGridWalRecord(DataGridWalRecordKind.LogAppend, 1, payload).Encode();
        Assert.Equal(DataGridWalRecord.MaxEncodedByteCount, frame.Length);
        Assert.Equal(payload.Length, DataGridWalRecord.ReadFrom(frame, 1).PayloadByteCount);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridWalRecord(DataGridWalRecordKind.LogAppend, 1, new byte[payload.Length + 1])
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridWalRecord.ReadFrom(new byte[frame.Length + 1], 1)
        );
        var small = new DataGridWalRecord(DataGridWalRecordKind.LogAppend, 1, []).Encode();
        BinaryPrimitives.WriteUInt32BigEndian(small.AsSpan(8), uint.MaxValue);
        Assert.Throws<InvalidDataException>(() => DataGridWalRecord.ReadFrom(small, 1));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridWalRecord(DataGridWalRecordKind.None, 1, [])
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridWalRecord((DataGridWalRecordKind)7, 1, [])
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridWalRecord(DataGridWalRecordKind.Identity, 0, [])
        );
    }

    [Fact]
    public void SnapshotDescriptorHasStableEncodingAndOwnedDigest()
    {
        var digest = Enumerable.Range(0, 32).Select(value => (byte)value).ToArray();
        var snapshot = new DataGridSnapshotDescriptor(SnapshotId, 10, 2, 256, digest);
        digest[0] = 99;
        var encoded = snapshot.Encode();
        Assert.Equal(
            "00112233445566778899AABBCCDDEEFF000000000000000A00000000000000020000000000000100000102030405060708090A0B0C0D0E0F101112131415161718191A1B1C1D1E1F",
            Convert.ToHexString(encoded)
        );
        var restored = DataGridSnapshotDescriptor.ReadFrom(encoded);
        encoded[40] = 99;
        var exported = restored.GetDigest();
        exported[0] = 99;
        Assert.Equal(SnapshotId, restored.SnapshotId);
        Assert.Equal(10, restored.LastIncludedIndex);
        Assert.Equal(2, restored.LastIncludedTerm);
        Assert.Equal(256, restored.ByteCount);
        Assert.Equal(0, restored.GetDigest()[0]);
    }

    [Fact]
    public void SnapshotManifestUsesTheWalChecksumForCorruptionDetection()
    {
        var descriptor = new DataGridSnapshotDescriptor(SnapshotId, 10, 2, 256, new byte[32]);
        var wal = new DataGridWalRecord(
            DataGridWalRecordKind.SnapshotManifest,
            20,
            descriptor.Encode()
        ).Encode();
        var restored = DataGridSnapshotDescriptor.ReadFrom(
            DataGridWalRecord.ReadFrom(wal, 20).GetPayload()
        );
        Assert.Equal(10, restored.LastIncludedIndex);
        wal[DataGridWalRecord.HeaderByteCount + 40] ^= 1;
        Assert.Throws<InvalidDataException>(() => DataGridWalRecord.ReadFrom(wal, 20));
    }

    [Theory]
    [InlineData(0)]
    [InlineData(16)]
    [InlineData(24)]
    [InlineData(32)]
    public void SnapshotDescriptorRejectsInvalidFields(int offset)
    {
        var encoded = new DataGridSnapshotDescriptor(SnapshotId, 10, 2, 256, new byte[32]).Encode();
        encoded.AsSpan(offset, offset == 0 ? 16 : 8).Clear();
        Assert.Throws<InvalidDataException>(() => DataGridSnapshotDescriptor.ReadFrom(encoded));
    }

    [Fact]
    public void SnapshotLimitsAndExactPayloadLengthAreEnforced()
    {
        var maximum = new DataGridSnapshotDescriptor(
            SnapshotId,
            long.MaxValue,
            long.MaxValue,
            DataGridSnapshotDescriptor.MaxSnapshotByteCount,
            new byte[32]
        );
        Assert.Equal(
            DataGridSnapshotDescriptor.MaxSnapshotByteCount,
            DataGridSnapshotDescriptor.ReadFrom(maximum.Encode()).ByteCount
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridSnapshotDescriptor(
                SnapshotId,
                1,
                1,
                DataGridSnapshotDescriptor.MaxSnapshotByteCount + 1,
                new byte[32]
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridSnapshotDescriptor(SnapshotId, 1, 1, 1, new byte[31])
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridSnapshotDescriptor.ReadFrom(new byte[71])
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridSnapshotDescriptor.ReadFrom(new byte[73])
        );
    }

    private static void Rehash(byte[] frame) =>
        SHA256.HashData(frame.AsSpan(0, frame.Length - 32), frame.AsSpan(frame.Length - 32));
}
