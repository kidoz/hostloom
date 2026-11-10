using System.Buffers.Binary;
using HostLoom.DataGrid;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridProtocolTests
{
    private static readonly Guid Map = new("00112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Cluster = new("10112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Sender = new("20112233-4455-6677-8899-aabbccddeeff");
    private static readonly Guid Attempt = new("30112233-4455-6677-8899-aabbccddeeff");

    [Theory]
    [InlineData(DataGridMutationKind.Put)]
    [InlineData(DataGridMutationKind.PutIfAbsent)]
    [InlineData(DataGridMutationKind.CompareExchange)]
    [InlineData(DataGridMutationKind.Remove)]
    [InlineData(DataGridMutationKind.CompareRemove)]
    public void MutationRoundTripsCanonicalBytes(DataGridMutationKind kind)
    {
        var item = new DataGridMutationItem(
            DataGridKey.FromUtf8("catalog:eu"),
            (kind is DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove)
                ? []
                : "books"u8,
            (kind is DataGridMutationKind.CompareExchange or DataGridMutationKind.CompareRemove)
                ? new(Cluster, 10)
                : default
        );
        var command = new DataGridMutationCommand(
            kind,
            Map,
            1,
            2,
            DataGridExpiryKind.Never,
            0,
            [item]
        );
        var encoded = command.Encode();
        var restored = DataGridMutationCommand.ReadFrom(encoded);
        Assert.Equal(encoded, restored.Encode());
        Assert.Equal(command.ComputeDigest(), restored.ComputeDigest());
        Assert.Equal(item.ExpectedVersion, restored.GetItem(0).ExpectedVersion);
        Assert.Equal(item.Key, restored.GetItem(0).Key);
        Assert.Equal(item.GetValue(), restored.GetItem(0).GetValue());
    }

    [Fact]
    public void CanonicalFixtureAndDigestMatchIndependentEncoding()
    {
        var command = Put();
        Assert.Equal(
            "484C474F0001000100112233445566778899AABBCCDDEEFF0000000000000001000000000000000200000001000000000000000000016B0000000176",
            Convert.ToHexString(command.Encode())
        );
        // SHA-256 fixture computed independently with Python struct/uuid/hashlib.
        Assert.Equal(
            "8823A3B86D8FA6F734E9BBCB01CE298222B6D298BCEE1044B570A1B23B8A12B2",
            Convert.ToHexString(command.ComputeDigest())
        );
        var frame = Frame(command.Encode());
        Assert.Equal(
            "000000800001000310112233445566778899AABBCCDDEEFF20112233445566778899AABBCCDDEEFF0000000000000001000000000000000230112233445566778899AABBCCDDEEFF",
            Convert.ToHexString(frame.Encode().AsSpan(0, 72))
        );
    }

    [Fact]
    public void RetriesChangeOnlyAttemptEnvelopeAndRetainLogicalDigest()
    {
        var command = Put();
        var first = Frame(command.Encode());
        var second = new DataGridProtocolFrame(
            DataGridMessageKind.Mutation,
            Cluster,
            Sender,
            1,
            2,
            new Guid("40112233-4455-6677-8899-aabbccddeeff"),
            command.Encode()
        );
        Assert.NotEqual(first.Encode(), second.Encode());
        Assert.Equal(
            DataGridMutationCommand.ReadFrom(first).ComputeDigest(),
            DataGridMutationCommand.ReadFrom(second).ComputeDigest()
        );
        var changed = new DataGridMutationCommand(
            DataGridMutationKind.Put,
            Map,
            1,
            3,
            DataGridExpiryKind.Never,
            0,
            [new(DataGridKey.FromUtf8("k"), "v"u8)]
        );
        Assert.NotEqual(command.ComputeDigest(), changed.ComputeDigest());
    }

    [Fact]
    public void InputsCollectionsExportsAndDecodedBuffersAreOwned()
    {
        byte[] value = [1, 2];
        var item = new DataGridMutationItem(DataGridKey.FromUtf8("k"), value);
        DataGridMutationItem[] items = [item];
        var command = new DataGridMutationCommand(
            DataGridMutationKind.Put,
            Map,
            1,
            2,
            DataGridExpiryKind.Never,
            0,
            items
        );
        items[0] = new(DataGridKey.FromUtf8("changed"), []);
        value[0] = 99;
        item.GetValue()[0] = 99;
        var body = command.Encode();
        var frame = Frame(body);
        var restored = DataGridMutationCommand.ReadFrom(frame);
        body[^1] = 99;
        frame.GetPayload()[^1] = 99;
        var encodedFrame = frame.Encode();
        var decodedFrame = DataGridProtocolFrame.ReadFrom(encodedFrame, Cluster, Sender);
        encodedFrame[^1] = 99;
        Assert.Equal(new byte[] { 1, 2 }, restored.GetItem(0).GetValue());
        Assert.Equal(command.Encode(), decodedFrame.GetPayload());
    }

    [Fact]
    public void DuplicateKeysAndInvalidOperationSpecificFieldsAreRejected()
    {
        var item = new DataGridMutationItem(DataGridKey.FromUtf8("k"), "v"u8);
        Assert.Throws<ArgumentException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.Put,
                Map,
                1,
                1,
                DataGridExpiryKind.Never,
                0,
                [item, item]
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.CompareExchange,
                Map,
                1,
                1,
                DataGridExpiryKind.Never,
                0,
                [item]
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.Remove,
                Map,
                1,
                1,
                DataGridExpiryKind.Never,
                0,
                [item]
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.Put,
                Map,
                1,
                1,
                DataGridExpiryKind.Never,
                0,
                [new(item.Key, [], new(Cluster, 1))]
            )
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridMutationItem(item.Key, new byte[65537])
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.Put,
                Map,
                1,
                1,
                DataGridExpiryKind.Never,
                0,
                []
            )
        );
    }

    [Theory]
    [InlineData(DataGridExpiryKind.Never, 1)]
    [InlineData(DataGridExpiryKind.Relative, 0)]
    [InlineData(DataGridExpiryKind.Relative, -1)]
    [InlineData(DataGridExpiryKind.Relative, long.MaxValue)]
    [InlineData(DataGridExpiryKind.Absolute, -1)]
    [InlineData(DataGridExpiryKind.Absolute, long.MaxValue)]
    [InlineData((DataGridExpiryKind)3, 0)]
    public void InvalidExpiryPolicyIsRejected(DataGridExpiryKind expiry, long ticks) =>
        Assert.Throws<ArgumentException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.Put,
                Map,
                1,
                1,
                expiry,
                ticks,
                [new(DataGridKey.FromUtf8("k"), [])]
            )
        );

    [Fact]
    public void TickPoliciesAndEmptyValuesRemainDistinctFromRemove()
    {
        foreach (
            var (kind, ticks) in new[]
            {
                (DataGridExpiryKind.Never, 0L),
                (DataGridExpiryKind.Relative, 1L),
                (DataGridExpiryKind.Absolute, 0L),
            }
        )
        {
            var command = new DataGridMutationCommand(
                DataGridMutationKind.Put,
                Map,
                1,
                1,
                kind,
                ticks,
                [new(DataGridKey.FromUtf8("k"), [])]
            );
            var restored = DataGridMutationCommand.ReadFrom(command.Encode());
            Assert.Equal(kind, restored.ExpiryKind);
            Assert.Equal(ticks, restored.ExpiryTicks);
            Assert.Equal(DataGridMutationKind.Put, restored.Kind);
        }
    }

    [Fact]
    public void EveryTruncatedCommandAndTrailingBytesAreRejected()
    {
        var bytes = Put().Encode();
        for (var length = 0; length < bytes.Length; length++)
        {
            var prefix = bytes[..length];
            Assert.Throws<InvalidDataException>(() => DataGridMutationCommand.ReadFrom(prefix));
        }
        Assert.Throws<InvalidDataException>(() => DataGridMutationCommand.ReadFrom([.. bytes, 0]));
        foreach (var offset in new[] { 0, 4, 6, 40, 41, 42, 52, 55 })
        {
            var changed = (byte[])bytes.Clone();
            changed[offset] = 0xff;
            Assert.Throws<InvalidDataException>(() => DataGridMutationCommand.ReadFrom(changed));
        }
    }

    [Fact]
    public void BatchAndEnvelopeLimitsRejectBeforeUnboundedAllocation()
    {
        var items = Enumerable
            .Range(0, 128)
            .Select(i => new DataGridMutationItem(DataGridKey.FromUtf8($"catalog:{i}"), []))
            .ToArray();
        Assert.Equal(
            128,
            DataGridMutationCommand
                .ReadFrom(
                    new DataGridMutationCommand(
                        DataGridMutationKind.Put,
                        Map,
                        1,
                        1,
                        DataGridExpiryKind.Never,
                        0,
                        items
                    ).Encode()
                )
                .ItemCount
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.Put,
                Map,
                1,
                1,
                DataGridExpiryKind.Never,
                0,
                [.. items, new(DataGridKey.FromUtf8("extra"), [])]
            )
        );
        var large = Enumerable
            .Range(0, 16)
            .Select(i => new DataGridMutationItem(
                DataGridKey.FromUtf8($"catalog:{i}"),
                new byte[65536]
            ))
            .ToArray();
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.Put,
                Map,
                1,
                1,
                DataGridExpiryKind.Never,
                0,
                large
            )
        );
        var maximum = Frame(new byte[DataGridProtocolFrame.MaxPayloadByteCount]);
        Assert.Equal(1048576, maximum.Encode().Length);
        Assert.Equal(
            DataGridProtocolFrame.MaxPayloadByteCount,
            DataGridProtocolFrame.ReadFrom(maximum.Encode(), Cluster, Sender).GetPayload().Length
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            Frame(new byte[DataGridProtocolFrame.MaxPayloadByteCount + 1])
        );
        var bytes = Put().Encode();
        BinaryPrimitives.WriteUInt32BigEndian(bytes.AsSpan(55), uint.MaxValue);
        Assert.Throws<InvalidDataException>(() => DataGridMutationCommand.ReadFrom(bytes));
    }

    [Fact]
    public void EnvelopeRejectsForeignIdentityUnknownKindBadVersionAndLength()
    {
        var bytes = Frame(Put().Encode()).Encode();
        Assert.Throws<InvalidDataException>(() =>
            DataGridProtocolFrame.ReadFrom(bytes, Map, Sender)
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridProtocolFrame.ReadFrom(bytes, Cluster, Map)
        );
        foreach (var offset in new[] { 0, 4, 6 })
        {
            var changed = (byte[])bytes.Clone();
            changed[offset] = 0xff;
            Assert.Throws<InvalidDataException>(() =>
                DataGridProtocolFrame.ReadFrom(changed, Cluster, Sender)
            );
        }
        for (var length = 0; length < bytes.Length; length++)
        {
            var prefix = bytes[..length];
            Assert.Throws<InvalidDataException>(() =>
                DataGridProtocolFrame.ReadFrom(prefix, Cluster, Sender)
            );
        }
        Assert.Throws<InvalidDataException>(() =>
            DataGridProtocolFrame.ReadFrom([.. bytes, 0], Cluster, Sender)
        );
    }

    [Fact]
    public void EnvelopeAndCommandIdentitiesMustAgree()
    {
        var mismatched = new DataGridProtocolFrame(
            DataGridMessageKind.Mutation,
            Cluster,
            Sender,
            2,
            2,
            Attempt,
            Put().Encode()
        );
        Assert.Throws<InvalidDataException>(() => DataGridMutationCommand.ReadFrom(mismatched));
        var foreign = new DataGridMutationCommand(
            DataGridMutationKind.CompareExchange,
            Map,
            1,
            2,
            DataGridExpiryKind.Never,
            0,
            [new(DataGridKey.FromUtf8("k"), [], new(Map, 1))]
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridMutationCommand.ReadFrom(Frame(foreign.Encode()))
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridProtocolFrame(
                DataGridMessageKind.Mutation,
                Cluster,
                Sender,
                0,
                0,
                Attempt,
                []
            )
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridProtocolFrame(
                DataGridMessageKind.VoteRequest,
                Cluster,
                Sender,
                1,
                1,
                Attempt,
                []
            )
        );
        var peer = new DataGridProtocolFrame(
            DataGridMessageKind.VoteRequest,
            Cluster,
            Sender,
            0,
            0,
            Attempt,
            []
        );
        Assert.Throws<InvalidDataException>(() => DataGridMutationCommand.ReadFrom(peer));
    }

    [Fact]
    public void LargestMutationStillFitsOneReplicatedAppendFrame()
    {
        var items = new DataGridMutationItem[16];
        var remaining = DataGridMutationCommand.MaxEncodedByteCount - 52 - (16 * 7);
        for (var i = 0; i < items.Length; i++)
        {
            var size = Math.Min(65536, remaining);
            items[i] = new(new DataGridKey([(byte)i]), new byte[size]);
            remaining -= size;
        }
        var command = new DataGridMutationCommand(
            DataGridMutationKind.Put,
            Map,
            1,
            1,
            DataGridExpiryKind.Never,
            0,
            items
        );
        Assert.Equal(DataGridMutationCommand.MaxEncodedByteCount, command.Encode().Length);
        Assert.Equal(1048576, 72 + 34 + 20 + 36 + command.Encode().Length);
        Assert.Equal(command.Encode(), DataGridMutationCommand.ReadFrom(command.Encode()).Encode());
        items[^1] = new(items[^1].Key, new byte[items[^1].ValueByteCount + 1]);
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.Put,
                Map,
                1,
                1,
                DataGridExpiryKind.Never,
                0,
                items
            )
        );
    }

    [Fact]
    public void CasVersionItemIndexMustBelongToTheBoundedBatchDomain()
    {
        var key = DataGridKey.FromUtf8("k");
        Assert.Throws<ArgumentException>(() =>
            new DataGridMutationCommand(
                DataGridMutationKind.CompareExchange,
                Map,
                1,
                1,
                DataGridExpiryKind.Never,
                0,
                [new(key, [], new(Cluster, 1, 128))]
            )
        );
        var valid = new DataGridMutationCommand(
            DataGridMutationKind.CompareExchange,
            Map,
            1,
            1,
            DataGridExpiryKind.Never,
            0,
            [new(key, [], new(Cluster, 1, 127))]
        ).Encode();
        Assert.Equal(
            127,
            DataGridMutationCommand.ReadFrom(valid).GetItem(0).ExpectedVersion.ItemIndex
        );
        BinaryPrimitives.WriteInt32BigEndian(valid.AsSpan(52 + 2 + 1 + 24), 128);
        Assert.Throws<InvalidDataException>(() => DataGridMutationCommand.ReadFrom(valid));
    }

    private static DataGridMutationCommand Put() =>
        new(
            DataGridMutationKind.Put,
            Map,
            1,
            2,
            DataGridExpiryKind.Never,
            0,
            [new(DataGridKey.FromUtf8("k"), "v"u8)]
        );

    private static DataGridProtocolFrame Frame(ReadOnlySpan<byte> body) =>
        new(DataGridMessageKind.Mutation, Cluster, Sender, 1, 2, Attempt, body);
}
