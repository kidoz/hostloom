using System.Buffers.Binary;
using System.Security.Cryptography;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridPeerCodecTests
{
    [Fact]
    public void VoteAndAppendBytesMatchIndependentFixtures()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var request = fixture.Message(RaftMessageKind.VoteRequest, term: 3, index: 9, previous: 2);
        var frame = DataGridPeerCodec.Encode(request, config);
        Assert.Equal(
            Convert.FromHexString("000000000000000300000000000000090000000000000002"),
            frame.GetPayload()
        );
        var vote = DataGridPeerCodec.Decode(frame, config, 0, 1, false);
        Assert.Equal(request.Token, vote.Token);
        Assert.Equal(9, vote.Index);
        Assert.Equal(2, vote.PreviousTerm);
        var entry = new RaftEntry(
            1,
            3,
            DataGridCommittedCommandCodec.Encode(DataGridCommittedCommand.AdvanceTime(new(5)))
        );
        var append = fixture.Message(RaftMessageKind.AppendRequest, term: 3, entries: [entry]);
        var bytes = DataGridPeerCodec.Encode(append, config).GetPayload();
        Assert.Equal(3, BinaryPrimitives.ReadInt64BigEndian(bytes));
        Assert.Equal(1, BinaryPrimitives.ReadUInt16BigEndian(bytes.AsSpan(32)));
        Assert.Equal(1, BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan(34)));
        Assert.Equal(3, BinaryPrimitives.ReadInt64BigEndian(bytes.AsSpan(42)));
        Assert.Equal((uint)entry.ByteCount, BinaryPrimitives.ReadUInt32BigEndian(bytes.AsSpan(50)));
        Assert.Equal(entry.GetCommand(), bytes[54..]);
        Assert.True(
            entry.SameAs(
                Assert.Single(
                    DataGridPeerCodec
                        .Decode(DataGridPeerCodec.Encode(append, config), config, 0, 1, false)
                        .Entries
                )
            )
        );
    }

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void ResponseBytesAndDirectionsAreStrict(bool accepted)
    {
        using var fixture = new DataGridPeerFixture();
        var response = fixture.Message(
            RaftMessageKind.AppendResponse,
            term: 3,
            index: accepted ? 9 : 0,
            accepted: accepted,
            conflict: accepted ? 0 : 4
        );
        var frame = DataGridPeerCodec.Encode(response, fixture.Configuration);
        Assert.Equal(33, frame.GetPayload().Length);
        var decoded = DataGridPeerCodec.Decode(frame, fixture.Configuration, 0, 1, true);
        Assert.Equal(accepted, decoded.Accepted);
        Assert.Equal(response.Index, decoded.Index);
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerCodec.Decode(frame, fixture.Configuration, 0, 1, false)
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerCodec.Decode(frame, fixture.Configuration, 1, 0, true)
        );
    }

    [Fact]
    public void TruncationsTrailingFieldsCountsTermsAndBooleanRejectBeforeDispatch()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var frame = DataGridPeerCodec.Encode(
            fixture.Message(RaftMessageKind.VoteRequest, term: 3, index: 2, previous: 1),
            config
        );
        var payload = frame.GetPayload();
        for (var i = 0; i < payload.Length; i++)
        {
            var truncated = DataGridPeerCodec.Frame(
                config,
                0,
                frame.Kind,
                frame.AttemptId,
                payload.AsSpan(0, i)
            );
            Assert.Throws<InvalidDataException>(() =>
                DataGridPeerCodec.Decode(truncated, config, 0, 1, false)
            );
        }
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerCodec.Decode(
                DataGridPeerCodec.Frame(config, 0, frame.Kind, frame.AttemptId, [.. payload, 0]),
                config,
                0,
                1,
                false
            )
        );
        var append = DataGridPeerCodec.Encode(
            fixture.Message(RaftMessageKind.AppendRequest, term: 1),
            config
        );
        var oversized = append.GetPayload();
        BinaryPrimitives.WriteUInt16BigEndian(oversized.AsSpan(32), 129);
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerCodec.Decode(
                DataGridPeerCodec.Frame(config, 0, append.Kind, append.AttemptId, oversized),
                config,
                0,
                1,
                false
            )
        );
        var bad = frame.GetPayload();
        BinaryPrimitives.WriteInt64BigEndian(bad.AsSpan(16), 4);
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerCodec.Decode(
                DataGridPeerCodec.Frame(config, 0, frame.Kind, frame.AttemptId, bad),
                config,
                0,
                1,
                false
            )
        );
        var boolean = DataGridPeerCodec
            .Encode(fixture.Message(RaftMessageKind.VoteResponse, term: 1, accepted: true), config)
            .GetPayload();
        boolean[8] = 2;
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerCodec.Decode(
                DataGridPeerCodec.Frame(
                    config,
                    0,
                    DataGridMessageKind.VoteResponse,
                    frame.AttemptId,
                    boolean
                ),
                config,
                0,
                1,
                true
            )
        );
    }

    [Fact]
    public void MemberHelloRequiresExactBuildConfigurationAndVersions()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var hello = DataGridPeerCodec.Hello(config, 0, Guid.NewGuid(), fixture.Build);
        Assert.Equal(72, hello.GetPayload().Length);
        Assert.Equal(Convert.FromHexString("0001000100010001"), hello.GetPayload()[..8]);
        DataGridPeerCodec.ValidateHello(hello, config, fixture.Build);
        for (var i = 0; i < 72; i++)
        {
            var changed = hello.GetPayload();
            changed[i] ^= 1;
            Assert.Throws<InvalidDataException>(() =>
                DataGridPeerCodec.ValidateHello(
                    DataGridPeerCodec.Frame(config, 0, hello.Kind, hello.AttemptId, changed),
                    config,
                    fixture.Build
                )
            );
        }
    }

    [Theory]
    [InlineData(0u)]
    [InlineData(67u)]
    [InlineData(1048573u)]
    [InlineData(uint.MaxValue)]
    public async Task StreamLengthRejectedBeforeReadingAnyBody(uint length)
    {
        using var fixture = new DataGridPeerFixture();
        var bytes = new byte[4];
        BinaryPrimitives.WriteUInt32BigEndian(bytes, length);
        using var stream = new MemoryStream(bytes);
        await Assert.ThrowsAsync<InvalidDataException>(async () =>
            await DataGridPeerCodec.ReadAsync(
                stream,
                fixture.Configuration,
                0,
                TestContext.Current.CancellationToken
            )
        );
        Assert.Equal(4, stream.Position);
    }

    [Fact]
    public void SnapshotChunkOffsetAndRetryRulesAreBounded()
    {
        using var fixture = new DataGridPeerFixture();
        var id = Guid.NewGuid();
        var attempt = DataGridPeerCodec.Attempt(new(1, 1));
        var target = new byte[4];
        DataGridProtocolFrame Chunk(Guid snapshot, ulong offset, byte[] bytes)
        {
            using var writer = new GridWriter(65564);
            writer.Id(snapshot);
            writer.Number(offset, 8);
            writer.Number((ulong)bytes.Length, 4);
            writer.Raw(bytes);
            return DataGridPeerCodec.Frame(
                fixture.Configuration,
                0,
                DataGridMessageKind.SnapshotChunk,
                attempt,
                writer.ToArray()
            );
        }
        Assert.Equal(
            2,
            DataGridPeerTransport.CopyChunk(Chunk(id, 0, [1, 2]), attempt, id, target, 0)
        );
        Assert.Equal(
            2,
            DataGridPeerTransport.CopyChunk(Chunk(id, 0, [1, 2]), attempt, id, target, 2)
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerTransport.CopyChunk(Chunk(id, 0, [1, 3]), attempt, id, target, 2)
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerTransport.CopyChunk(Chunk(id, 3, [1]), attempt, id, target, 2)
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerTransport.CopyChunk(Chunk(id, ulong.MaxValue, [1]), attempt, id, target, 2)
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerTransport.CopyChunk(Chunk(id, 2, []), attempt, id, target, 2)
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridPeerTransport.CopyChunk(Chunk(Guid.NewGuid(), 2, [3]), attempt, id, target, 2)
        );
        Assert.Equal(
            4,
            DataGridPeerTransport.CopyChunk(Chunk(id, 2, [3, 4]), attempt, id, target, 2)
        );
        Assert.Equal(new byte[] { 1, 2, 3, 4 }, target);
    }

    [Fact]
    public void CertificatePinsRequireExactMemberDnsAndExplicitDenylist()
    {
        using var fixture = new DataGridPeerFixture();
        var policy = fixture.Credentials(1);
        Assert.Equal(0, policy.Resolve(fixture.Certificates[0]));
        using var rogue = fixture.Issue("other.example");
        Assert.Equal(-1, policy.Resolve(rogue));
        var denied = fixture.Credentials(1, denied: [Convert.ToHexString(fixture.Fingerprints[0])]);
        Assert.Equal(-1, denied.Resolve(fixture.Certificates[0]));
        using var expired = fixture.Issue("node-a.example", expired: true);
        Assert.Equal(-1, policy.Resolve(expired));
    }

    [Fact]
    public void SnapshotLeaderObservationPersistsTermBeforeTransferAuthority()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var node = new RaftNode(config, 1, 1, new(config, 1, 0));
        var effects = node.ObserveLeader(5);
        var write = Assert.Single(effects.OfType<RaftWrite>());
        Assert.Equal(5, node.CurrentTerm);
        Assert.Equal(1, node.Durable.Term);
        Assert.Null(write.Image.Vote);
        Assert.Empty(effects.OfType<RaftSend>());
        node.Persisted(write.Token, true);
        Assert.Equal(5, node.Durable.Term);
        Assert.Empty(node.ObserveLeader(4));
    }
}
