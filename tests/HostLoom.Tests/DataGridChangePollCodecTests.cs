using System.Buffers.Binary;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridChangePollCodecTests
{
    private static readonly Guid Map = DataGridPeerFixture.Map;

    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void RequestRoundTripsAndRejectsMalformedCursor(bool resume)
    {
        using var fixture = new DataGridPeerFixture();
        DataGridChangeCursor? after = resume ? new(Guid.NewGuid(), Map, 3) : null;
        var request = DataGridClientRequest.PollChanges(Map, after);
        var frame = DataGridClientCodec.EncodeRequest(
            request,
            fixture.Configuration.IncarnationId,
            DataGridPeerFixture.Principal,
            Guid.NewGuid()
        );
        var copy = DataGridClientCodec.DecodeRequest(frame);
        Assert.Equal(DataGridMessageKind.PollChanges, copy.Kind);
        Assert.Equal(Map, copy.MapId);
        Assert.Equal(after, copy.ChangeCursor);
        Assert.Equal(0, frame.SessionNumber);
        Assert.Equal(0UL, frame.RequestNumber);
        var bytes = frame.GetPayload();
        for (var i = 0; i < bytes.Length; i++)
            Assert.Throws<InvalidDataException>(() =>
                DataGridClientCodec.DecodeRequest(WithBody(frame, bytes[..i]))
            );
        Assert.Throws<InvalidDataException>(() =>
            DataGridClientCodec.DecodeRequest(WithBody(frame, [.. bytes, 0]))
        );
        bytes[16] = 2;
        Assert.Throws<InvalidDataException>(() =>
            DataGridClientCodec.DecodeRequest(WithBody(frame, bytes))
        );
        if (resume)
        {
            bytes[16] = 1;
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(^8), -1);
            Assert.Throws<InvalidDataException>(() =>
                DataGridClientCodec.DecodeRequest(WithBody(frame, bytes))
            );
        }
    }

    [Fact]
    public void PageBindsPositionAndAcceptsMaximumKeysInCommitOrder()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var start = new DataGridChangeCursor(Guid.NewGuid(), Map, 1);
        var request = DataGridClientRequest.PollChanges(Map, start);
        var key = new DataGridKey(new byte[DataGridKey.MaxByteCount]);
        var changes = Enumerable
            .Range(0, DataGridChangePage.MaxChanges)
            .Select(i => new DataGridCommittedChange(10 + i / 2, key))
            .ToArray();
        var advanced = start with { Sequence = start.Sequence + changes.Length };
        var page = new DataGridChangePage(advanced, changes, new(80), 90, false, true);
        var frame = EncodePage(config, request, page);
        DataGridChangePage Decode(DataGridProtocolFrame candidate, DataGridClientRequest query) =>
            DataGridClientCodec.DecodeReply(candidate, query, frame.AttemptId, config).ChangePage!;

        Assert.True(frame.Encode().Length < DataGridProtocolFrame.MaxEncodedByteCount);
        var copy = Decode(frame, request);
        Assert.Equal(changes, copy.Changes.ToArray());
        Assert.Equal(advanced, copy.Cursor);
        Assert.Equal(new DataGridTimestamp(80), copy.AppliedTime);
        Assert.Equal(90, copy.AppliedIndex);
        Assert.True(copy.HasMore);
        Assert.False(copy.Gap);

        Assert.Throws<InvalidDataException>(() =>
            Decode(frame, DataGridClientRequest.PollChanges(Map))
        );
        Assert.Throws<InvalidDataException>(() =>
            Decode(frame, DataGridClientRequest.PollChanges(Guid.NewGuid(), start))
        );
        Assert.Throws<InvalidDataException>(() =>
            Decode(frame, DataGridClientRequest.PollChanges(Map, advanced))
        );
        Assert.Throws<InvalidDataException>(() =>
            Decode(
                frame,
                DataGridClientRequest.PollChanges(Map, start with { Generation = Guid.NewGuid() })
            )
        );
        var bytes = frame.GetPayload();
        Assert.Throws<InvalidDataException>(() => Decode(WithBody(frame, [.. bytes, 0]), request));
        Assert.Throws<InvalidDataException>(() => Decode(WithBody(frame, bytes[..^1]), request));
        var invalidCount = (byte[])bytes.Clone();
        BinaryPrimitives.WriteUInt16BigEndian(invalidCount.AsSpan(58), 129);
        Assert.Throws<InvalidDataException>(() => Decode(WithBody(frame, invalidCount), request));
        foreach (var invalidIndex in new long[] { 0, page.AppliedIndex + 1 })
        {
            BinaryPrimitives.WriteInt64BigEndian(bytes.AsSpan(60), invalidIndex);
            Assert.Throws<InvalidDataException>(() => Decode(WithBody(frame, bytes), request));
        }
    }

    [Fact]
    public void GapRequiresAnEarlierPositionAndCarriesAnEmptyReplacementTail()
    {
        using var fixture = new DataGridPeerFixture();
        var config = fixture.Configuration;
        var request = DataGridClientRequest.PollChanges(Map, new(Guid.NewGuid(), Map, 3));
        var page = new DataGridChangePage(
            new(Guid.NewGuid(), Map, 0),
            [],
            new(10),
            10,
            true,
            false
        );
        var frame = EncodePage(config, request, page);
        var gap = DataGridClientCodec
            .DecodeReply(frame, request, frame.AttemptId, config)
            .ChangePage!;
        Assert.True(gap.Gap);
        Assert.Empty(gap.Changes);
        Assert.Equal(page.Cursor, gap.Cursor);
        Assert.Throws<InvalidDataException>(() =>
            DataGridClientCodec.DecodeReply(
                frame,
                DataGridClientRequest.PollChanges(Map),
                frame.AttemptId,
                config
            )
        );

        var invalid = EncodePage(
            config,
            request,
            new(
                page.Cursor with
                {
                    Sequence = 1,
                },
                [new(9, DataGridPeerFixture.Key)],
                new(10),
                10,
                true,
                false
            )
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridClientCodec.DecodeReply(invalid, request, invalid.AttemptId, config)
        );
    }

    [Fact]
    public void ObservationIsAnIndependentMapGrant()
    {
        var principal = new DataGridClientPrincipal(
            Guid.NewGuid(),
            new byte[32],
            false,
            [new(Map, DataGridMapActions.ObserveChanges)]
        );
        Assert.True(principal.Allows(DataGridClientRequest.PollChanges(Map)));
        Assert.False(principal.Allows(DataGridClientRequest.PollChanges(Guid.NewGuid())));
        Assert.False(principal.Allows(DataGridClientRequest.Read(Map, [DataGridPeerFixture.Key])));
        Assert.False(
            principal.Allows(DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()))
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridClientPrincipal(
                Guid.NewGuid(),
                new byte[32],
                false,
                [new(Map, (DataGridMapActions)8)]
            )
        );
    }

    private static DataGridProtocolFrame EncodePage(
        DataGridBootstrapConfiguration configuration,
        DataGridClientRequest request,
        DataGridChangePage page
    ) =>
        DataGridClientCodec.EncodeReply(
            request,
            DataGridClientReply.Completed(DataGridMessageKind.PollChangesResult, changePage: page),
            configuration.IncarnationId,
            configuration.GetMember(0).MemberId,
            Guid.NewGuid()
        );

    private static DataGridProtocolFrame WithBody(DataGridProtocolFrame frame, byte[] body) =>
        new(
            frame.Kind,
            frame.IncarnationId,
            frame.SenderId,
            frame.SessionNumber,
            frame.RequestNumber,
            frame.AttemptId,
            body
        );
}
