using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridClientCodecTests
{
    [Theory]
    [InlineData(0)]
    [InlineData(1)]
    [InlineData(2)]
    [InlineData(3)]
    public void RequestsRoundTripAndRejectEveryTruncatedBody(int kind)
    {
        using var fixture = new DataGridPeerFixture();
        var request = kind switch
        {
            0 => DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            1 => DataGridClientRequest.Mutate(Mutation()),
            2 => DataGridClientRequest.OpenSession(DataGridPeerFixture.Nonce, new(42)),
            _ => DataGridClientRequest.CloseSession(1, 1),
        };
        var frame = Frame(fixture, request);
        var copy = DataGridClientCodec.DecodeRequest(frame);
        Assert.Equal(request.Kind, copy.Kind);
        Assert.Equal(request.SessionNumber, copy.SessionNumber);
        Assert.Equal(request.RequestNumber, copy.RequestNumber);
        var bytes = frame.GetPayload();
        for (var i = 0; i < bytes.Length; i++)
            Assert.Throws<InvalidDataException>(() =>
                DataGridClientCodec.DecodeRequest(WithPayload(frame, bytes[..i]))
            );
        Assert.Throws<InvalidDataException>(() =>
            DataGridClientCodec.DecodeRequest(WithPayload(frame, [.. bytes, 0]))
        );
    }

    [Fact]
    public void HelloRejectsVersionAndConfigurationMismatch()
    {
        using var fixture = new DataGridPeerFixture();
        var hello = DataGridClientCodec.Hello(
            fixture.Configuration.IncarnationId,
            DataGridPeerFixture.Principal,
            Guid.NewGuid(),
            fixture.Build,
            fixture.Configuration.ComputeDigest()
        );
        Assert.Equal(72, hello.GetPayload().Length);
        DataGridClientCodec.ValidateHello(hello, fixture.Configuration);
        foreach (var offset in new[] { 0, 2, 4, 6, 40 })
        {
            var bytes = hello.GetPayload();
            bytes[offset] ^= 1;
            Assert.Throws<InvalidDataException>(() =>
                DataGridClientCodec.ValidateHello(WithPayload(hello, bytes), fixture.Configuration)
            );
        }
    }

    [Fact]
    public void ReplyBindsAttemptCountAndCanonicalVersions()
    {
        using var fixture = new DataGridPeerFixture();
        var request = DataGridClientRequest.Mutate(Mutation());
        var attempt = Guid.NewGuid();
        var version = new DataGridEntryVersion(fixture.Configuration.IncarnationId, 2);
        var reply = DataGridClientReply.Completed(
            DataGridMessageKind.MutationResult,
            new(DataGridApplyStatus.Completed, [new(DataGridMutationStatus.Stored, version)])
        );
        var frame = DataGridClientCodec.EncodeReply(
            request,
            reply,
            fixture.Configuration.IncarnationId,
            fixture.Configuration.GetMember(0).MemberId,
            attempt
        );
        Assert.Equal(
            version,
            DataGridClientCodec
                .DecodeReply(frame, request, attempt, fixture.Configuration)
                .Result!.GetItem(0)
                .Version
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridClientCodec.DecodeReply(frame, request, Guid.NewGuid(), fixture.Configuration)
        );
        var body = frame.GetPayload();
        for (var i = 0; i < body.Length; i++)
            Assert.Throws<InvalidDataException>(() =>
                DataGridClientCodec.DecodeReply(
                    WithPayload(frame, body[..i]),
                    request,
                    attempt,
                    fixture.Configuration
                )
            );
        foreach (var offset in new[] { 0, 2, 3, 4, body.Length - 1 })
        {
            var invalid = (byte[])body.Clone();
            invalid[offset] = 255;
            Assert.Throws<InvalidDataException>(() =>
                DataGridClientCodec.DecodeReply(
                    WithPayload(frame, invalid),
                    request,
                    attempt,
                    fixture.Configuration
                )
            );
        }
    }

    [Fact]
    public void ReadVectorOwnsValuesAndRequiresPositivePresentTtl()
    {
        using var fixture = new DataGridPeerFixture();
        var request = DataGridClientRequest.Read(
            DataGridPeerFixture.Map,
            [DataGridPeerFixture.Key, DataGridKey.FromUtf8("catalog:us")]
        );
        var version = new DataGridEntryVersion(fixture.Configuration.IncarnationId, 2);
        var read = new DataGridReadResult(
            new(42),
            [new([7], version, TimeSpan.FromTicks(5)), null]
        );
        var reply = DataGridClientReply.Completed(DataGridMessageKind.ReadResult, read: read);
        var frame = DataGridClientCodec.EncodeReply(
            request,
            reply,
            fixture.Configuration.IncarnationId,
            fixture.Configuration.GetMember(0).MemberId,
            Guid.NewGuid()
        );
        var result = DataGridClientCodec
            .DecodeReply(frame, request, frame.AttemptId, fixture.Configuration)
            .Read!;
        Assert.Equal(new DataGridTimestamp(42), result.CapturedAt);
        Assert.Equal(TimeSpan.FromTicks(5), result.GetItem(0)!.RemainingTimeToLive);
        Assert.Null(result.GetItem(1));
        result.GetItem(0)!.GetValue()[0] = 0;
        Assert.Equal(7, result.GetItem(0)!.GetValue()[0]);
        var body = frame.GetPayload();
        for (var i = 0; i < body.Length; i++)
            Assert.Throws<InvalidDataException>(() =>
                DataGridClientCodec.DecodeReply(
                    WithPayload(frame, body[..i]),
                    request,
                    frame.AttemptId,
                    fixture.Configuration
                )
            );
    }

    [Fact]
    public void FailureRejectsUnknownRedirectAndImpossibleUnknownFlag()
    {
        using var fixture = new DataGridPeerFixture();
        Assert.Throws<ArgumentException>(() =>
            new DataGridClientFailure(DataGridClientFailureCode.OutcomeUnknown, false)
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridClientFailure(DataGridClientFailureCode.Redirect, false)
        );
        var request = DataGridClientRequest.Read(
            DataGridPeerFixture.Map,
            [DataGridPeerFixture.Key]
        );
        var frame = DataGridClientCodec.EncodeReply(
            request,
            DataGridClientReply.Failed(
                new(DataGridClientFailureCode.Redirect, false, Guid.NewGuid())
            ),
            fixture.Configuration.IncarnationId,
            fixture.Configuration.GetMember(0).MemberId,
            Guid.NewGuid()
        );
        Assert.Throws<InvalidDataException>(() =>
            DataGridClientCodec.DecodeReply(frame, request, frame.AttemptId, fixture.Configuration)
        );
    }

    [Fact]
    public void MapActionsAreDefaultDenyAndConfigurationBound()
    {
        using var fixture = new DataGridPeerFixture();
        var principal = new DataGridClientPrincipal(
            DataGridPeerFixture.Principal,
            fixture.Fingerprints[0],
            false,
            [new(DataGridPeerFixture.Map, DataGridMapActions.Read)]
        );
        Assert.True(
            principal.Allows(
                DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key])
            )
        );
        Assert.False(principal.Allows(DataGridClientRequest.Mutate(Mutation())));
        Assert.False(principal.Allows(DataGridClientRequest.OpenSession(Guid.NewGuid(), new(42))));
        var unknown = new DataGridClientPrincipal(
            DataGridPeerFixture.Principal,
            fixture.Fingerprints[0],
            true,
            [new(Guid.NewGuid(), DataGridMapActions.Read)]
        );
        Assert.Throws<ArgumentException>(() =>
            new DataGridClientAccessPolicy(fixture.Configuration, fixture.Root, [unknown])
        );
    }

    internal static DataGridMutationCommand Mutation(
        long session = 1,
        ulong number = 1,
        DataGridMutationKind kind = DataGridMutationKind.Put,
        DataGridEntryVersion version = default,
        byte value = 7
    ) =>
        new(
            kind,
            DataGridPeerFixture.Map,
            session,
            number,
            DataGridExpiryKind.Never,
            0,
            [
                new(
                    DataGridPeerFixture.Key,
                    kind is DataGridMutationKind.Remove or DataGridMutationKind.CompareRemove
                        ? []
                        : [value],
                    version
                ),
            ]
        );

    private static DataGridProtocolFrame Frame(
        DataGridPeerFixture fixture,
        DataGridClientRequest request
    ) =>
        DataGridClientCodec.EncodeRequest(
            request,
            fixture.Configuration.IncarnationId,
            DataGridPeerFixture.Principal,
            Guid.NewGuid()
        );

    private static DataGridProtocolFrame WithPayload(DataGridProtocolFrame frame, byte[] payload) =>
        new(
            frame.Kind,
            frame.IncarnationId,
            frame.SenderId,
            frame.SessionNumber,
            frame.RequestNumber,
            frame.AttemptId,
            payload
        );
}
