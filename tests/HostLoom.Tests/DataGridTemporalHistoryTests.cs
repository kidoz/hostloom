using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridTemporalHistoryTests
{
    private static readonly Guid Nonce = new("50112233-4455-6677-8899-aabbccddeeff");
    private const long Lifetime = TimeSpan.TicksPerMinute * 10;

    [Fact]
    public void RelativeExpiryBindsUnobservedMutationTimeAndRejectsRefreshedDeadline()
    {
        var write = Op(Relative(10), Stored(3), 1);
        var first = Op(Read(), Found(3, 200, 5), 3);
        var second = Op(Read(), Found(3, 204, 1), 5);
        var expired = Op(Read(), Missing(205), 7);
        Assert.Equal(DataGridHistoryStatus.Valid, Check([write, first, second, expired]));
        second = second with { Outcome = new(Found(3, 204, 2), true, []) };
        Assert.Equal(DataGridHistoryStatus.Invalid, Check([write, first, second, expired]));
    }

    [Fact]
    public void RelativeTtlCannotExceedOriginalDurationOrRequireMutationAfterRead()
    {
        for (var remaining = 1; remaining <= 15; remaining++)
        {
            var write = Op(Relative(10), Stored(3), 1);
            var first = Op(Read(), Found(3, 20, remaining), 3);
            var second = Op(Read(), remaining > 5 ? Found(3, 25, remaining - 5) : Missing(25), 5);
            Assert.Equal(
                remaining <= 10 ? DataGridHistoryStatus.Valid : DataGridHistoryStatus.Invalid,
                Check([write, first, second])
            );
            if (remaining > 5)
            {
                second = second with { Outcome = new(Found(3, 25, remaining - 4), true, []) };
                Assert.Equal(DataGridHistoryStatus.Invalid, Check([write, first, second]));
            }
        }
    }

    [Fact]
    public void RelativeExpiryBeforeConditionalCannotBeUndoneByLaterRead()
    {
        var write = Op(Relative(10), Stored(3), 1);
        var first = Op(Read(), Found(3, 200, 5), 3);
        var remove = Op(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(
                    1,
                    2,
                    DataGridMutationKind.CompareRemove,
                    new(DataGridPeerFixture.Cluster, 3)
                )
            ),
            Vector(DataGridMutationStatus.Missing),
            5
        );
        var expired = Op(Read(), Missing(206), 7);
        Assert.Equal(DataGridHistoryStatus.Valid, Check([write, first, remove, expired]));
        expired = expired with { Outcome = new(Found(3, 206, 1), true, []) };
        Assert.Equal(DataGridHistoryStatus.Invalid, Check([write, first, remove, expired]));
    }

    [Fact]
    public void SessionOpenCloseNonceRetirementAndHighWaterAreChecked()
    {
        var open = Op(Open(), Opened(1, 100), 1);
        var changed = Op(
            DataGridClientRequest.OpenSession(Nonce, new(103)),
            Failed(DataGridClientFailureCode.RequestIdentityMismatch),
            3
        );
        var close = Op(
            DataGridClientRequest.CloseSession(1, 1),
            DataGridClientReply.Completed(DataGridMessageKind.CloseSessionResult),
            5
        );
        var blocked = Op(Open(), Failed(DataGridClientFailureCode.SessionExpired), 7);
        var read = Op(Read(), Missing(105), 9);
        var delayed = Op(Open(), Failed(DataGridClientFailureCode.SessionExpired), 11);
        var fresh = Op(Open(Guid.NewGuid(), 105), Opened(2, 105), 13);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            Check([open, changed, close, blocked, read, delayed, fresh], strict: true)
        );
        fresh = fresh with { Outcome = new(Opened(1, 105), true, []) };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            Check([open, changed, close, blocked, read, delayed, fresh], strict: true)
        );
    }

    [Fact]
    public void SessionExpiryPrecedesRetainedReplayButDoesNotDeleteMapData()
    {
        var open = Op(Open(), Opened(1, 100), 1);
        var request = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation());
        var write = Op(request, Stored(3), 3);
        var read = Op(Read(), Found(3, 100 + Lifetime, null), 5);
        var retry = Op(request, Failed(DataGridClientFailureCode.SessionExpired), 7);
        var fresh = Op(Open(Guid.NewGuid(), 100 + Lifetime), Opened(2, 100 + Lifetime), 9);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            Check([open, write, read, retry, fresh], strict: true)
        );
        retry = retry with { Outcome = new(Stored(3), true, []) };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            Check([open, write, read, retry, fresh], strict: true)
        );
    }

    [Fact]
    public void PrincipalOwnershipAndSequenceFailuresDoNotMutateData()
    {
        var open = Op(Open(), Opened(1, 100), 1);
        var foreign = Op(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Failed(DataGridClientFailureCode.SessionExpired),
            3
        ) with
        {
            PrincipalId = Guid.NewGuid(),
        };
        var write = Op(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(3),
            5
        );
        var next = Op(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(1, 2, value: 9)),
            Stored(4),
            7
        );
        var retired = Op(write.Request, Failed(DataGridClientFailureCode.RequestRetired), 9);
        var changed = Op(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(1, 2)),
            Failed(DataGridClientFailureCode.RequestIdentityMismatch),
            11
        );
        var gap = Op(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(1, 4)),
            Failed(DataGridClientFailureCode.InvalidRequest),
            13
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            Check([open, foreign, write, next, retired, changed, gap], strict: true)
        );
    }

    [Fact]
    public void UnknownOpenBindsNonceAndAllocatedNumberToLaterMutationAndRetry()
    {
        var unknown = Op(Open(), Failed(DataGridClientFailureCode.OutcomeUnknown), 1);
        var write = Op(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(3),
            3
        );
        var retry = Op(Open(), Opened(1, 100), 5);
        Assert.Equal(DataGridHistoryStatus.Valid, Check([unknown, write, retry], strict: true));
        retry = retry with { Outcome = new(Opened(2, 100), true, []) };
        Assert.Equal(DataGridHistoryStatus.Invalid, Check([unknown, write, retry], strict: true));
    }

    [Fact]
    public void UnknownCloseCanExplainExpiredSessionWhileEntryRemainsLive()
    {
        var open = Op(Open(), Opened(1, 100), 1);
        var write = Op(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(3),
            3
        );
        var close = Op(
            DataGridClientRequest.CloseSession(1, 2),
            Failed(DataGridClientFailureCode.OutcomeUnknown),
            5
        );
        var rejected = Op(write.Request, Failed(DataGridClientFailureCode.SessionExpired), 7);
        var read = Op(Read(), Found(3, 102, null), 9);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            Check([open, write, close, rejected, read], strict: true)
        );
    }

    [Fact]
    public void SuccessfulCloseCannotReturnRetainedMutationSuccess()
    {
        var open = Op(Open(), Opened(1, 100), 1);
        var write = Op(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(3),
            3
        );
        var close = Op(
            DataGridClientRequest.CloseSession(1, 2),
            DataGridClientReply.Completed(DataGridMessageKind.CloseSessionResult),
            5
        );
        var invalid = Op(write.Request, Stored(3), 7);
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            Check([open, write, close, invalid], strict: true)
        );
    }

    [Fact]
    public void OpenWindowAndReturnedExpiryMustAgree()
    {
        var open = Op(Open(), Opened(1, 100), 1);
        var read = Op(Read(), Missing(99), 3);
        Assert.Equal(DataGridHistoryStatus.Invalid, Check([open, read], strict: true));
        var early = Op(
            DataGridClientRequest.OpenSession(Nonce, new(100 + TimeSpan.TicksPerSecond * 6)),
            Failed(DataGridClientFailureCode.InvalidRequest),
            3
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            Check([Op(Read(), Missing(100), 1), early], strict: true)
        );
    }

    [Fact]
    public void MultiAttemptSemanticFailureAndPolicyOutcomesStayInconclusive()
    {
        var rejected = Op(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Failed(DataGridClientFailureCode.SessionExpired),
            1
        );
        rejected = rejected with
        {
            Outcome = new(
                rejected.Outcome!.Reply,
                true,
                [
                    new(Guid.NewGuid(), Guid.NewGuid(), true),
                    new(Guid.NewGuid(), Guid.NewGuid(), true),
                ]
            ),
        };
        Assert.Equal(DataGridHistoryStatus.Inconclusive, Check([rejected], strict: true));
        Assert.Equal(
            DataGridHistoryStatus.Inconclusive,
            Check([Op(Relative(10), Vector(DataGridMutationStatus.InvalidRequest), 1)])
        );
    }

    [Fact]
    public void ThirtyTwoOperationsFitTimeCapacityAtMaximumTimestamp()
    {
        var history = Enumerable
            .Range(0, 32)
            .Select(i => Op(Read(), Missing(DateTimeOffset.MaxValue.Ticks), i * 2 + 1))
            .ToArray();
        Assert.Equal(DataGridHistoryStatus.Valid, Check(history));
        Assert.Equal(
            DataGridHistoryStatus.Inconclusive,
            DataGridClientHistoryChecker.Check(history, 1).Status
        );
    }

    [Fact]
    public void LostReadAndUnobservedTimeBarrierCanRetireClosedNonceBeforeReopen()
    {
        var open = Op(Open(), Opened(1, 100), 1);
        var close = Op(
            DataGridClientRequest.CloseSession(1, 1),
            DataGridClientReply.Completed(DataGridMessageKind.CloseSessionResult),
            3
        );
        var lostRead = Op(Read(), Failed(DataGridClientFailureCode.QuorumUnavailable), 5);
        var reopen = Op(DataGridClientRequest.OpenSession(Nonce, new(110)), Opened(2, 106), 7);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            Check([open, close, lostRead, reopen], strict: true)
        );
        Assert.Equal(DataGridHistoryStatus.Valid, Check([open, close, reopen], strict: true));
    }

    [Fact]
    public void HiddenBarrierCanRetireEarlierCutoffAndKeepLaterTombstoneBeforeNextOperation()
    {
        var other = Guid.NewGuid();
        var first = Op(Open(), Opened(1, 100), 1);
        var second = Op(DataGridClientRequest.OpenSession(other, new(108)), Opened(2, 100), 3);
        var closeFirst = Op(
            DataGridClientRequest.CloseSession(1, 1),
            DataGridClientReply.Completed(DataGridMessageKind.CloseSessionResult),
            5
        );
        var closeSecond = Op(
            DataGridClientRequest.CloseSession(2, 1),
            DataGridClientReply.Completed(DataGridMessageKind.CloseSessionResult),
            7
        );
        var before = Op(Read(), Missing(102), 9);
        var reopen = Op(DataGridClientRequest.OpenSession(Nonce, new(114)), Opened(3, 110), 11);
        var mismatch = Op(
            DataGridClientRequest.OpenSession(other, new(116)),
            Failed(DataGridClientFailureCode.RequestIdentityMismatch),
            13
        );
        var after = Op(Read(), Missing(111), 15);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            Check(
                [first, second, closeFirst, closeSecond, before, reopen, mismatch, after],
                strict: true
            )
        );
    }

    private static DataGridHistoryStatus Check(
        IReadOnlyList<DataGridClientHistoryOperation> history,
        bool strict = false
    ) => DataGridClientHistoryChecker.Check(history, startsWithEmptySessions: strict).Status;

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

    private static DataGridClientRequest Open(Guid? nonce = null, long time = 100) =>
        DataGridClientRequest.OpenSession(nonce ?? Nonce, new(time + 4));

    private static DataGridClientRequest Relative(long ttl) =>
        DataGridClientRequest.Mutate(
            new(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                1,
                1,
                DataGridExpiryKind.Relative,
                ttl,
                [new(DataGridPeerFixture.Key, [7])]
            )
        );

    private static DataGridClientReply Opened(long session, long time) =>
        DataGridClientReply.Completed(
            DataGridMessageKind.OpenSessionResult,
            new(
                DataGridApplyStatus.Completed,
                sessionNumber: session,
                expiresAt: new(time + Lifetime)
            )
        );

    private static DataGridClientReply Failed(DataGridClientFailureCode code) =>
        DataGridClientReply.Failed(new(code, true));

    private static DataGridClientReply Vector(DataGridMutationStatus status, long index = 0) =>
        DataGridClientReply.Completed(
            DataGridMessageKind.MutationResult,
            new(
                DataGridApplyStatus.Completed,
                [new(status, index == 0 ? default : new(DataGridPeerFixture.Cluster, index))]
            )
        );

    private static DataGridClientReply Stored(long index) =>
        Vector(DataGridMutationStatus.Stored, index);

    private static DataGridClientReply Found(long index, long time, long? remaining) =>
        DataGridClientReply.Completed(
            DataGridMessageKind.ReadResult,
            read: new(
                new(time),
                [
                    new(
                        [7],
                        new(DataGridPeerFixture.Cluster, index),
                        remaining is { } ticks ? TimeSpan.FromTicks(ticks) : null
                    ),
                ]
            )
        );

    private static DataGridClientReply Missing(long time) =>
        DataGridClientReply.Completed(DataGridMessageKind.ReadResult, read: new(new(time), [null]));

    private static DataGridClientHistoryOperation Op(
        DataGridClientRequest request,
        DataGridClientReply reply,
        long order
    ) =>
        new(
            Guid.NewGuid(),
            DataGridPeerFixture.Principal,
            order,
            order + 1,
            request,
            new(reply, true, [])
        );
}
