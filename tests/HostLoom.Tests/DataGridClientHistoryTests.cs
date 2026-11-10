using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridClientHistoryTests
{
    [Fact]
    public void OverlapAllowsReadBeforeWriteButRealTimePreventsStaleRead()
    {
        var write = Operation(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(2),
            1,
            4
        );
        var read = Operation(Read(), Missing(), 2, 3);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([write, read]).Status
        );
        read = read with { InvocationOrder = 5, CompletionOrder = 6 };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check([write, read]).Status
        );
    }

    [Fact]
    public void RetriedLogicalResultDoesNotApplyAgainAfterAnotherSessionWrite()
    {
        var first = Operation(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(2),
            1,
            2
        );
        var second = Operation(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(2, value: 9)),
            Stored(3),
            3,
            4
        );
        var retry = first with
        {
            OperationId = Guid.NewGuid(),
            InvocationOrder = 5,
            CompletionOrder = 6,
        };
        var read = Operation(
            Read(),
            DataGridClientReply.Completed(
                DataGridMessageKind.ReadResult,
                read: new(new(42), [new([9], new(DataGridPeerFixture.Cluster, 3), null)])
            ),
            7,
            8
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([first, second, retry, read]).Status
        );
    }

    [Fact]
    public void PendingAndUnknownBranchWhileRelativeTtlAndBudgetRemainInconclusive()
    {
        var write = Operation(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(2),
            1,
            2
        );
        Assert.Equal(
            DataGridHistoryStatus.Inconclusive,
            DataGridClientHistoryChecker.Check([write], 1).Status
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker
                .Check([write with { Outcome = null, CompletionOrder = null }])
                .Status
        );
        var unknown = write with
        {
            Outcome = new(
                DataGridClientReply.Failed(new(DataGridClientFailureCode.OutcomeUnknown, true)),
                true,
                []
            ),
        };
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([unknown]).Status
        );
        var ttl = Operation(
            Read(),
            DataGridClientReply.Completed(
                DataGridMessageKind.ReadResult,
                read: new(
                    new(42),
                    [new([7], new(DataGridPeerFixture.Cluster, 2), TimeSpan.FromSeconds(1))]
                )
            ),
            3,
            4
        );
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check([write, ttl]).Status
        );
    }

    [Fact]
    public void HistoryUsesActualIncarnationForConditionalCommandsAndOwnsSnapshots()
    {
        var history = new DataGridClientHistory();
        var request = DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(
                1,
                2,
                DataGridMutationKind.CompareExchange,
                new(DataGridPeerFixture.Cluster, 2)
            )
        );
        var id = history.Begin(DataGridPeerFixture.Cluster, DataGridPeerFixture.Principal, request);
        var before = history.GetOperations();
        history.Complete(id, new(Stored(3), true, []));
        Assert.Null(before[0].Outcome);
        Assert.NotNull(history.GetOperations()[0].Outcome);
    }

    [Fact]
    public void PreAdmissionRejectionDoesNotChangeModel()
    {
        var rejected = Operation(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            DataGridClientReply.Failed(new(DataGridClientFailureCode.Unauthorized, false)),
            1,
            2,
            admitted: false
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker
                .Check([rejected, Operation(Read(), Missing(), 3, 4)])
                .Status
        );
    }

    [Fact]
    public void UnknownMayCommitAfterItsResponseAndAfterAnotherReadCompletes()
    {
        var unknown = Unknown(DataGridClientCodecTests.Mutation(), 1, 2);
        var missing = Operation(Read(), Missing(), 3, 4);
        var found = Operation(Read(), Found(7, 2), 5, 6);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([unknown, missing, found]).Status
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([unknown, missing]).Status
        );
    }

    [Fact]
    public void PendingWriteCanExplainReadButCannotPrecedeItsOwnInvocation()
    {
        var pending = Unknown(DataGridClientCodecTests.Mutation(), 1, 2) with
        {
            CompletionOrder = null,
            Outcome = null,
        };
        var read = Operation(Read(), Found(7, 2), 3, 4);
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([pending, read]).Status
        );
        pending = pending with { InvocationOrder = 5 };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check([pending, read]).Status
        );
    }

    [Fact]
    public void SymbolicConditionalMismatchCannotLaterBindToTheExcludedVersion()
    {
        var unknown = Unknown(DataGridClientCodecTests.Mutation(), 1, 2);
        var cas = Operation(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(
                    1,
                    2,
                    DataGridMutationKind.CompareExchange,
                    new(DataGridPeerFixture.Cluster, 2)
                )
            ),
            Result(DataGridMutationStatus.VersionMismatch),
            3,
            4
        );
        var read = Operation(Read(), Found(7, 2), 5, 6);
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check([unknown, cas, read]).Status
        );
        read = read with { Outcome = new(Found(7, 3), true, []) };
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([unknown, cas, read]).Status
        );
    }

    [Fact]
    public void SymbolicRecreationCannotReuseAnEarlierVersion()
    {
        var first = Operation(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(2),
            1,
            2
        );
        var remove = Operation(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(1, 2, DataGridMutationKind.Remove)
            ),
            Result(DataGridMutationStatus.Removed),
            3,
            4
        );
        var recreate = Unknown(DataGridClientCodecTests.Mutation(1, 3), 5, 6);
        var read = Operation(Read(), Found(7, 2), 7, 8);
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check([first, remove, recreate, read]).Status
        );
    }

    [Fact]
    public void AbsoluteExpiryUsesReadTimeAndCannotBeRefreshedByRetainedReplay()
    {
        var request = DataGridClientRequest.Mutate(
            new(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                1,
                1,
                DataGridExpiryKind.Absolute,
                100,
                [new(DataGridPeerFixture.Key, [7])]
            )
        );
        var write = Operation(request, Stored(2), 1, 2);
        var live = Operation(Read(), Found(7, 2, 40, 60), 3, 4);
        var missing = Operation(
            Read(),
            DataGridClientReply.Completed(
                DataGridMessageKind.ReadResult,
                read: new(new(100), [null])
            ),
            5,
            6
        );
        var replay = write with
        {
            OperationId = Guid.NewGuid(),
            InvocationOrder = 7,
            CompletionOrder = 8,
        };
        var later = Operation(
            Read(),
            DataGridClientReply.Completed(
                DataGridMessageKind.ReadResult,
                read: new(new(101), [null])
            ),
            9,
            10
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([write, live, missing, replay, later]).Status
        );
        later = later with { Outcome = new(Found(7, 2, 101, 1), true, []) };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check([write, live, missing, replay, later]).Status
        );
    }

    [Fact]
    public void UnobservedClockAdvanceCanExpireEntryBeforeConditionalMutation()
    {
        var write = Operation(
            DataGridClientRequest.Mutate(
                new(
                    DataGridMutationKind.Put,
                    DataGridPeerFixture.Map,
                    1,
                    1,
                    DataGridExpiryKind.Absolute,
                    100,
                    [new(DataGridPeerFixture.Key, [7])]
                )
            ),
            Stored(2),
            1,
            2
        );
        var remove = Operation(
            DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(
                    1,
                    2,
                    DataGridMutationKind.CompareRemove,
                    new(DataGridPeerFixture.Cluster, 2)
                )
            ),
            Result(DataGridMutationStatus.Missing),
            3,
            4
        );
        var read = Operation(
            Read(),
            DataGridClientReply.Completed(
                DataGridMessageKind.ReadResult,
                read: new(new(101), [null])
            ),
            5,
            6
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([write, remove, read]).Status
        );
    }

    [Fact]
    public void RelativeExpiryBranchesAndAppliedSessionFailuresRequireSessionMode()
    {
        var relative = Unknown(
            new(
                DataGridMutationKind.Put,
                DataGridPeerFixture.Map,
                1,
                1,
                DataGridExpiryKind.Relative,
                10,
                [new(DataGridPeerFixture.Key, [7])]
            ),
            1,
            2
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([relative]).Status
        );
        var session = relative with
        {
            Request = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Outcome = new(
                DataGridClientReply.Failed(new(DataGridClientFailureCode.SessionExpired, true)),
                true,
                []
            ),
        };
        Assert.Equal(
            DataGridHistoryStatus.Inconclusive,
            DataGridClientHistoryChecker.Check([session]).Status
        );
    }

    [Fact]
    public void UnknownBatchRetainsPartialResultsAndStoredItemOrdinal()
    {
        var first = Operation(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(2),
            1,
            2
        );
        var otherKey = DataGridKey.FromUtf8("catalog:us");
        var command = new DataGridMutationCommand(
            DataGridMutationKind.PutIfAbsent,
            DataGridPeerFixture.Map,
            2,
            1,
            DataGridExpiryKind.Never,
            0,
            [new(DataGridPeerFixture.Key, [7]), new(otherKey, [9])]
        );
        var unknown = Unknown(command, 3, 4);
        var vector = DataGridClientReply.Completed(
            DataGridMessageKind.ReadResult,
            read: new(
                new(42),
                [
                    new([7], new(DataGridPeerFixture.Cluster, 2), null),
                    new([9], new(DataGridPeerFixture.Cluster, 3, 1), null),
                ]
            )
        );
        var read = Operation(
            DataGridClientRequest.Read(
                DataGridPeerFixture.Map,
                [DataGridPeerFixture.Key, otherKey]
            ),
            vector,
            5,
            6
        );
        var retry = Operation(
            DataGridClientRequest.Mutate(command),
            DataGridClientReply.Completed(
                DataGridMessageKind.MutationResult,
                new(
                    DataGridApplyStatus.Completed,
                    [
                        new(DataGridMutationStatus.AlreadyExists),
                        new(DataGridMutationStatus.Stored, new(DataGridPeerFixture.Cluster, 3, 1)),
                    ]
                )
            ),
            7,
            8
        );
        Assert.Equal(
            DataGridHistoryStatus.Valid,
            DataGridClientHistoryChecker.Check([first, unknown, read, retry]).Status
        );
        var wrongOrdinal = DataGridClientReply.Completed(
            DataGridMessageKind.ReadResult,
            read: new(
                new(42),
                [
                    new([7], new(DataGridPeerFixture.Cluster, 2), null),
                    new([9], new(DataGridPeerFixture.Cluster, 3), null),
                ]
            )
        );
        read = read with { Outcome = new(wrongOrdinal, true, []) };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check([first, unknown, read, retry]).Status
        );
    }

    [Fact]
    public void SymbolicOrderNeedsRoomBeforeAnObservedVersion()
    {
        var unknown = Unknown(DataGridClientCodecTests.Mutation(), 1, 2);
        var second = Operation(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(1, 2)),
            Stored(1),
            3,
            4
        );
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check([unknown, second]).Status
        );
    }

    [Fact]
    public void CapturedGridTimeCannotMoveBackward()
    {
        var write = Operation(
            DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation()),
            Stored(2),
            1,
            2
        );
        var first = Operation(Read(), Found(7, 2, 42), 3, 4);
        var backward = Operation(Read(), Found(7, 2, 41), 5, 6);
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check([write, first, backward]).Status
        );
    }

    private static DataGridClientHistoryOperation Unknown(
        DataGridMutationCommand command,
        long start,
        long end
    ) =>
        Operation(
            DataGridClientRequest.Mutate(command),
            DataGridClientReply.Failed(new(DataGridClientFailureCode.OutcomeUnknown, true)),
            start,
            end
        );

    private static DataGridClientReply Found(
        byte value,
        long index,
        long time = 42,
        long? remaining = null
    ) =>
        DataGridClientReply.Completed(
            DataGridMessageKind.ReadResult,
            read: new(
                new(time),
                [
                    new(
                        [value],
                        new(DataGridPeerFixture.Cluster, index),
                        remaining is { } ticks ? TimeSpan.FromTicks(ticks) : null
                    ),
                ]
            )
        );

    private static DataGridClientReply Result(DataGridMutationStatus status) =>
        DataGridClientReply.Completed(
            DataGridMessageKind.MutationResult,
            new(DataGridApplyStatus.Completed, [new(status)])
        );

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

    private static DataGridClientReply Stored(long index) =>
        DataGridClientReply.Completed(
            DataGridMessageKind.MutationResult,
            new(
                DataGridApplyStatus.Completed,
                [new(DataGridMutationStatus.Stored, new(DataGridPeerFixture.Cluster, index))]
            )
        );

    private static DataGridClientReply Missing() =>
        DataGridClientReply.Completed(DataGridMessageKind.ReadResult, read: new(new(42), [null]));

    private static DataGridClientHistoryOperation Operation(
        DataGridClientRequest request,
        DataGridClientReply reply,
        long start,
        long end,
        bool admitted = true
    ) =>
        new(
            Guid.NewGuid(),
            DataGridPeerFixture.Principal,
            start,
            end,
            request,
            new(reply, admitted, [])
        );
}
