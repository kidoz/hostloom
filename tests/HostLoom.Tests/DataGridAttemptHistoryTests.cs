using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using Xunit;

namespace HostLoom.Tests;

public sealed class DataGridAttemptHistoryTests
{
    [Theory]
    [InlineData(false)]
    [InlineData(true)]
    public void LostAttemptAndSemanticRetryAreCheckedWithObservedOrdering(bool changedIdentity)
    {
        var (history, operation) = Trace(changedIdentity);
        Assert.Equal(DataGridHistoryStatus.Valid, Check(history.GetOperations()).Status);
        var altered = history.GetOperations().ToArray();
        if (changedIdentity)
            altered[operation] = altered[operation] with { Request = Mutation() };
        else
        {
            var outcome = altered[operation].Outcome!;
            var reply = DataGridClientReply.Failed(
                new(DataGridClientFailureCode.RequestIdentityMismatch, true)
            );
            var attempts = outcome.Attempts.ToArray();
            attempts[^1] = attempts[^1] with { Reply = reply };
            altered[operation] = altered[operation] with { Outcome = new(reply, true, attempts) };
        }
        Assert.Equal(DataGridHistoryStatus.Invalid, Check(altered).Status);
        Assert.Equal(
            DataGridHistoryStatus.Inconclusive,
            DataGridClientHistoryChecker.Check(history.GetOperations(), 1, true).Status
        );
    }

    [Theory]
    [InlineData("missing-order")]
    [InlineData("missing-reply")]
    [InlineData("duplicate-id")]
    [InlineData("outside-call")]
    [InlineData("overlap")]
    [InlineData("unsubmitted-reply")]
    [InlineData("early-success")]
    public void IncompleteOrContradictoryAttemptEvidenceCannotPass(string defect)
    {
        var (history, operation) = Trace(true);
        var altered = history.GetOperations().ToArray();
        var outcome = altered[operation].Outcome!;
        var attempts = outcome.Attempts.ToArray();
        switch (defect)
        {
            case "missing-order":
                attempts[0] = attempts[0] with { InvocationOrder = null };
                break;
            case "missing-reply":
                attempts[^1] = attempts[^1] with { Reply = null };
                break;
            case "duplicate-id":
                attempts[^1] = attempts[^1] with { AttemptId = attempts[0].AttemptId };
                break;
            case "outside-call":
                attempts[0] = attempts[0] with
                {
                    InvocationOrder = altered[operation].InvocationOrder,
                };
                break;
            case "overlap":
                attempts[^1] = attempts[^1] with { InvocationOrder = attempts[0].CompletionOrder };
                break;
            case "unsubmitted-reply":
                attempts[^1] = attempts[^1] with { RequestSubmitted = false };
                break;
            case "early-success":
                attempts[0] = attempts[0] with { Reply = Stored() };
                break;
        }
        altered[operation] = altered[operation] with
        {
            Outcome = new(outcome.Reply, true, attempts),
        };
        Assert.Equal(DataGridHistoryStatus.Inconclusive, Check(altered).Status);
    }

    [Fact]
    public void ExpandedAttemptsRetainTheThirtyTwoOperationLimit()
    {
        var (history, _) = Trace(true);
        var operations = history.GetOperations().ToList();
        for (var i = operations.Count; i < 32; i++)
            operations.Add(
                new(
                    Guid.NewGuid(),
                    DataGridPeerFixture.Principal,
                    1000 + i * 2,
                    1001 + i * 2,
                    Mutation(),
                    new(Stored(), true, [])
                )
            );
        Assert.Equal(DataGridHistoryStatus.Inconclusive, Check(operations).Status);
    }

    private static (DataGridClientHistory History, int Operation) Trace(bool changedIdentity)
    {
        var history = new DataGridClientHistory();
        var open = history.Begin(
            DataGridPeerFixture.Cluster,
            DataGridPeerFixture.Principal,
            DataGridClientRequest.OpenSession(Guid.NewGuid(), new(104))
        );
        var expires = 100 + TimeSpan.TicksPerMinute * 10;
        history.Complete(
            open,
            new(
                DataGridClientReply.Completed(
                    DataGridMessageKind.OpenSessionResult,
                    new(DataGridApplyStatus.Completed, sessionNumber: 1, expiresAt: new(expires))
                ),
                true,
                []
            )
        );
        if (changedIdentity)
        {
            var stored = history.Begin(
                DataGridPeerFixture.Cluster,
                DataGridPeerFixture.Principal,
                Mutation()
            );
            history.Complete(stored, new(Stored(), true, []));
        }
        var request = changedIdentity ? Mutation(9) : Mutation();
        var call = history.Begin(
            DataGridPeerFixture.Cluster,
            DataGridPeerFixture.Principal,
            request
        );
        var lost = new DataGridClientAttempt(Guid.NewGuid(), Guid.NewGuid(), true)
        {
            InvocationOrder = history.AttemptEvent(),
            CompletionOrder = history.AttemptEvent(),
        };
        var reply = DataGridClientReply.Failed(
            new(
                changedIdentity
                    ? DataGridClientFailureCode.RequestIdentityMismatch
                    : DataGridClientFailureCode.SessionExpired,
                true
            )
        );
        var retry = new DataGridClientAttempt(Guid.NewGuid(), Guid.NewGuid(), true)
        {
            InvocationOrder = history.AttemptEvent(),
            CompletionOrder = history.AttemptEvent(),
            Reply = reply,
        };
        history.Complete(call, new(reply, true, [lost, retry]));
        var read = history.Begin(
            DataGridPeerFixture.Cluster,
            DataGridPeerFixture.Principal,
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key])
        );
        history.Complete(
            read,
            new(
                DataGridClientReply.Completed(
                    DataGridMessageKind.ReadResult,
                    read: new(
                        new(changedIdentity ? 100 : expires),
                        [new([7], new(DataGridPeerFixture.Cluster, 3), null)]
                    )
                ),
                true,
                []
            )
        );
        return (history, changedIdentity ? 2 : 1);
    }

    private static DataGridClientRequest Mutation(byte value = 7) =>
        DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(value: value));

    private static DataGridClientReply Stored() =>
        DataGridClientReply.Completed(
            DataGridMessageKind.MutationResult,
            new(
                DataGridApplyStatus.Completed,
                [new(DataGridMutationStatus.Stored, new(DataGridPeerFixture.Cluster, 3))]
            )
        );

    private static DataGridHistoryCheck Check(
        IReadOnlyList<DataGridClientHistoryOperation> history
    ) => DataGridClientHistoryChecker.Check(history, startsWithEmptySessions: true);
}
