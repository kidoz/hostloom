using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using Xunit;
using GridClient = HostLoom.DataGrid.Client.DataGridClient;

namespace HostLoom.Tests;

public sealed class DataGridAdmissionHistoryTlsTests
{
    [Theory(Timeout = 45000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task ConditionalCallsRaceBeforeActorAdmissionAndRetainBothResults(bool deleteFirst)
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        var token = deadline.Token;
        await using var fixture = new DataGridClientTlsTests.ClientCluster();
        var leader = await fixture.Cluster.Leader(token);
        var history = new DataGridClientHistory();
        await using var seed = fixture.Client(leader, history);
        await using var writer = fixture.Client(leader, history);
        await using var remover = fixture.Client(leader, history);
        var first = await Open(seed, token);
        var second = await Open(writer, token);
        var third = await Open(remover, token);
        var stored = await seed.ExecuteAsync(Mutation(first), token);
        var version = Item(stored).Version;
        var cas = Mutation(
            second,
            kind: DataGridMutationKind.CompareExchange,
            expected: version,
            value: 9
        );
        var remove = Mutation(third, kind: DataGridMutationKind.CompareRemove, expected: version);
        var requests = deleteFirst ? new[] { remove, cas } : [cas, remove];
        var clients = deleteFirst ? new[] { remover, writer } : [writer, remover];
        var results = await Race(fixture, history, clients, requests, token);
        var statuses = results.Select(result => Item(result).Status).ToArray();
        Assert.Equal(
            1,
            statuses.Count(status =>
                status is DataGridMutationStatus.Stored or DataGridMutationStatus.Removed
            )
        );
        Assert.Contains(
            statuses,
            status =>
                status is DataGridMutationStatus.VersionMismatch or DataGridMutationStatus.Missing
        );
        await seed.ExecuteAsync(Read(), token);
        for (var i = 0; i < 2; i++)
            Assert.Equal(Item(results[i]), Item(await clients[i].ExecuteAsync(requests[i], token)));
        AssertValid(history);
        RejectChangedDuplicate(history);
        fixture.AssertHealthy();
    }

    [Fact(Timeout = 45000)]
    public async Task ReadsPutAndPutIfAbsentRaceAcrossRemovalAndRecreation()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        var token = deadline.Token;
        await using var fixture = new DataGridClientTlsTests.ClientCluster();
        var leader = await fixture.Cluster.Leader(token);
        var history = new DataGridClientHistory();
        await using var writer = fixture.Client(leader, history);
        await using var creator = fixture.Client(leader, history);
        await using var reader = fixture.Client(leader, history);
        var putSession = await Open(writer, token);
        var createSession = await Open(creator, token);
        await Race(
            fixture,
            history,
            [reader, writer, creator],
            [
                Read(),
                Mutation(putSession),
                Mutation(createSession, kind: DataGridMutationKind.PutIfAbsent, value: 9),
            ],
            token
        );
        var before = await reader.ExecuteAsync(Read(), token);
        var old = before.Reply.Read!.GetItem(0)!;
        await Race(
            fixture,
            history,
            [creator, reader, writer],
            [
                Mutation(createSession, 2, DataGridMutationKind.PutIfAbsent, value: 11),
                Read(),
                Mutation(putSession, 2, DataGridMutationKind.CompareRemove, old.Version),
            ],
            token
        );
        await writer.ExecuteAsync(Mutation(putSession, 3, DataGridMutationKind.Remove), token);
        var recreate = await creator.ExecuteAsync(
            Mutation(createSession, 3, DataGridMutationKind.PutIfAbsent, value: 13),
            token
        );
        Assert.Equal(DataGridMutationStatus.Stored, Item(recreate).Status);
        Assert.NotEqual(old.Version, Item(recreate).Version);
        var stale = await writer.ExecuteAsync(
            Mutation(putSession, 4, DataGridMutationKind.CompareExchange, old.Version),
            token
        );
        Assert.Equal(DataGridMutationStatus.VersionMismatch, Item(stale).Status);
        var read = await reader.ExecuteAsync(Read(), token);
        Assert.Equal(Item(recreate).Version, read.Reply.Read!.GetItem(0)!.Version);
        AssertValid(history);
        var operations = history.GetOperations().ToArray();
        operations[^1] = operations[^1] with { Outcome = before };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check(operations, startsWithEmptySessions: true).Status
        );
        fixture.AssertHealthy();
    }

    [Theory(Timeout = 45000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LostReplyThenSemanticRetryUsesPhysicalAttemptEvidence(bool changedIdentity)
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        var token = deadline.Token;
        var clock = new DataGridSessionPressureTlsTests.LogicalClock();
        await using var fixture = new DataGridClientTlsTests.ClientCluster(timeProvider: clock);
        var leader = await fixture.Cluster.Leader(token);
        var history = new DataGridClientHistory();
        await using var client = fixture.Client(leader, history);
        var session = await Open(client, token, clock);
        var original = Mutation(session);
        if (changedIdentity)
            Assert.Equal(
                DataGridMutationStatus.Stored,
                Item(await client.ExecuteAsync(original, token)).Status
            );
        var request = changedIdentity ? Mutation(session, value: 9) : original;
        var armed = 1;
        fixture.Override = async (node, incoming, cancellation) =>
        {
            var reply = await node.ExecuteAsync(
                DataGridPeerFixture.Principal,
                incoming,
                cancellation
            );
            if (incoming.Mutation is not null && Interlocked.Exchange(ref armed, 0) == 1)
            {
                if (changedIdentity)
                    Assert.Equal(
                        DataGridClientFailureCode.RequestIdentityMismatch,
                        reply.Failure!.Code
                    );
                else
                {
                    Assert.Equal(DataGridMutationStatus.Stored, reply.Result!.GetItem(0).Status);
                    clock.Advance(TimeSpan.FromMinutes(10) + TimeSpan.FromTicks(1));
                    Assert.Null(
                        (
                            await node.ExecuteAsync(
                                DataGridPeerFixture.Principal,
                                Read(),
                                cancellation
                            )
                        ).Failure
                    );
                }
                throw new IOException("Controlled loss of an applied reply before the SDK retry.");
            }
            return reply;
        };
        var outcome = await client.ExecuteAsync(request, token);
        fixture.Override = null;
        Assert.Equal(0, armed);
        Assert.Equal(
            changedIdentity
                ? DataGridClientFailureCode.RequestIdentityMismatch
                : DataGridClientFailureCode.SessionExpired,
            outcome.Reply.Failure!.Code
        );
        Assert.InRange(outcome.Attempts.Count, 2, 3);
        Assert.True(outcome.Attempts[0].RequestSubmitted);
        Assert.Null(outcome.Attempts[0].Reply);
        Assert.Same(outcome.Reply, outcome.Attempts[^1].Reply);
        Assert.All(
            outcome.Attempts,
            attempt =>
            {
                Assert.NotNull(attempt.InvocationOrder);
                Assert.True(attempt.CompletionOrder > attempt.InvocationOrder);
            }
        );
        var read = await client.ExecuteAsync(Read(), token);
        Assert.Equal(new byte[] { 7 }, read.Reply.Read!.GetItem(0)!.GetValue());
        AssertValid(history);
        var operations = history.GetOperations().ToArray();
        var index = Array.FindIndex(
            operations,
            operation => ReferenceEquals(operation.Outcome, outcome)
        );
        if (changedIdentity)
        {
            operations[index] = operations[index] with { Request = original };
            Assert.Equal(
                DataGridHistoryStatus.Invalid,
                DataGridClientHistoryChecker.Check(operations, startsWithEmptySessions: true).Status
            );
        }
        else
        {
            var forged = DataGridClientReply.Failed(
                new(DataGridClientFailureCode.RequestIdentityMismatch, true)
            );
            var attempts = outcome.Attempts.ToArray();
            attempts[^1] = attempts[^1] with { Reply = forged };
            operations[index] = operations[index] with { Outcome = new(forged, true, attempts) };
            Assert.Equal(
                DataGridHistoryStatus.Invalid,
                DataGridClientHistoryChecker.Check(operations, startsWithEmptySessions: true).Status
            );
        }
        var incomplete = history.GetOperations().ToArray();
        incomplete[index] = incomplete[index] with
        {
            Outcome = new(
                outcome.Reply,
                true,
                outcome.Attempts.Select(attempt => attempt with { InvocationOrder = null })
            ),
        };
        Assert.Equal(
            DataGridHistoryStatus.Inconclusive,
            DataGridClientHistoryChecker.Check(incomplete, startsWithEmptySessions: true).Status
        );
        Assert.Equal(
            DataGridHistoryStatus.Inconclusive,
            DataGridClientHistoryChecker.Check(history.GetOperations(), 1, true).Status
        );
        fixture.AssertHealthy();
    }

    private static async Task<DataGridClientOutcome[]> Race(
        DataGridClientTlsTests.ClientCluster fixture,
        DataGridClientHistory history,
        GridClient[] clients,
        DataGridClientRequest[] requests,
        CancellationToken token
    )
    {
        var arrived = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var count = 0;
        fixture.Override = async (node, request, cancellation) =>
        {
            if (Interlocked.Increment(ref count) == requests.Length)
                arrived.TrySetResult();
            await release.Task.WaitAsync(cancellation);
            return await node.ExecuteAsync(DataGridPeerFixture.Principal, request, cancellation);
        };
        var calls = clients
            .Select((client, i) => client.ExecuteAsync(requests[i], token))
            .ToArray();
        try
        {
            await arrived.Task.WaitAsync(token);
            var pending = history
                .GetOperations()
                .Where(operation => operation.CompletionOrder is null)
                .ToArray();
            Assert.Equal(requests.Length, pending.Length);
            Assert.All(calls, call => Assert.False(call.IsCompleted));
            release.TrySetResult();
            var outcomes = await Task.WhenAll(calls).WaitAsync(token);
            Assert.All(outcomes, outcome => Assert.Null(outcome.Reply.Failure));
            var finished = history
                .GetOperations()
                .Where(operation => pending.Any(old => old.OperationId == operation.OperationId))
                .ToArray();
            Assert.True(
                finished.Max(operation => operation.InvocationOrder)
                    < finished.Min(operation => operation.CompletionOrder)
            );
            return outcomes;
        }
        finally
        {
            release.TrySetResult();
            fixture.Override = null;
            await Task.WhenAll(calls).WaitAsync(token);
        }
    }

    private static void RejectChangedDuplicate(DataGridClientHistory history)
    {
        var operations = history.GetOperations().ToArray();
        var duplicate = operations[^1];
        var prior = duplicate.Outcome!;
        var forged = DataGridClientReply.Completed(
            DataGridMessageKind.MutationResult,
            new(
                DataGridApplyStatus.Completed,
                [
                    new(
                        DataGridMutationStatus.Stored,
                        new(DataGridPeerFixture.Cluster, long.MaxValue)
                    ),
                ]
            )
        );
        operations[^1] = duplicate with
        {
            Outcome = new(forged, prior.MayHaveBeenAdmitted, prior.Attempts),
        };
        Assert.Equal(
            DataGridHistoryStatus.Invalid,
            DataGridClientHistoryChecker.Check(operations, startsWithEmptySessions: true).Status
        );
    }

    private static void AssertValid(DataGridClientHistory history)
    {
        var result = DataGridClientHistoryChecker.Check(
            history.GetOperations(),
            startsWithEmptySessions: true
        );
        Assert.True(result.Status == DataGridHistoryStatus.Valid, result.Reason);
    }

    private static DataGridMutationResult Item(DataGridClientOutcome outcome)
    {
        Assert.Null(outcome.Reply.Failure);
        return outcome.Reply.Result!.GetItem(0);
    }

    private static async Task<long> Open(
        GridClient client,
        CancellationToken token,
        TimeProvider? clock = null
    )
    {
        var outcome = await client.ExecuteAsync(
            DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                DataGridTimestamp.FromUtc((clock ?? TimeProvider.System).GetUtcNow().AddSeconds(4))
            ),
            token
        );
        Assert.Null(outcome.Reply.Failure);
        return outcome.Reply.Result!.SessionNumber;
    }

    private static DataGridClientRequest Mutation(
        long session,
        ulong number = 1,
        DataGridMutationKind kind = DataGridMutationKind.Put,
        DataGridEntryVersion expected = default,
        byte value = 7
    ) =>
        DataGridClientRequest.Mutate(
            DataGridClientCodecTests.Mutation(session, number, kind, expected, value)
        );

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(token);
        source.CancelAfter(TimeSpan.FromSeconds(40));
        return source;
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Admission history tests require Linux TLS 1.3."
        );
}
