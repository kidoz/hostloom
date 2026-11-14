using System.Collections.Concurrent;
using System.Diagnostics;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Runtime;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridResourceBoundaryTlsTests
{
    [Theory(Timeout = 60000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task LiveKeyOrByteQuotaRefusesWithoutMutationAndRetainsRefusalAfterRecovery(
        bool byteQuota
    )
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        var token = deadline.Token;
        var a = DataGridPeerFixture.Key;
        var b = DataGridKey.FromUtf8("catalog:us");
        await using var lab = new DataGridSessionPressureTlsTests.Lab(
            1,
            mapKeys: byteQuota ? 4 : 1,
            mapBytes: byteQuota ? a.ByteCount + 4 : 1024
        );
        var leader = await lab.Cluster.Leader(token);
        var client = lab.Client(0, leader);
        var writer = await lab.Open(client, token);
        var editor = await lab.Open(client, token);
        var original = Item(
            await lab.Send(
                client,
                Mutation(writer, 1, DataGridMutationKind.Put, a, [1, 2, 3, 4]),
                token
            )
        );
        Assert.Equal(DataGridMutationStatus.Stored, original.Status);
        var refusedRequest = Mutation(writer, 2, DataGridMutationKind.Put, b, [9]);
        var refused = await lab.Send(client, refusedRequest, token);
        Assert.Equal(DataGridMutationStatus.CapacityExceeded, Item(refused).Status);
        Assert.Single(refused.Attempts);
        await AssertState(a, original.Version, [1, 2, 3, 4], b);
        var replacement = Item(
            await lab.Send(
                client,
                Mutation(editor, 1, DataGridMutationKind.Put, a, [5, 6, 7]),
                token
            )
        );
        Assert.Equal(DataGridMutationStatus.Stored, replacement.Status);
        Assert.NotEqual(original.Version, replacement.Version);
        await AssertState(a, replacement.Version, [5, 6, 7], b);
        await lab.Compact(token);
        await lab.StopAll();
        lab.Cluster.StartAll();
        client = lab.Client(0, await lab.Cluster.Leader(token));
        Assert.Equal(Item(refused), Item(await lab.Send(client, refusedRequest, token)));
        await AssertState(a, replacement.Version, [5, 6, 7], b);
        Assert.Equal(
            DataGridMutationStatus.Removed,
            Item(
                await lab.Send(
                    client,
                    Mutation(editor, 2, DataGridMutationKind.Remove, a, []),
                    token
                )
            ).Status
        );
        var replay = await lab.Send(client, refusedRequest, token);
        Assert.Equal(Item(refused), Item(replay));
        Assert.Single(replay.Attempts);
        var empty = await lab.Send(
            client,
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [a, b]),
            token
        );
        Assert.Null(empty.Reply.Failure);
        Assert.Null(empty.Reply.Read!.GetItem(0));
        Assert.Null(empty.Reply.Read.GetItem(1));
        var fresh = Item(
            await lab.Send(client, Mutation(writer, 3, DataGridMutationKind.Put, b, [8, 9]), token)
        );
        Assert.Equal(DataGridMutationStatus.Stored, fresh.Status);
        await AssertState(b, fresh.Version, [8, 9], a);
        lab.Healthy();

        async Task AssertState(
            DataGridKey present,
            DataGridEntryVersion version,
            byte[] value,
            DataGridKey missing
        )
        {
            var read = await lab.Send(
                client,
                DataGridClientRequest.Read(DataGridPeerFixture.Map, [present, missing]),
                token
            );
            Assert.Null(read.Reply.Failure);
            Assert.Equal(version, read.Reply.Read!.GetItem(0)!.Version);
            Assert.Equal(value, read.Reply.Read.GetItem(0)!.GetValue());
            Assert.Null(read.Reply.Read.GetItem(1));
            await lab.AppliedEverywhere(
                lab.Cluster.Nodes.First(node => node!.Status.Ready)!.Status.AppliedIndex,
                token
            );
            Assert.All(
                lab.Cluster.Nodes,
                node =>
                {
                    var map = node!.Diagnostics.Maps[0];
                    Assert.Equal(1, map.ChargedKeys);
                    Assert.Equal(present.ByteCount + value.Length, map.ChargedBytes);
                }
            );
        }
    }

    [Fact(Timeout = 60000)]
    public async Task RetryStormHonorsSharedReconnectCallAndAttemptBudgetsWithoutChangingIdentity()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        var token = deadline.Token;
        using var capture = new DataGridMeterCapture();
        await using (var fixture = new DataGridClientTlsTests.ClientCluster())
        {
            var leader = await fixture.Cluster.Leader(token);
            await using var opener = fixture.Client(leader);
            var requests = new DataGridClientRequest[8];
            for (var i = 0; i < requests.Length; i++)
            {
                var opened = await opener.ExecuteAsync(
                    DataGridClientRequest.OpenSession(
                        Guid.NewGuid(),
                        DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(4))
                    ),
                    token
                );
                Assert.Null(opened.Reply.Failure);
                requests[i] = Mutation(
                    opened.Reply.Result!.SessionNumber,
                    1,
                    DataGridMutationKind.Put,
                    DataGridPeerFixture.Key,
                    [(byte)i]
                );
            }
            var observed = new ConcurrentQueue<DataGridMutationCommand>();
            var release = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            fixture.Override = async (node, request, cancellation) =>
            {
                if (request.Mutation is not { } mutation)
                    return await node.ExecuteAsync(
                        DataGridPeerFixture.Principal,
                        request,
                        cancellation
                    );
                observed.Enqueue(mutation);
                await release.Task.WaitAsync(cancellation);
                return DataGridClientReply.Failed(
                    new(DataGridClientFailureCode.CapacityExceeded, false)
                );
            };
            var history = new DataGridClientHistory();
            await using var client = fixture.Client(leader, history);
            var started = Stopwatch.GetTimestamp();
            var calls = requests.Select(request => client.ExecuteAsync(request, token)).ToArray();
            DataGridClientOutcome[] outcomes;
            try
            {
                var overflow = await client.ExecuteAsync(
                    DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
                    token
                );
                Assert.Equal(
                    DataGridClientFailureCode.CapacityExceeded,
                    overflow.Reply.Failure!.Code
                );
                Assert.False(overflow.MayHaveBeenAdmitted);
                Assert.Empty(overflow.Attempts);
                release.TrySetResult();
                outcomes = await Task.WhenAll(calls).WaitAsync(token);
            }
            finally
            {
                release.TrySetResult();
            }
            var read = await opener.ExecuteAsync(
                DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
                token
            );
            Assert.Null(read.Reply.Failure);
            Assert.Null(read.Reply.Read!.GetItem(0));
            await Task.WhenAll(fixture.Servers.Select(server => server.DisposeAsync().AsTask()));
            var elapsed = Stopwatch.GetElapsedTime(started);
            Assert.All(
                outcomes,
                outcome =>
                {
                    Assert.InRange(outcome.Attempts.Count, 1, 3);
                    Assert.Equal(
                        outcome.Attempts.Count,
                        outcome.Attempts.Select(attempt => attempt.AttemptId).Distinct().Count()
                    );
                    Assert.Contains(
                        outcome.Reply.Failure!.Code,
                        new[]
                        {
                            DataGridClientFailureCode.CapacityExceeded,
                            DataGridClientFailureCode.QuorumUnavailable,
                            DataGridClientFailureCode.OutcomeUnknown,
                        }
                    );
                    if (outcome.MayHaveBeenAdmitted)
                        Assert.Equal(
                            DataGridClientFailureCode.OutcomeUnknown,
                            outcome.Reply.Failure.Code
                        );
                }
            );
            Assert.Contains(
                outcomes,
                outcome => outcome.Attempts.Any(attempt => !attempt.RequestSubmitted)
            );
            Assert.True(capture.Sum("client.timeouts") > 0);
            Assert.True(observed.Count > 2);
            Assert.True(
                observed.Count <= 2 + Math.Ceiling(elapsed.TotalSeconds * 2),
                FormattableString.Invariant(
                    $"Observed {observed.Count} dispatches in {elapsed.TotalSeconds:F3}s."
                )
            );
            foreach (var command in observed)
            {
                var original = Assert
                    .Single(requests, request => request.SessionNumber == command.SessionNumber)
                    .Mutation!;
                Assert.Equal(original.Encode(), command.Encode());
            }
            Assert.Equal(9, history.GetOperations().Count);
            Assert.All(history.GetOperations(), operation => Assert.NotNull(operation.Outcome));
            TestContext.Current.TestOutputHelper!.WriteLine(
                FormattableString.Invariant(
                    $"Eight calls: {observed.Count} server dispatches, {outcomes.Sum(outcome => outcome.Attempts.Count)} entered attempts, {elapsed.TotalSeconds:F3}s."
                )
            );
            fixture.AssertHealthy();
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 30000)]
    public async Task RedirectCycleStopsAtTwoRedirectsAndThreeAttempts()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        var token = deadline.Token;
        await using var fixture = new DataGridClientTlsTests.ClientCluster();
        var leader = await fixture.Cluster.Leader(token);
        var received = 0;
        fixture.Override = (node, _, _) =>
        {
            Interlocked.Increment(ref received);
            return Task.FromResult(
                DataGridClientReply.Failed(
                    new(
                        DataGridClientFailureCode.Redirect,
                        false,
                        fixture
                            .Peer.Configuration.GetMember((node.Diagnostics.MemberOrdinal + 1) % 3)
                            .MemberId
                    )
                )
            );
        };
        await using var client = fixture.Client(leader);
        var started = Stopwatch.GetTimestamp();
        var outcome = await client.ExecuteAsync(
            DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]),
            token
        );
        Assert.Equal(DataGridClientFailureCode.Redirect, outcome.Reply.Failure!.Code);
        Assert.False(outcome.MayHaveBeenAdmitted);
        Assert.Equal(3, outcome.Attempts.Count);
        Assert.Equal(3, received);
        Assert.All(
            outcome.Attempts,
            attempt =>
                Assert.Equal(DataGridClientFailureCode.Redirect, attempt.Reply!.Failure!.Code)
        );
        Assert.Equal(3, outcome.Attempts.Select(attempt => attempt.MemberId).Distinct().Count());
        Assert.True(received <= 2 + Math.Floor(Stopwatch.GetElapsedTime(started).TotalSeconds * 2));
        fixture.AssertHealthy();
    }

    private static DataGridClientRequest Mutation(
        long session,
        ulong number,
        DataGridMutationKind kind,
        DataGridKey key,
        byte[] value
    ) =>
        DataGridClientRequest.Mutate(
            new(
                kind,
                DataGridPeerFixture.Map,
                session,
                number,
                DataGridExpiryKind.Never,
                0,
                [new(key, value)]
            )
        );

    private static DataGridMutationResult Item(DataGridClientOutcome outcome)
    {
        Assert.Null(outcome.Reply.Failure);
        return outcome.Reply.Result!.GetItem(0);
    }

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var deadline = CancellationTokenSource.CreateLinkedTokenSource(token);
        deadline.CancelAfter(TimeSpan.FromSeconds(50));
        return deadline;
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(OperatingSystem.IsLinux(), "Resource boundary TLS cases require Linux.");
}
