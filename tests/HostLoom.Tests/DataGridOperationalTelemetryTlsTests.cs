using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridOperationalTelemetryTlsTests
{
    [Fact(Timeout = 60000)]
    public async Task LiveCapacityRejectionsExpiryAndRetainedResultsEmitMapAndSessionSignals()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        var token = timeout.Token;
        using var capture = new DataGridMeterCapture();
        await using (var lab = new DataGridSessionPressureTlsTests.Lab(1, mapKeys: 2))
        {
            var leader = await lab.Cluster.Leader(token);
            var client = lab.Client(0, leader);
            var session = await lab.Open(client, token);
            var key = DataGridKey.FromUtf8("catalog:us");
            var filled = await lab.Send(
                client,
                DataGridClientRequest.Mutate(
                    new(
                        DataGridMutationKind.Put,
                        DataGridPeerFixture.Map,
                        session,
                        1,
                        DataGridExpiryKind.Relative,
                        TimeSpan.TicksPerSecond,
                        [
                            new(DataGridPeerFixture.Key, [7]),
                            new(DataGridKey.FromUtf8("catalog:expiry"), [8]),
                        ]
                    )
                ),
                token
            );
            Assert.Null(filled.Reply.Failure);
            var refused = Mutation(session, 2, DataGridMutationKind.Put, key);
            Assert.Equal(
                DataGridMutationStatus.CapacityExceeded,
                Item(await lab.Send(client, refused, token)).Status
            );
            lab.Clock.Advance(TimeSpan.FromSeconds(2));
            await lab.WaitFor(
                () =>
                    lab.Cluster.Nodes.All(node =>
                        node!.Diagnostics.Maps[0].ExpiredReclaimed == 2
                        && node.Diagnostics.Maps[0].ChargedKeys == 0
                    ),
                token
            );
            Assert.Equal(
                DataGridMutationStatus.CapacityExceeded,
                Item(await lab.Send(client, refused, token)).Status
            );
            var stored = Item(
                await lab.Send(client, Mutation(session, 3, DataGridMutationKind.Put, key), token)
            );
            Assert.Equal(DataGridMutationStatus.Stored, stored.Status);
            Assert.Equal(
                DataGridMutationStatus.AlreadyExists,
                Item(
                    await lab.Send(
                        client,
                        Mutation(session, 4, DataGridMutationKind.PutIfAbsent, key),
                        token
                    )
                ).Status
            );
            Assert.Equal(
                DataGridMutationStatus.VersionMismatch,
                Item(
                    await lab.Send(
                        client,
                        Mutation(
                            session,
                            5,
                            DataGridMutationKind.CompareExchange,
                            key,
                            new(DataGridPeerFixture.Cluster, stored.Version.LogIndex + 100)
                        ),
                        token
                    )
                ).Status
            );
            lab.Clock.Advance(TimeSpan.FromMinutes(11));
            var expired = await lab.Send(
                client,
                Mutation(session, 6, DataGridMutationKind.Put, key),
                token
            );
            Assert.Equal(DataGridClientFailureCode.SessionExpired, expired.Reply.Failure!.Code);
            lab.Healthy();
        }
        Assert.Contains(
            capture.Measurements,
            m => m.Name == "hostloom.datagrid.runtime.map.expired.reclaimed" && m.Value == 2
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.map.position"
                && m.Tags[DataGridRuntimeDiagnostics.PositionTag] is "applied"
                && m.Value > 0
        );
        foreach (
            var status in new[]
            {
                "stored",
                "capacity_exceeded",
                "already_exists",
                "version_mismatch",
            }
        )
            Assert.Contains(
                capture.Measurements,
                m =>
                    m.Name == "hostloom.datagrid.runtime.map.mutation.items"
                    && Equals(m.Tags[DataGridRuntimeDiagnostics.OutcomeTag], status)
            );
        Assert.Equal(
            2,
            capture
                .Measurements.Where(m =>
                    m.Name == "hostloom.datagrid.runtime.map.mutation.items"
                    && m.Tags[DataGridRuntimeDiagnostics.OutcomeTag] is "capacity_exceeded"
                )
                .Sum(m => m.Value)
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.results"
                && m.Tags[DataGridRuntimeDiagnostics.OutcomeTag] is "session_expired"
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.capacity.items"
                && m.Tags[DataGridRuntimeDiagnostics.ResourceTag] is "sessions"
                && m.Tags[DataGridRuntimeDiagnostics.MeasureTag] is "limit"
                && m.Value == 4096
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.capacity.bytes"
                && m.Tags[DataGridRuntimeDiagnostics.ResourceTag] is "retained_results"
                && m.Tags[DataGridRuntimeDiagnostics.MeasureTag] is "charged"
                && m.Value > 0
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.map.state"
                && m.Tags[DataGridRuntimeDiagnostics.StateTag] is "stopped"
                && m.Value == 1
        );
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 60000)]
    public async Task QuorumLossSeparatesDeadlinesUnknownAndCanceledCallsFromHistoricalHints()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        var token = timeout.Token;
        using var capture = new DataGridMeterCapture();
        await using (var fixture = new DataGridClientTlsTests.ClientCluster())
        {
            var leader = await fixture.Cluster.Leader(token);
            await using var client = fixture.Client(leader);
            var opened = await client.ExecuteAsync(
                DataGridClientRequest.OpenSession(
                    Guid.NewGuid(),
                    DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(4))
                ),
                token
            );
            Assert.Null(opened.Reply.Failure);
            Assert.Null((await client.ExecuteAsync(Read(), token)).Reply.Failure);
            var runtime = fixture.Cluster.Nodes[leader]!;
            await runtime.WaitForAppliedAsync(runtime.Status.CommitIndex, token);
            var barrier = runtime.Diagnostics.LastSuccessfulReadBarrierIndex;
            Assert.True(barrier > 0);
            foreach (var peer in Enumerable.Range(0, 3).Where(peer => peer != leader))
                await fixture.Cluster.Stop(peer);
            var read = await runtime.ReadAsync(
                DataGridPeerFixture.Map,
                DataGridPeerFixture.Key,
                token
            );
            Assert.Equal(RaftCompletionKind.Unavailable, read.Kind);
            var request = Mutation(
                opened.Reply.Result!.SessionNumber,
                1,
                DataGridMutationKind.Put,
                DataGridPeerFixture.Key
            );
            Assert.Equal(1, capture.Sum("runtime.timeouts"));
            var release = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var finished = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            fixture.Override = async (node, incoming, cancellation) =>
            {
                try
                {
                    var response = await node.ExecuteAsync(
                        DataGridPeerFixture.Principal,
                        incoming,
                        cancellation
                    );
                    await release.Task.WaitAsync(token);
                    return response;
                }
                finally
                {
                    finished.TrySetResult();
                }
            };
            DataGridClientOutcome unknown;
            try
            {
                unknown = await client.ExecuteAsync(request, token);
            }
            finally
            {
                release.TrySetResult();
            }
            await finished.Task.WaitAsync(token);
            fixture.Override = null;
            var actorTimeouts = capture.Sum("runtime.timeouts");
            Assert.Equal(DataGridClientFailureCode.OutcomeUnknown, unknown.Reply.Failure!.Code);
            Assert.True(unknown.MayHaveBeenAdmitted);
            Assert.Equal(barrier, runtime.Diagnostics.LastSuccessfulReadBarrierIndex);
            Assert.True(runtime.Diagnostics.LocalReadyHint);
            Assert.Contains(
                capture.Measurements,
                m => m.Name == "hostloom.datagrid.runtime.replica.lag" && m.Value > 0
            );
            var entered = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            fixture.Override = (node, incoming, cancellation) =>
            {
                var pending = node.ExecuteAsync(
                    DataGridPeerFixture.Principal,
                    incoming,
                    cancellation
                );
                entered.TrySetResult();
                return pending;
            };
            using var canceled = CancellationTokenSource.CreateLinkedTokenSource(token);
            await using var other = fixture.Client(leader);
            var call = other.ExecuteAsync(request, canceled.Token);
            await entered.Task.WaitAsync(token);
            await canceled.CancelAsync();
            Assert.Equal(
                DataGridClientFailureCode.OutcomeUnknown,
                (await call).Reply.Failure!.Code
            );
            Assert.Equal(1, capture.Sum("client.timeouts"));
            Assert.Equal(actorTimeouts, capture.Sum("runtime.timeouts"));
        }
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.client.timeouts"
                && m.Tags[DataGridClientDiagnostics.AdmissionTag] is true
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.operations"
                && m.Tags[DataGridRuntimeDiagnostics.OutcomeTag] is "unavailable"
        );
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static DataGridClientRequest Mutation(
        long session,
        ulong number,
        DataGridMutationKind kind,
        DataGridKey key,
        DataGridEntryVersion expected = default
    ) =>
        DataGridClientRequest.Mutate(
            new(
                kind,
                DataGridPeerFixture.Map,
                session,
                number,
                DataGridExpiryKind.Never,
                0,
                [new(key, [9], expected)]
            )
        );

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

    private static DataGridMutationResult Item(DataGridClientOutcome outcome)
    {
        Assert.Null(outcome.Reply.Failure);
        return outcome.Reply.Result!.GetItem(0);
    }

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(50));
        return timeout;
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Operational telemetry TLS cases require Linux."
        );
}
