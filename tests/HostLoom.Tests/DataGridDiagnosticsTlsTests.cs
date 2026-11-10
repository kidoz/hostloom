using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridDiagnosticsTlsTests
{
    [Fact(Timeout = 30000)]
    public async Task LiveSdkMetricsSeparateUncertainCallsFromCommittedResultsAndRetainedRetry()
    {
        RequireLinux();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        timeout.CancelAfter(TimeSpan.FromSeconds(25));
        using var capture = new DataGridMeterCapture();
        await using (var fixture = new DataGridClientTlsTests.ClientCluster())
        {
            var leader = await fixture.Cluster.Leader(timeout.Token);
            await using var client = fixture.Client(leader);
            var opened = await client.ExecuteAsync(
                DataGridClientRequest.OpenSession(
                    Guid.NewGuid(),
                    DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(4))
                ),
                timeout.Token
            );
            Assert.Null(opened.Reply.Failure);
            var entered = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var release = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            DataGridEntryVersion original = default;
            fixture.Override = async (node, request, token) =>
            {
                var reply = await node.ExecuteAsync(DataGridPeerFixture.Principal, request, token);
                if (
                    request.Kind == DataGridMessageKind.Mutation
                    && reply.Result?.GetItem(0).Status == DataGridMutationStatus.Stored
                )
                {
                    original = reply.Result.GetItem(0).Version;
                    entered.TrySetResult();
                    await release.Task.WaitAsync(timeout.Token);
                }
                return reply;
            };
            var mutation = DataGridClientRequest.Mutate(
                DataGridClientCodecTests.Mutation(opened.Reply.Result!.SessionNumber)
            );
            var pending = client.ExecuteAsync(mutation, timeout.Token);
            try
            {
                await entered.Task.WaitAsync(timeout.Token);
                client.Disable();
                var uncertain = await pending;
                Assert.Equal(
                    DataGridClientFailureCode.OutcomeUnknown,
                    uncertain.Reply.Failure!.Code
                );
                Assert.True(uncertain.MayHaveBeenAdmitted);
            }
            finally
            {
                release.TrySetResult();
            }
            fixture.Override = null;
            await using var recovered = fixture.Client(leader);
            var retry = await recovered.ExecuteAsync(mutation, timeout.Token);
            Assert.Null(retry.Reply.Failure);
            Assert.Equal(original, retry.Reply.Result!.GetItem(0).Version);
            Assert.Null(
                (
                    await recovered.ExecuteAsync(
                        DataGridClientRequest.Read(
                            DataGridPeerFixture.Map,
                            [DataGridPeerFixture.Key]
                        ),
                        timeout.Token
                    )
                )
                    .Reply
                    .Failure
            );
            fixture.AssertHealthy();
        }
        var calls = capture
            .Measurements.Where(m => m.Name == "hostloom.datagrid.client.calls")
            .ToArray();
        Assert.Equal(4, calls.Length);
        Assert.Single(
            calls,
            m =>
                m.Tags.GetValueOrDefault(DataGridClientDiagnostics.OutcomeTag) is "outcome_unknown"
                && m.Tags.GetValueOrDefault(DataGridClientDiagnostics.AdmissionTag) is true
        );
        Assert.Equal(
            3,
            calls.Count(m =>
                m.Tags.GetValueOrDefault(DataGridClientDiagnostics.OutcomeTag) is "success"
            )
        );
        Assert.Equal(
            4,
            capture.Measurements.Count(m => m.Name == "hostloom.datagrid.client.call.duration")
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.operation.duration"
                && m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.OperationTag) is "read"
                && m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.OutcomeTag) is "read"
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.persistence.duration"
                && m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.OutcomeTag) is "persisted"
        );
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StalledPersistenceKeepsClientQueueBoundedAndShutdownOrFaultDrainsIt(
        bool fault
    )
    {
        RequireLinux();
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        timeout.CancelAfter(TimeSpan.FromSeconds(25));
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        var directory = Path.Combine(
            Path.GetTempPath(),
            "hostloom-grid-telemetry-" + Guid.NewGuid().ToString("N")
        );
        Directory.CreateDirectory(directory);
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        try
        {
            using (var bootstrap = DataGridFileStore.Bootstrap(directory, fixture.Configuration, 0))
            { }
            using var store = DataGridFileStore.Open(
                directory,
                fixture.Configuration,
                0,
                observe: step =>
                {
                    if (step != DataGridStorageStep.WalWritten)
                        return;
                    entered.TrySetResult();
                    release.Wait(timeout.Token);
                    if (fault)
                        throw new IOException("catalog confidential injected filesystem failure");
                }
            );
            await using var runtime = new DataGridPeerRuntime(
                store,
                fixture.Credentials(0),
                fixture.Listeners[0],
                DataGridPeerFixture.Routes,
                1
            );
            var blocked = runtime.ObserveLeaderAsync(10, timeout.Token);
            Task[] queued = [];
            try
            {
                await entered.Task.WaitAsync(timeout.Token);
                queued = Enumerable
                    .Range(0, 128)
                    .Select(_ =>
                        runtime.ReadAsync(
                            DataGridPeerFixture.Map,
                            DataGridPeerFixture.Key,
                            timeout.Token
                        )
                    )
                    .ToArray();
                Assert.Equal(128, capture.Sum("runtime.queue.items", "client"));
                Assert.Equal(128 * 128, capture.Sum("runtime.queue.bytes", "client"));
                var overflow = await Record.ExceptionAsync(() =>
                    runtime.ReadAsync(
                        DataGridPeerFixture.Map,
                        DataGridPeerFixture.Key,
                        timeout.Token
                    )
                );
                Assert.IsType<IOException>(overflow);
                Task? shutdown = fault ? null : runtime.DisposeAsync().AsTask();
                release.Set();
                await Record.ExceptionAsync(() => blocked);
                foreach (var task in queued)
                    Assert.NotNull(await Record.ExceptionAsync(() => task));
                if (shutdown is not null)
                    await shutdown;
                else
                    await runtime.DisposeAsync();
                Assert.True(runtime.Status.Stopped);
                Assert.NotNull(
                    await Record.ExceptionAsync(() =>
                        runtime.ReadAsync(
                            DataGridPeerFixture.Map,
                            DataGridPeerFixture.Key,
                            timeout.Token
                        )
                    )
                );
            }
            finally
            {
                release.Set();
            }
        }
        finally
        {
            Directory.Delete(directory, recursive: true);
        }
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.queue.rejected"
                && m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.LaneTag) is "client"
                && m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.ReasonTag) is "items"
        );
        Assert.Contains(
            capture.Measurements,
            m =>
                m.Name == "hostloom.datagrid.runtime.persistence.duration"
                && Equals(
                    m.Tags.GetValueOrDefault(DataGridRuntimeDiagnostics.OutcomeTag),
                    fault ? "faulted" : "canceled"
                )
        );
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS 1.3 server evidence requires Linux."
        );
}
