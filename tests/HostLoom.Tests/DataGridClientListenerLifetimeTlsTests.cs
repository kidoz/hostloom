using System.Net;
using System.Net.Sockets;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Transport;
using Xunit;
using GridClient = HostLoom.DataGrid.Client.DataGridClient;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridClientListenerLifetimeTlsTests
{
    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StoppedClientListenerCannotRebindAndReplacementPreservesPeerProgress(
        bool started
    )
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        await using (var lab = new DataGridCredentialTlsTests.Lab())
        {
            var leader = await lab.Cluster.Leader(timeout.Token);
            var follower = (leader + 1) % 3;
            await lab.Servers[follower].DisposeAsync();
            using var listener = Listener(lab, follower);
            var candidate = lab.CreateServer(follower, listener);
            lab.Servers[follower] = candidate;
            try
            {
                if (started)
                    candidate.Start();
                var stopped = candidate.DisposeAsync().AsTask();
                Assert.Same(stopped, candidate.DisposeAsync().AsTask());
                await stopped.WaitAsync(timeout.Token);
                Assert.Throws<ObjectDisposedException>(candidate.Start);
                AssertEndpointFree(lab, follower);
                await using var writer = new GridClient(lab.Options(leader));
                var session = await Open(writer, timeout.Token);
                var version = Stored(
                    await writer.ExecuteAsync(
                        DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
                        timeout.Token
                    )
                );
                await AppliedEverywhere(lab, version.LogIndex, timeout.Token);
                Assert.Equal(1, lab.Security[follower].Generation);
                using var replacementListener = Listener(lab, follower);
                var replacement = lab.CreateServer(follower, replacementListener);
                lab.Servers[follower] = replacement;
                replacement.Start();
                await candidate.DisposeAsync(); // Old disposal must not close the replacement's endpoint.
                await using var reader = new GridClient(lab.Options(follower));
                Assert.Equal(
                    version,
                    (await reader.ExecuteAsync(Read(), timeout.Token))
                        .Reply.Read!.GetItem(0)!
                        .Version
                );
                lab.Healthy();
                await replacement.DisposeAsync();
            }
            finally
            {
                // Also releases the endpoint when the old implementation incorrectly permits a late Start.
                listener.Stop();
            }
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 30000)]
    public async Task BindFailureCanRetrySameListenerWhileSharedPeerRuntimeKeepsReplicating()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        await using (var lab = new DataGridCredentialTlsTests.Lab())
        {
            var leader = await lab.Cluster.Leader(timeout.Token);
            var follower = (leader + 1) % 3;
            await lab.Servers[follower].DisposeAsync();
            using var blocker = Listener(lab, follower);
            blocker.Start();
            using var listener = Listener(lab, follower);
            var candidate = lab.CreateServer(follower, listener);
            lab.Servers[follower] = candidate;
            Assert.Throws<SocketException>(candidate.Start);
            using (var probe = Listener(lab, follower))
                Assert.Throws<SocketException>(() => probe.Start());
            await using var writer = new GridClient(lab.Options(leader));
            var session = await Open(writer, timeout.Token);
            var version = Stored(
                await writer.ExecuteAsync(
                    DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session)),
                    timeout.Token
                )
            );
            await AppliedEverywhere(lab, version.LogIndex, timeout.Token);
            Assert.Equal(1, lab.Security[follower].Generation);
            blocker.Stop();
            candidate.Start();
            Assert.Throws<InvalidOperationException>(candidate.Start);
            await using var reader = new GridClient(lab.Options(follower));
            Assert.Equal(
                version,
                (await reader.ExecuteAsync(Read(), timeout.Token)).Reply.Read!.GetItem(0)!.Version
            );
            await candidate.DisposeAsync();
            await candidate.DisposeAsync();
            AssertEndpointFree(lab, follower);
            Assert.Equal(
                version,
                (await writer.ExecuteAsync(Read(), timeout.Token)).Reply.Read!.GetItem(0)!.Version
            );
            lab.Healthy();
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory(Timeout = 30000)]
    [InlineData(true)]
    [InlineData(false)]
    public async Task ClientStopDrainsReceiverWithoutStoppingSharedQuorumAndRetryRetainsOriginalResult(
        bool cooperative
    )
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        await using (var lab = new DataGridCredentialTlsTests.Lab())
        {
            var leader = await lab.Cluster.Leader(timeout.Token);
            var runtime = lab.Cluster.Nodes[leader]!;
            await using var client = new GridClient(lab.Options(leader));
            var session = await Open(client, timeout.Token);
            await capture.WaitFor(_ => capture.Reserved("request", "client") == 0, timeout.Token);
            var entered = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var canceled = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var exited = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            var release = new TaskCompletionSource(
                TaskCreationOptions.RunContinuationsAsynchronously
            );
            DataGridEntryVersion original = default;
            lab.Hook = async (member, principal, request, token) =>
            {
                var result = await lab.Cluster.Nodes[member]!.ExecuteAsync(
                    principal,
                    request,
                    token
                );
                if (member == leader && request.Mutation is not null)
                {
                    original = result.Result!.GetItem(0).Version;
                    using var registration = token.Register(() => canceled.TrySetResult());
                    entered.TrySetResult();
                    try
                    {
                        await release.Task.WaitAsync(cooperative ? token : timeout.Token);
                    }
                    finally
                    {
                        // WaitAsync cancellation may finish before an earlier token callback runs.
                        if (token.IsCancellationRequested)
                            canceled.TrySetResult();
                        exited.TrySetResult();
                    }
                }
                return result;
            };
            var logical = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session));
            var call = client.ExecuteAsync(logical, timeout.Token);
            try
            {
                await entered.Task.WaitAsync(timeout.Token);
                Assert.Equal(1, capture.Reserved("request", "client"));
                client.Disable();
                var unknown = await call;
                Assert.Equal(DataGridClientFailureCode.OutcomeUnknown, unknown.Reply.Failure!.Code);
                Assert.True(unknown.MayHaveBeenAdmitted);
                Assert.Single(unknown.Attempts);
                var stopped = lab.Servers[leader].DisposeAsync().AsTask();
                Assert.Same(stopped, lab.Servers[leader].DisposeAsync().AsTask());
                await canceled.Task.WaitAsync(timeout.Token);
                if (!cooperative)
                {
                    Assert.False(stopped.IsCompleted);
                    Assert.False(exited.Task.IsCompleted);
                    Assert.Equal(1, capture.Reserved("request", "client"));
                }
                // This fresh read barrier needs the live majority, independently of the stopped/draining client listener.
                var peerRead = await runtime.ReadAsync(
                    DataGridPeerFixture.Map,
                    DataGridPeerFixture.Key,
                    timeout.Token
                );
                Assert.Equal(RaftCompletionKind.Read, peerRead.Kind);
                Assert.Equal(original, peerRead.Entry!.Version);
                Assert.Null(runtime.Failure);
                Assert.Equal(1, lab.Security[leader].Generation);
                release.TrySetResult();
                await stopped.WaitAsync(timeout.Token);
                await exited.Task.WaitAsync(timeout.Token);
                Assert.Equal(0, capture.Reserved("request", "client"));
                Assert.Equal(0, capture.Reserved("connection", "client"));
                Assert.Equal(0, capture.Reserved("handshake", "client"));
                AssertEndpointFree(lab, leader);
                lab.Hook = null;
                using var listener = Listener(lab, leader);
                var replacement = lab.CreateServer(leader, listener);
                lab.Servers[leader] = replacement;
                replacement.Start();
                await using var retry = new GridClient(lab.Options(leader));
                Assert.Equal(original, Stored(await retry.ExecuteAsync(logical, timeout.Token)));
                Assert.Equal(
                    original,
                    (await retry.ExecuteAsync(Read(), timeout.Token))
                        .Reply.Read!.GetItem(0)!
                        .Version
                );
                await AppliedEverywhere(lab, original.LogIndex, timeout.Token);
                lab.Healthy();
                await replacement.DisposeAsync();
            }
            finally
            {
                release.TrySetResult();
                await Record.ExceptionAsync(async () => await call);
            }
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static TcpListener Listener(DataGridCredentialTlsTests.Lab lab, int member) =>
        new(IPAddress.Loopback, lab.Ports[member]);

    private static void AssertEndpointFree(DataGridCredentialTlsTests.Lab lab, int member)
    {
        using var probe = Listener(lab, member);
        probe.Start();
    }

    private static async Task AppliedEverywhere(
        DataGridCredentialTlsTests.Lab lab,
        long index,
        CancellationToken token
    )
    {
        foreach (var node in lab.Cluster.Nodes)
            await node!.WaitForAppliedAsync(index, token);
    }

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

    private static DataGridEntryVersion Stored(DataGridClientOutcome outcome)
    {
        Assert.Null(outcome.Reply.Failure);
        Assert.Equal(DataGridMutationStatus.Stored, outcome.Reply.Result!.GetItem(0).Status);
        return outcome.Reply.Result.GetItem(0).Version;
    }

    private static async Task<long> Open(GridClient client, CancellationToken token)
    {
        var result = await client.ExecuteAsync(
            DataGridClientRequest.OpenSession(
                Guid.NewGuid(),
                new(DateTimeOffset.UtcNow.AddSeconds(4).Ticks)
            ),
            token
        );
        Assert.Null(result.Reply.Failure);
        return result.Reply.Result!.SessionNumber;
    }

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(token);
        source.CancelAfter(TimeSpan.FromSeconds(25));
        return source;
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Shared client-listener lifecycle checks require Linux."
        );
}
