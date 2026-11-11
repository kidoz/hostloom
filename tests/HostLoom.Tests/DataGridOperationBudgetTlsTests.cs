using System.Net.Sockets;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridOperationBudgetTlsTests
{
    [Fact(Timeout = 30000)]
    public async Task HigherTermEndsPendingOwnershipWithoutClaimingRollbackAndAllowsImmediateRefusalReuse()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        await using (var lab = new DataGridClientTlsTests.ClientCluster())
        {
            var leader = await lab.Cluster.Leader(timeout.Token);
            var runtime = lab.Cluster.Nodes[leader]!;
            foreach (var follower in Enumerable.Range(0, 3).Where(i => i != leader))
                await lab.Cluster.Stop(follower);
            using var caller = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
            var waiting = Enumerable
                .Range(0, 8)
                .Select(_ =>
                    runtime.ExecuteAsync(DataGridPeerFixture.Principal, Read(), caller.Token)
                )
                .ToArray();
            await capture.WaitFor(
                _ =>
                    capture.Sum("runtime.queue.items", "client") == 0
                    && capture.Sum("runtime.admission.active") == 8,
                timeout.Token
            );
            await caller.CancelAsync();
            Assert.All(
                await Task.WhenAll(waiting),
                reply => Assert.True(reply.Failure!.MayHaveBeenAdmitted)
            );
            Assert.Equal(8, capture.Sum("runtime.admission.active"));
            await runtime.ObserveLeaderAsync(runtime.Status.Term + 1, timeout.Token);
            Assert.Equal(0, capture.Sum("runtime.admission.active"));
            Assert.Equal(0, capture.Sum("runtime.admission.bytes"));
            var unavailable = await runtime.ExecuteAsync(
                DataGridPeerFixture.Principal,
                Read(),
                timeout.Token
            );
            Assert.Equal(DataGridClientFailureCode.QuorumUnavailable, unavailable.Failure!.Code);
            Assert.False(unavailable.Failure.MayHaveBeenAdmitted);
            Assert.Equal(0, capture.Sum("runtime.admission.active"));
            Assert.Null(runtime.Failure);
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory(Timeout = 30000)]
    [InlineData(DataGridMessageKind.Read)]
    [InlineData(DataGridMessageKind.Mutation)]
    [InlineData(DataGridMessageKind.OpenSession)]
    [InlineData(DataGridMessageKind.CloseSession)]
    public async Task CanceledPendingReadsRetainPrincipalShareAcrossWireRequestsUntilQuorumCompletion(
        DataGridMessageKind kind
    )
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        await using (var lab = new DataGridClientTlsTests.ClientCluster())
        {
            var leader = await lab.Cluster.Leader(timeout.Token);
            var runtime = lab.Cluster.Nodes[leader]!;
            var followers = Enumerable.Range(0, 3).Where(i => i != leader).ToArray();
            foreach (var follower in followers)
                await lab.Cluster.Stop(follower);
            using var caller = CancellationTokenSource.CreateLinkedTokenSource(timeout.Token);
            var waiting = Enumerable
                .Range(0, 8)
                .Select(_ =>
                    runtime.ExecuteAsync(DataGridPeerFixture.Principal, Read(), caller.Token)
                )
                .ToArray();
            await capture.WaitFor(
                _ =>
                    capture.Sum("runtime.queue.items", "client") == 0
                    && capture.Sum("runtime.admission.active") == 8,
                timeout.Token
            );
            await caller.CancelAsync();
            Assert.All(
                await Task.WhenAll(waiting),
                reply => Assert.True(reply.Failure!.MayHaveBeenAdmitted)
            );
            Assert.Equal(0, capture.Sum("runtime.operations.active"));
            Assert.Equal(8, capture.Sum("runtime.admission.active"));
            var request = kind switch
            {
                DataGridMessageKind.Read => Read(),
                DataGridMessageKind.Mutation => DataGridClientRequest.Mutate(
                    DataGridClientCodecTests.Mutation(1)
                ),
                DataGridMessageKind.OpenSession => DataGridClientRequest.OpenSession(
                    Guid.NewGuid(),
                    DataGridTimestamp.FromUtc(DateTimeOffset.UtcNow.AddSeconds(4))
                ),
                _ => DataGridClientRequest.CloseSession(1, 1),
            };
            using (var tcp = new TcpClient())
            await using (var tls = await lab.Hello(leader, tcp, timeout.Token))
            {
                var frame = DataGridClientCodec.EncodeRequest(
                    request,
                    lab.Peer.Configuration.IncarnationId,
                    DataGridPeerFixture.Principal,
                    Guid.NewGuid()
                );
                await DataGridTlsSupport.WriteAsync(tls, frame, timeout.Token);
                var response = await DataGridTlsSupport.ReadAsync(
                    tls,
                    lab.Peer.Configuration.IncarnationId,
                    lab.Peer.Configuration.GetMember(leader).MemberId,
                    timeout.Token
                );
                var refused = DataGridClientCodec.DecodeReply(
                    response,
                    request,
                    frame.AttemptId,
                    lab.Peer.Configuration
                );
                Assert.Equal(DataGridClientFailureCode.CapacityExceeded, refused.Failure!.Code);
                Assert.False(refused.Failure.MayHaveBeenAdmitted);
            }
            await using var other = lab.Client(leader, otherWriter: true);
            var otherCall = other.ExecuteAsync(Read(), timeout.Token);
            await capture.WaitFor(_ => capture.Sum("runtime.admission.active") == 9, timeout.Token);
            lab.Cluster.Start(followers[0]);
            Assert.Null((await otherCall).Reply.Failure);
            await capture.WaitFor(_ => capture.Sum("runtime.admission.active") == 0, timeout.Token);
            for (var i = 0; i < 16; i++)
            {
                Assert.Null(
                    (
                        await runtime.ExecuteAsync(
                            DataGridPeerFixture.Principal,
                            Read(),
                            timeout.Token
                        )
                    ).Failure
                );
                Assert.Equal(0, capture.Sum("runtime.admission.active"));
            }
            Assert.Null(runtime.Failure);
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory(Timeout = 30000)]
    [InlineData("recover")]
    [InlineData("stop")]
    [InlineData("fault")]
    public async Task PhysicalCallerTimeoutCannotRefundExecutingAndQueuedWorkBeforeOwnerReturns(
        string exit
    )
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var release = new ManualResetEventSlim();
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var member = -1;
        var armed = 0;
        await using (
            var lab = new DataGridClientTlsTests.ClientCluster(
                (node, step) =>
                {
                    if (
                        step != DataGridStorageStep.WalWritten
                        || node != Volatile.Read(ref member)
                        || Interlocked.Exchange(ref armed, 0) == 0
                    )
                        return;
                    entered.TrySetResult();
                    release.Wait(timeout.Token);
                    if (exit == "fault")
                        throw new IOException("Controlled persistence failure.");
                }
            )
        )
        {
            try
            {
                var leader = await lab.Cluster.Leader(timeout.Token);
                var runtime = lab.Cluster.Nodes[leader]!;
                var followers = Enumerable.Range(0, 3).Where(i => i != leader).ToArray();
                foreach (var follower in followers)
                    await lab.Cluster.Stop(follower);
                Volatile.Write(ref member, leader);
                Volatile.Write(ref armed, 1);
                var first = runtime.ExecuteAsync(
                    DataGridPeerFixture.Principal,
                    Read(),
                    timeout.Token
                );
                await entered.Task.WaitAsync(timeout.Token);
                var waiting = Enumerable
                    .Range(0, 7)
                    .Select(_ =>
                        runtime.ExecuteAsync(DataGridPeerFixture.Principal, Read(), timeout.Token)
                    )
                    .Prepend(first)
                    .ToArray();
                Assert.All(
                    await Task.WhenAll(waiting),
                    reply =>
                    {
                        Assert.Equal(
                            DataGridClientFailureCode.QuorumUnavailable,
                            reply.Failure!.Code
                        );
                        Assert.True(reply.Failure.MayHaveBeenAdmitted);
                    }
                );
                Assert.Equal(0, capture.Sum("runtime.operations.active"));
                Assert.Equal(7, capture.Sum("runtime.queue.items", "client"));
                Assert.Equal(8, capture.Sum("runtime.admission.active"));
                Assert.True(
                    capture.Sum("runtime.admission.bytes")
                        > capture.Sum("runtime.queue.bytes", "client")
                );
                var refused = await runtime.ExecuteAsync(
                    DataGridPeerFixture.Principal,
                    Read(),
                    timeout.Token
                );
                Assert.Equal(DataGridClientFailureCode.CapacityExceeded, refused.Failure!.Code);
                Assert.False(refused.Failure.MayHaveBeenAdmitted);
                var other = runtime.ExecuteAsync(Guid.NewGuid(), Read(), timeout.Token);
                Assert.Equal(9, capture.Sum("runtime.admission.active"));
                Task? stopped = null;
                if (exit == "stop")
                {
                    stopped = runtime.DisposeAsync().AsTask();
                    Assert.Equal(9, capture.Sum("runtime.admission.active"));
                    var stoppedReply = await runtime.ExecuteAsync(
                        DataGridPeerFixture.Principal,
                        Read(),
                        timeout.Token
                    );
                    Assert.Equal(
                        DataGridClientFailureCode.QuorumUnavailable,
                        stoppedReply.Failure!.Code
                    );
                    Assert.False(stoppedReply.Failure.MayHaveBeenAdmitted);
                    Assert.Equal(9, capture.Sum("runtime.admission.active"));
                    Assert.Throws<IOException>(() =>
                        DataGridFileStore.Open(
                            lab.Cluster.Paths[leader],
                            lab.Peer.Configuration,
                            leader
                        )
                    );
                }
                release.Set();
                if (exit == "recover")
                    lab.Cluster.Start(followers[0]);
                else
                    await (stopped ?? runtime.Completion).WaitAsync(timeout.Token);
                var result = await other;
                if (exit == "recover")
                {
                    Assert.Null(result.Failure);
                    Assert.Null(runtime.Failure);
                }
                else
                {
                    Assert.True(result.Failure!.MayHaveBeenAdmitted);
                    Assert.True(runtime.Diagnostics.Storage.Closed);
                    if (exit == "fault")
                        Assert.NotNull(runtime.Failure);
                    using var recovered = DataGridFileStore.Open(
                        lab.Cluster.Paths[leader],
                        lab.Peer.Configuration,
                        leader
                    );
                    Assert.Equal(0, recovered.RestoreApplication().SessionCount);
                }
                await capture.WaitFor(
                    _ =>
                        capture.Sum("runtime.admission.active") == 0
                        && capture.Sum("runtime.admission.bytes") == 0
                        && capture.Sum("runtime.queue.items", "client") == 0,
                    timeout.Token
                );
                Assert.Equal(0, capture.Sum("runtime.admission.bytes"));
                Assert.Equal(0, capture.Sum("runtime.queue.items", "client"));
            }
            finally
            {
                release.Set();
            }
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS 1.3 operation ownership evidence requires Linux."
        );

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(token);
        source.CancelAfter(TimeSpan.FromSeconds(25));
        return source;
    }
}
