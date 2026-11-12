using System.Net;
using System.Net.Sockets;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Storage;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridStartupLifetimeTlsTests
{
    [Theory(Timeout = 25000)]
    [InlineData("member")]
    [InlineData("configuration")]
    [InlineData("routes")]
    [InlineData("port")]
    public async Task FailedSharedStartupReleasesTransferredResourcesAndPreservesExternalSecurity(
        string reason
    )
    {
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "File-backed TLS startup checks require Linux."
        );
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(
            TestContext.Current.CancellationToken
        );
        timeout.CancelAfter(TimeSpan.FromSeconds(20));
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        var member = reason == "member" ? 1 : 0;
        var credentials =
            reason == "configuration"
                ? new DataGridPeerCredentials(
                    new DataGridBootstrapConfiguration(
                        Guid.NewGuid(),
                        Enumerable.Range(0, 3).Select(fixture.Configuration.GetMember).ToArray(),
                        Enumerable
                            .Range(0, fixture.Configuration.MapCount)
                            .Select(fixture.Configuration.GetMap)
                            .ToArray()
                    ),
                    0,
                    fixture.Certificates[0],
                    fixture.Root,
                    fixture.Fingerprints,
                    fixture.Build
                )
                : fixture.Credentials(member);
        await using var shared = new DataGridSecurityContext(credentials);
        await using var corrected =
            member != 0 || reason == "configuration"
                ? new DataGridSecurityContext(fixture.Credentials(0))
                : null;
        var retrySecurity = corrected ?? shared;
        var path = Path.Combine(
            Path.GetTempPath(),
            "hostloom-startup-" + Guid.NewGuid().ToString("N")
        );
        Directory.CreateDirectory(path);
        using var attempted =
            reason == "port"
                ? new TcpListener(IPAddress.Loopback, fixture.Configuration.GetMember(0).Port)
                : fixture.Listeners[0];
        try
        {
            Guid generation;
            using (var initial = DataGridFileStore.Bootstrap(path, fixture.Configuration, 0))
                generation = initial.GenerationId;
            using (var store = DataGridFileStore.Open(path, fixture.Configuration, 0))
            {
                var failure = Record.Exception(() =>
                    new DataGridPeerRuntime(
                        store,
                        fixture.Credentials(0),
                        attempted,
                        reason == "routes"
                            ? DataGridPeerFixture.Routes[..2]
                            : DataGridPeerFixture.Routes,
                        1,
                        new FrozenClock(),
                        shared
                    )
                );
                if (reason == "port")
                    Assert.IsType<SocketException>(failure);
                else
                    Assert.IsType<ArgumentException>(failure);
                Assert.True(store.Progress.Closed);
            }
            Assert.Equal(1, shared.Generation);
            AssertSigns(shared, fixture, member);
            using var endpointProbe = new TcpListener(
                IPAddress.Loopback,
                fixture.Configuration.GetMember(0).Port
            );
            if (reason == "port")
            {
                // The separately owned listener that caused the conflict must stay bound.
                Assert.Throws<SocketException>(() => endpointProbe.Start());
                fixture.Listeners[0].Stop();
            }
            endpointProbe.Start(); // Failed startup must release the transferred listener, too.
            endpointProbe.Stop();
            using (var reopened = DataGridFileStore.Open(path, fixture.Configuration, 0))
            {
                Assert.Equal(generation, reopened.GenerationId);
                Assert.Equal(0, reopened.Image.Term);
                Assert.Equal(0, reopened.Image.CommitIndex);
                using var listener = new TcpListener(
                    IPAddress.Loopback,
                    fixture.Configuration.GetMember(0).Port
                );
                await using var runtime = new DataGridPeerRuntime(
                    reopened,
                    fixture.Credentials(0),
                    listener,
                    DataGridPeerFixture.Routes,
                    1,
                    new FrozenClock(),
                    retrySecurity
                );
                await using (
                    var sender = new DataGridPeerTransport(
                        fixture.Credentials(1),
                        fixture.Listeners[1],
                        DataGridPeerFixture.Routes,
                        new Receiver()
                    )
                )
                {
                    var response = await sender.SendAsync(
                        new RaftMessage(
                            RaftMessageKind.VoteRequest,
                            1,
                            0,
                            10,
                            new(1, 1),
                            fixture.Configuration.IncarnationId,
                            fixture.Configuration.ComputeDigest()
                        ),
                        timeout.Token
                    );
                    Assert.True(response.Accepted);
                    Assert.Equal(10, response.Term);
                }
                Assert.Null(runtime.Failure);
                var stopped = runtime.DisposeAsync().AsTask();
                Assert.Same(stopped, runtime.DisposeAsync().AsTask());
                await stopped.WaitAsync(timeout.Token);
                await runtime.Completion.WaitAsync(timeout.Token);
                Assert.True(runtime.Diagnostics.Storage.Closed);
                Assert.Equal(0, runtime.ActiveConnections);
                AssertSigns(retrySecurity, fixture, 0);
            }
            // Restart again after a successful stop, retaining the same selected generation and term.
            using (var recovered = DataGridFileStore.Open(path, fixture.Configuration, 0))
            {
                Assert.Equal(generation, recovered.GenerationId);
                Assert.Equal(10, recovered.Image.Term);
                Assert.Equal(0, recovered.Image.CommitIndex);
                using var listener = new TcpListener(
                    IPAddress.Loopback,
                    fixture.Configuration.GetMember(0).Port
                );
                await using var restarted = new DataGridPeerRuntime(
                    recovered,
                    fixture.Credentials(0),
                    listener,
                    DataGridPeerFixture.Routes,
                    2,
                    new FrozenClock(),
                    retrySecurity
                );
                Assert.Equal(10, await restarted.ObserveLeaderAsync(0, timeout.Token));
                await restarted.DisposeAsync();
                Assert.Null(restarted.Failure);
            }
            endpointProbe.Start();
            endpointProbe.Stop();
            AssertSigns(shared, fixture, member);
            await shared.Reload(credentials).Retired.WaitAsync(timeout.Token);
            Assert.Equal(2, shared.Generation);
            AssertSigns(shared, fixture, member);
        }
        finally
        {
            attempted.Stop();
            Directory.Delete(path, true);
        }
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static void AssertSigns(
        DataGridSecurityContext security,
        DataGridPeerFixture fixture,
        int member
    )
    {
        using var lease = security.Acquire();
        using var signer = lease.Material.Member.Certificate.GetECDsaPrivateKey();
        using var verifier = fixture.Certificates[member].GetECDsaPublicKey();
        Assert.NotNull(signer);
        Assert.NotNull(verifier);
        var signature = signer.SignData("catalog startup"u8, HashAlgorithmName.SHA256);
        Assert.True(verifier.VerifyData("catalog startup"u8, signature, HashAlgorithmName.SHA256));
    }

    // Startup/ownership checks do not require an autonomous election or background peer traffic.
    private sealed class FrozenClock : TimeProvider
    {
        public override ITimer CreateTimer(
            TimerCallback callback,
            object? state,
            TimeSpan dueTime,
            TimeSpan period
        ) => new FrozenTimer();

        private sealed class FrozenTimer : ITimer
        {
            private int _disposed;

            public bool Change(TimeSpan dueTime, TimeSpan period) =>
                Volatile.Read(ref _disposed) == 0;

            public void Dispose() => Interlocked.Exchange(ref _disposed, 1);

            public ValueTask DisposeAsync()
            {
                Dispose();
                return ValueTask.CompletedTask;
            }
        }
    }

    private sealed class Receiver : IDataGridPeerReceiver
    {
        public Task<long> ObserveLeaderAsync(long term, CancellationToken cancellationToken) =>
            Task.FromResult(term);

        public Task<RaftMessage?> ReceiveAsync(
            RaftMessage message,
            CancellationToken cancellationToken
        ) =>
            throw new InvalidOperationException(
                "Sender-only fixture must not dispatch incoming work."
            );
    }
}
