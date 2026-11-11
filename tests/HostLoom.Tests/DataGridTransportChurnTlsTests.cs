using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Transport;
using Xunit;

namespace HostLoom.Tests;

[Collection(nameof(DataGridDiagnosticsTests))]
public sealed class DataGridTransportChurnTlsTests
{
    private static readonly Guid Other = new("30112233-4455-6677-8899-aabbccddeeff");

    [Theory(Timeout = 30000)]
    [InlineData("tcp")]
    [InlineData("tls")]
    [InlineData("hello")]
    [InlineData("body")]
    public Task RepeatedDisconnectsReleaseEachProtocolStageAndPreservePeerProgress(string stage) =>
        WithLab(
            new ClientReceiver(),
            async lab =>
            {
                using var pacing = new PeriodicTimer(TimeSpan.FromMilliseconds(150));
                for (var i = 0; i < 16; i++)
                {
                    await pacing.WaitForNextTickAsync(lab.Token);
                    await Disconnect(lab, stage);
                    await lab.Capture.WaitFor(
                        _ => lab.Capture.Reserved("connection", "client") == 0,
                        lab.Token
                    );
                    Assert.Equal(0, lab.Capture.Reserved("principal", "client"));
                    Assert.Equal(0, lab.Capture.Reserved("handshake", "client"));
                    Assert.Equal(0, lab.Capture.Reserved("request", "client"));
                    Assert.Equal(0, lab.Receiver.Calls);
                    if (i % 4 == 3)
                        await PeerProgress(lab, i + 1);
                }
                await pacing.WaitForNextTickAsync(lab.Token);
                await Healthy(lab, lab.Fixture.Certificates[0], DataGridPeerFixture.Principal);
                Assert.Equal(1, lab.Receiver.Calls);
                Assert.Equal(4, lab.Control.Calls);
                await lab.Server.DisposeAsync();
                Assert.Equal(
                    17,
                    lab.Capture.Measurements.Count(m =>
                        m.Name == "hostloom.datagrid.transport.exchanges" && IsClientInbound(m)
                    )
                );
            },
            TestContext.Current.CancellationToken
        );

    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public Task FullClientHandshakeBudgetLeavesPeerCapacityAndDrainsOnDeadlineOrStop(
        bool shutdown
    ) =>
        WithLab(
            new ClientReceiver(),
            async lab =>
            {
                var sockets = new List<TcpClient>();
                using var pacing = new PeriodicTimer(TimeSpan.FromMilliseconds(150));
                try
                {
                    for (var i = 0; i < 12; i++)
                    {
                        await pacing.WaitForNextTickAsync(lab.Token);
                        var tcp = new TcpClient();
                        sockets.Add(tcp);
                        await tcp.ConnectAsync(IPAddress.Loopback, lab.Port, lab.Token);
                        await lab.Capture.WaitFor(
                            _ => lab.Capture.Reserved("handshake", "client") == i + 1,
                            lab.Token
                        );
                    }
                    Assert.Equal(12, lab.Capture.Reserved("connection", "client"));
                    await pacing.WaitForNextTickAsync(lab.Token);
                    using (var excess = new TcpClient())
                    {
                        await excess.ConnectAsync(IPAddress.Loopback, lab.Port, lab.Token);
                        await lab.Capture.WaitFor(
                            records => records.Any(m => Rejection(m, "handshake")),
                            lab.Token
                        );
                        await DataGridTransportAdmissionTlsTests.Closed(
                            excess.GetStream(),
                            lab.Token
                        );
                    }
                    await PeerProgress(lab, 1);
                    Assert.Equal(12, lab.Capture.Reserved("handshake", "client"));
                    Assert.Equal(0, lab.Capture.Reserved("principal", "client"));
                    Assert.Equal(0, lab.Capture.Reserved("request", "client"));
                    Assert.Equal(0, lab.Receiver.Calls);
                    if (shutdown)
                    {
                        var stopped = lab.Server.DisposeAsync().AsTask();
                        Assert.Same(stopped, lab.Server.DisposeAsync().AsTask());
                        await stopped;
                    }
                    foreach (var tcp in sockets)
                        await DataGridTransportAdmissionTlsTests.Closed(tcp.GetStream(), lab.Token);
                    await lab.Capture.WaitFor(
                        _ => lab.Capture.Reserved("connection", "client") == 0,
                        lab.Token
                    );
                    Assert.Equal(0, lab.Capture.Reserved("handshake", "client"));
                    if (!shutdown)
                        await Healthy(
                            lab,
                            lab.Fixture.Certificates[0],
                            DataGridPeerFixture.Principal
                        );
                }
                finally
                {
                    foreach (var tcp in sockets)
                        tcp.Dispose();
                }
            },
            TestContext.Current.CancellationToken
        );

    [Fact(Timeout = 30000)]
    public async Task ReloadCannotResetHeldPrincipalQuotaAndRetirementWaitsForActualRelease()
    {
        var receiver = new ClientReceiver(hold: true);
        await WithLab(
            receiver,
            async lab =>
            {
                var sockets = new List<TcpClient>();
                var streams = new List<SslStream>();
                using var pacing = new PeriodicTimer(TimeSpan.FromMilliseconds(150));
                using var replacement = lab.Fixture.Issue(
                    "catalog-replacement.example",
                    clientOnly: true
                );
                try
                {
                    for (var i = 0; i < 8; i++)
                    {
                        await pacing.WaitForNextTickAsync(lab.Token);
                        var tcp = new TcpClient();
                        sockets.Add(tcp);
                        var tls = await Hello(
                            lab,
                            tcp,
                            lab.Fixture.Certificates[0],
                            DataGridPeerFixture.Principal
                        );
                        streams.Add(tls);
                        await DataGridTlsSupport.WriteAsync(
                            tls,
                            Request(lab, DataGridPeerFixture.Principal),
                            lab.Token
                        );
                    }
                    await receiver.Entered.Task.WaitAsync(lab.Token);
                    Assert.Equal(8, lab.Capture.Reserved("principal", "client"));
                    Assert.Equal(8, lab.Capture.Reserved("request", "client"));
                    await pacing.WaitForNextTickAsync(lab.Token);
                    await PrincipalRefused(lab, lab.Fixture.Certificates[0]);
                    var reload = lab.Security.Reload(
                        lab.Fixture.Credentials(0),
                        Policy(lab.Fixture, replacement)
                    );
                    await receiver.Canceled.Task.WaitAsync(lab.Token);
                    Assert.False(reload.Retired.IsCompleted);
                    Assert.Equal(2, reload.Generation);
                    await pacing.WaitForNextTickAsync(lab.Token);
                    await PrincipalRefused(lab, replacement);
                    Assert.Equal(8, receiver.Calls);
                    Assert.Equal(8, lab.Capture.Reserved("principal", "client"));
                    await pacing.WaitForNextTickAsync(lab.Token);
                    await Healthy(lab, lab.Fixture.Certificates[2], Other);
                    await PeerProgress(lab, 1);
                    Assert.Throws<IOException>(() =>
                        lab.Security.Reload(
                            lab.Fixture.Credentials(0),
                            Policy(lab.Fixture, replacement)
                        )
                    );
                    receiver.Release();
                    await reload.Retired.WaitAsync(lab.Token);
                    foreach (var stream in streams)
                        await DataGridTransportAdmissionTlsTests.Closed(stream, lab.Token);
                    await lab.Capture.WaitFor(
                        _ => lab.Capture.Reserved("connection", "client") == 0,
                        lab.Token
                    );
                    Assert.Equal(0, lab.Capture.Reserved("principal", "client"));
                    Assert.Equal(0, lab.Capture.Reserved("request", "client"));
                    await pacing.WaitForNextTickAsync(lab.Token);
                    await Healthy(lab, replacement, DataGridPeerFixture.Principal);
                    Assert.Equal(10, receiver.Calls);
                    Assert.Equal(2, lab.Security.Generation);
                    Assert.Equal(2, lab.Capture.Measurements.Count(m => Rejection(m, "principal")));
                }
                finally
                {
                    receiver.Release();
                    foreach (var stream in streams)
                        await stream.DisposeAsync();
                    foreach (var tcp in sockets)
                        tcp.Dispose();
                }
            },
            TestContext.Current.CancellationToken
        );
    }

    private static async Task Disconnect(Lab lab, string stage)
    {
        using var tcp = new TcpClient();
        if (stage is "hello" or "body")
        {
            await using var tls = await Hello(
                lab,
                tcp,
                lab.Fixture.Certificates[0],
                DataGridPeerFixture.Principal
            );
            await lab.Capture.WaitFor(
                _ => lab.Capture.Reserved("request", "client") == 1,
                lab.Token
            );
            if (stage == "body")
                await tls.WriteAsync(new byte[] { 0, 0, 0, 72, 1, 2, 3 }, lab.Token);
            return;
        }
        await tcp.ConnectAsync(IPAddress.Loopback, lab.Port, lab.Token);
        if (stage == "tls")
        {
            await using var tls = new SslStream(tcp.GetStream());
            await Authenticate(lab, tls);
            await lab.Capture.WaitFor(
                _ => lab.Capture.Reserved("principal", "client") == 1,
                lab.Token
            );
            Assert.Equal(0, lab.Capture.Reserved("request", "client"));
        }
        else
            await lab.Capture.WaitFor(
                _ => lab.Capture.Reserved("handshake", "client") == 1,
                lab.Token
            );
    }

    [SuppressMessage(
        "Security",
        "CA5398:Avoid hardcoded SslProtocols values",
        Justification = "The grid requires TLS 1.3 without fallback."
    )]
    private static Task Authenticate(Lab lab, SslStream tls) =>
        tls.AuthenticateAsClientAsync(
            new SslClientAuthenticationOptions
            {
                EnabledSslProtocols = SslProtocols.Tls13,
                TargetHost = lab.Fixture.Configuration.GetMember(0).DnsName,
                ClientCertificates = new X509CertificateCollection { lab.Fixture.Certificates[0] },
                CertificateChainPolicy = DataGridTlsSupport.ChainPolicy(
                    lab.Fixture.Root,
                    server: true
                ),
                AllowRenegotiation = false,
                AllowTlsResume = false,
                ApplicationProtocols = [DataGridClientServer.Protocol],
            },
            lab.Token
        );

    private static Task<SslStream> Hello(
        Lab lab,
        TcpClient tcp,
        X509Certificate2 certificate,
        Guid principal
    ) =>
        DataGridTransportAdmissionTlsTests.ClientHello(
            lab.Fixture,
            tcp,
            lab.Port,
            certificate,
            principal,
            lab.Token
        );

    private static async Task Healthy(Lab lab, X509Certificate2 certificate, Guid principal)
    {
        using var tcp = new TcpClient();
        await using var tls = await Hello(lab, tcp, certificate, principal);
        var request = Request(lab, principal);
        await DataGridTlsSupport.WriteAsync(tls, request, lab.Token);
        var frame = await DataGridTlsSupport.ReadAsync(
            tls,
            lab.Fixture.Configuration.IncarnationId,
            lab.Fixture.Configuration.GetMember(0).MemberId,
            lab.Token
        );
        var reply = DataGridClientCodec.DecodeReply(
            frame,
            Read(),
            request.AttemptId,
            lab.Fixture.Configuration
        );
        Assert.Null(reply.Failure);
        Assert.Null(reply.Read!.GetItem(0));
    }

    private static async Task PrincipalRefused(Lab lab, X509Certificate2 certificate)
    {
        using var tcp = new TcpClient();
        var failure = await Record.ExceptionAsync(async () =>
        {
            await using var tls = await Hello(lab, tcp, certificate, DataGridPeerFixture.Principal);
        });
        Assert.NotNull(failure);
        Assert.True(failure is IOException or AuthenticationException, failure.GetType().Name);
        Assert.Contains(lab.Capture.Measurements, m => Rejection(m, "principal"));
    }

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

    private static DataGridProtocolFrame Request(Lab lab, Guid principal) =>
        DataGridClientCodec.EncodeRequest(
            Read(),
            lab.Fixture.Configuration.IncarnationId,
            principal,
            Guid.NewGuid()
        );

    private static async Task PeerProgress(Lab lab, int sequence)
    {
        var reply = await lab.Sender.SendAsync(
            new(
                RaftMessageKind.VoteRequest,
                1,
                0,
                3,
                new(1, sequence),
                lab.Fixture.Configuration.IncarnationId,
                lab.Fixture.Configuration.ComputeDigest()
            ),
            lab.Token
        );
        Assert.True(reply.Accepted);
    }

    private static bool IsClientInbound(DataGridMeasurement m) =>
        m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.RoleTag) is "client"
        && m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.DirectionTag) is "inbound";

    private static bool Rejection(DataGridMeasurement m, string resource) =>
        m.Name == "hostloom.datagrid.transport.admission.rejected"
        && IsClientInbound(m)
        && Equals(m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ResourceTag), resource)
        && m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ReasonTag) is "capacity";

    private static DataGridClientAccessPolicy Policy(
        DataGridPeerFixture fixture,
        X509Certificate2? replacement = null
    ) =>
        new(
            fixture.Configuration,
            fixture.Root,
            [
                new(
                    DataGridPeerFixture.Principal,
                    (replacement ?? fixture.Certificates[0]).GetCertHash(HashAlgorithmName.SHA256),
                    true,
                    [new(DataGridPeerFixture.Map, DataGridMapActions.Read)]
                ),
                new(
                    Other,
                    fixture.Fingerprints[2],
                    true,
                    [new(DataGridPeerFixture.Map, DataGridMapActions.Read)]
                ),
            ]
        );

    private static async Task WithLab(
        ClientReceiver receiver,
        Func<Lab, Task> body,
        CancellationToken cancellationToken
    )
    {
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS 1.3 contention/churn evidence requires Linux."
        );
        using var timeout = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
        timeout.CancelAfter(TimeSpan.FromSeconds(25));
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        await using var security = new DataGridSecurityContext(
            fixture.Credentials(0),
            Policy(fixture)
        );
        var control = new PeerReceiver(fixture.Configuration);
        await using var peer = new DataGridPeerTransport(
            security,
            fixture.Listeners[0],
            DataGridPeerFixture.Routes,
            control
        );
        peer.Start();
        await using var sender = new DataGridPeerTransport(
            fixture.Credentials(1),
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            new PeerReceiver(fixture.Configuration)
        );
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        await using var server = new DataGridClientServer(security, listener, receiver);
        server.Start();
        try
        {
            await body(
                new(
                    fixture,
                    security,
                    server,
                    sender,
                    control,
                    receiver,
                    capture,
                    ((IPEndPoint)listener.LocalEndpoint).Port,
                    timeout.Token
                )
            );
        }
        finally
        {
            receiver.Release();
        }
        await server.DisposeAsync();
        await peer.DisposeAsync();
        await sender.DisposeAsync();
        Assert.Null(server.FatalError);
        Assert.Null(peer.FatalError);
        Assert.Null(sender.FatalError);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private sealed record Lab(
        DataGridPeerFixture Fixture,
        DataGridSecurityContext Security,
        DataGridClientServer Server,
        DataGridPeerTransport Sender,
        PeerReceiver Control,
        ClientReceiver Receiver,
        DataGridMeterCapture Capture,
        int Port,
        CancellationToken Token
    );

    private sealed class ClientReceiver(bool hold = false) : IDataGridClientReceiver
    {
        private readonly TaskCompletionSource _release = new(
            TaskCreationOptions.RunContinuationsAsynchronously
        );
        private int _calls,
            _canceled,
            _holding = hold ? 1 : 0;
        internal int Calls => Volatile.Read(ref _calls);
        internal TaskCompletionSource Entered { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal TaskCompletionSource Canceled { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        internal void Release()
        {
            Volatile.Write(ref _holding, 0);
            _release.TrySetResult();
        }

        public async Task<DataGridClientReply> ExecuteAsync(
            Guid principal,
            DataGridClientRequest request,
            CancellationToken cancellationToken
        )
        {
            var calls = Interlocked.Increment(ref _calls);
            if (Volatile.Read(ref _holding) != 0 && principal == DataGridPeerFixture.Principal)
            {
                using var canceled = cancellationToken.Register(() =>
                {
                    if (Interlocked.Increment(ref _canceled) == 8)
                        Canceled.TrySetResult();
                });
                if (calls == 8)
                    Entered.TrySetResult();
                // Deliberately retain ownership after cancellation until the test releases the gate.
                await _release.Task.WaitAsync(TestContext.Current.CancellationToken);
            }
            return DataGridClientReply.Completed(
                DataGridMessageKind.ReadResult,
                read: new(new(1), [null])
            );
        }
    }

    private sealed class PeerReceiver(DataGridBootstrapConfiguration configuration)
        : IDataGridPeerReceiver
    {
        private int _calls;
        internal int Calls => Volatile.Read(ref _calls);

        public Task<long> ObserveLeaderAsync(long term, CancellationToken cancellationToken) =>
            Task.FromResult(term);

        public Task<RaftMessage?> ReceiveAsync(
            RaftMessage message,
            CancellationToken cancellationToken
        )
        {
            Interlocked.Increment(ref _calls);
            return Task.FromResult<RaftMessage?>(
                new(
                    RaftMessageKind.VoteResponse,
                    message.To,
                    message.From,
                    message.Term,
                    message.Token,
                    configuration.IncarnationId,
                    configuration.ComputeDigest(),
                    accepted: true
                )
            );
        }
    }
}
