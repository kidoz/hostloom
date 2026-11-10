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
[SuppressMessage(
    "Security",
    "CA5398:Avoid hardcoded SslProtocols values",
    Justification = "The grid requires TLS 1.3 without fallback."
)]
public sealed class DataGridTransportAdmissionTlsTests
{
    [Theory(Timeout = 20000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task StalledHandshakesRefuseOverflowAndReleaseOnDeadlineOrShutdown(bool shutdown)
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        var receiver = new Receiver(fixture.Configuration);
        await using var server = Server(fixture, receiver);
        server.Start();
        var sockets = new List<TcpClient>();
        try
        {
            for (var i = 0; i < 4; i++)
            {
                var tcp = new TcpClient();
                sockets.Add(tcp);
                await tcp.ConnectAsync(
                    IPAddress.Loopback,
                    fixture.Configuration.GetMember(1).Port,
                    timeout.Token
                );
            }
            await capture.WaitFor(_ => capture.Reserved("handshake", "peer") == 4, timeout.Token);
            using var excess = new TcpClient();
            await excess.ConnectAsync(
                IPAddress.Loopback,
                fixture.Configuration.GetMember(1).Port,
                timeout.Token
            );
            await capture.WaitFor(
                records => records.Any(m => Rejection(m, "handshake", "capacity")),
                timeout.Token
            );
            await Closed(excess.GetStream(), timeout.Token);
            if (shutdown)
                await server.DisposeAsync();
            foreach (var tcp in sockets)
                await Closed(tcp.GetStream(), timeout.Token);
            await capture.WaitFor(_ => capture.Reserved("connection", "peer") == 0, timeout.Token);
            Assert.Equal(0, receiver.Calls);
            Assert.Null(server.FatalError);
            if (!shutdown)
            {
                using var tcp = new TcpClient();
                using var tls = await PeerHello(fixture, tcp, timeout.Token);
            }
        }
        finally
        {
            foreach (var tcp in sockets)
                tcp.Dispose();
        }
        await server.DisposeAsync();
        AssertPeak(capture, "handshake", "peer", 4);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 20000)]
    public async Task AuthenticatedMemberCeilingRefusesThirdConnectionAndRecoversAfterRelease()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        var receiver = new Receiver(fixture.Configuration);
        await using var server = Server(fixture, receiver);
        server.Start();
        using (var first = new TcpClient())
        using (var second = new TcpClient())
        using (var a = await PeerHello(fixture, first, timeout.Token))
        using (var b = await PeerHello(fixture, second, timeout.Token))
        {
            await capture.WaitFor(_ => capture.Reserved("member", "peer") == 2, timeout.Token);
            using var third = new TcpClient();
            Assert.NotNull(
                await Record.ExceptionAsync(async () =>
                {
                    using var rejected = await PeerHello(fixture, third, timeout.Token);
                })
            );
            await capture.WaitFor(
                records => records.Any(m => Rejection(m, "member", "capacity")),
                timeout.Token
            );
            Assert.Equal(0, receiver.Calls);
        }
        await capture.WaitFor(_ => capture.Reserved("member", "peer") == 0, timeout.Token);
        await using (
            var client = new DataGridPeerTransport(
                fixture.Credentials(0),
                fixture.Listeners[0],
                DataGridPeerFixture.Routes,
                new Receiver(fixture.Configuration)
            )
        )
        {
            Assert.True(
                (
                    await client.SendAsync(
                        fixture.Message(RaftMessageKind.VoteRequest, term: 3),
                        timeout.Token
                    )
                ).Accepted
            );
        }
        await server.DisposeAsync();
        Assert.Equal(1, receiver.Calls);
        Assert.Null(server.FatalError);
        AssertPeak(capture, "member", "peer", 2);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Theory(Timeout = 20000)]
    [InlineData("unauthorized")]
    [InlineData("malformed")]
    [InlineData("length")]
    [InlineData("unbound")]
    public async Task HostileClientCannotDispatchAndAllReservationsReturnBeforeHealthyRequest(
        string reason
    )
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        using var unbound = fixture.Issue("catalog-unbound.example", clientOnly: true);
        var healthyId = Guid.NewGuid();
        var policy = new DataGridClientAccessPolicy(
            fixture.Configuration,
            fixture.Root,
            [
                new(
                    DataGridPeerFixture.Principal,
                    fixture.Fingerprints[0],
                    true,
                    [
                        new(
                            DataGridPeerFixture.Map,
                            reason == "unauthorized"
                                ? DataGridMapActions.Mutate
                                : DataGridMapActions.Read
                        ),
                    ]
                ),
                new(
                    healthyId,
                    fixture.Fingerprints[2],
                    true,
                    [new(DataGridPeerFixture.Map, DataGridMapActions.Read)]
                ),
            ]
        );
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        var receiver = new ClientReceiver();
        await using var server = new DataGridClientServer(
            fixture.Credentials(0),
            policy,
            listener,
            receiver
        );
        server.Start();
        var port = ((IPEndPoint)listener.LocalEndpoint).Port;
        var request = DataGridClientRequest.Read(
            DataGridPeerFixture.Map,
            [DataGridPeerFixture.Key]
        );
        using (var tcp = new TcpClient())
        {
            if (reason == "unbound")
            {
                Assert.NotNull(
                    await Record.ExceptionAsync(async () =>
                    {
                        using var rejected = await ClientHello(
                            fixture,
                            tcp,
                            port,
                            unbound,
                            DataGridPeerFixture.Principal,
                            timeout.Token
                        );
                    })
                );
            }
            else
            {
                using var tls = await ClientHello(
                    fixture,
                    tcp,
                    port,
                    fixture.Certificates[0],
                    DataGridPeerFixture.Principal,
                    timeout.Token
                );
                var frame = DataGridClientCodec.EncodeRequest(
                    request,
                    fixture.Configuration.IncarnationId,
                    DataGridPeerFixture.Principal,
                    Guid.NewGuid()
                );
                if (reason == "length")
                {
                    await tls.WriteAsync(new byte[] { 255, 255, 255, 255 }, timeout.Token);
                    await Closed(tls, timeout.Token);
                }
                else
                {
                    if (reason == "malformed")
                        frame = new(
                            frame.Kind,
                            frame.IncarnationId,
                            frame.SenderId,
                            frame.SessionNumber,
                            frame.RequestNumber,
                            frame.AttemptId,
                            []
                        );
                    await DataGridTlsSupport.WriteAsync(tls, frame, timeout.Token);
                    var reply = await DataGridTlsSupport.ReadAsync(
                        tls,
                        fixture.Configuration.IncarnationId,
                        fixture.Configuration.GetMember(0).MemberId,
                        timeout.Token
                    );
                    Assert.Equal(
                        reason == "malformed"
                            ? DataGridClientFailureCode.InvalidRequest
                            : DataGridClientFailureCode.Unauthorized,
                        DataGridClientCodec
                            .DecodeReply(reply, request, frame.AttemptId, fixture.Configuration)
                            .Failure!.Code
                    );
                }
            }
        }
        await capture.WaitFor(_ => capture.Reserved("connection", "client") == 0, timeout.Token);
        Assert.Equal(0, receiver.Calls);
        Assert.Contains(
            capture.Measurements,
            m =>
                reason switch
                {
                    "unbound" => Outcome(m, "authentication"),
                    "length" => Outcome(m, "protocol"),
                    "malformed" => Rejection(m, "request", "invalid_request"),
                    _ => Rejection(m, "request", "unauthorized"),
                }
        );
        using (var tcp = new TcpClient())
        using (
            var tls = await ClientHello(
                fixture,
                tcp,
                port,
                fixture.Certificates[2],
                healthyId,
                timeout.Token
            )
        )
        {
            var frame = DataGridClientCodec.EncodeRequest(
                request,
                fixture.Configuration.IncarnationId,
                healthyId,
                Guid.NewGuid()
            );
            await DataGridTlsSupport.WriteAsync(tls, frame, timeout.Token);
            var reply = await DataGridTlsSupport.ReadAsync(
                tls,
                fixture.Configuration.IncarnationId,
                fixture.Configuration.GetMember(0).MemberId,
                timeout.Token
            );
            Assert.Equal(
                DataGridClientFailureCode.MissingMap,
                DataGridClientCodec
                    .DecodeReply(reply, request, frame.AttemptId, fixture.Configuration)
                    .Failure!.Code
            );
        }
        await server.DisposeAsync();
        Assert.Equal(1, receiver.Calls);
        Assert.Null(server.FatalError);
        Assert.Contains(capture.Measurements, m => Outcome(m, "completed"));
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    [Fact(Timeout = 20000)]
    public async Task SnapshotDisconnectReleasesImageReservationBeforeAnotherTransferStarts()
    {
        RequireLinux();
        using var timeout = Deadline(TestContext.Current.CancellationToken);
        using var capture = new DataGridMeterCapture();
        using var fixture = new DataGridPeerFixture();
        var receiver = new Receiver(fixture.Configuration);
        await using var server = Server(fixture, receiver);
        server.Start();
        var machine = new DataGridLocalStateMachine(fixture.Configuration);
        machine.Apply(1, DataGridCommittedCommand.AdvanceTime(new(1)));
        var image = machine.SaveSnapshot(Guid.NewGuid(), 1);
        using var first = new TcpClient();
        using var second = new TcpClient();
        var attemptNumber = 0;
        foreach (var tcp in new[] { first, second })
        {
            {
                using var tls = await PeerHello(fixture, tcp, timeout.Token);
                using var writer = new GridWriter(80);
                writer.Number(3, 8);
                writer.Raw(image.Descriptor.Encode());
                await DataGridPeerCodec.WriteAsync(
                    tls,
                    DataGridPeerCodec.Frame(
                        fixture.Configuration,
                        0,
                        DataGridMessageKind.SnapshotStart,
                        DataGridPeerCodec.Attempt(new(1, ++attemptNumber)),
                        writer.ToArray()
                    ),
                    timeout.Token
                );
                Assert.Equal(
                    DataGridMessageKind.SnapshotResponse,
                    (
                        await DataGridPeerCodec.ReadAsync(
                            tls,
                            fixture.Configuration,
                            1,
                            timeout.Token
                        )
                    ).Kind
                );
                Assert.True(server.TransferActive);
                Assert.Equal(
                    image.Descriptor.ByteCount,
                    capture.Sum("transport.reservations.bytes")
                );
            }
            await capture.WaitFor(_ => capture.Reserved("snapshot", "peer") == 0, timeout.Token);
        }
        await server.DisposeAsync();
        Assert.False(server.TransferActive);
        Assert.Equal(0, receiver.Calls);
        Assert.Null(server.FatalError);
        capture.AssertBalanced();
        capture.AssertPrivate();
    }

    private static DataGridPeerTransport Server(DataGridPeerFixture fixture, Receiver receiver) =>
        new(fixture.Credentials(1), fixture.Listeners[1], DataGridPeerFixture.Routes, receiver);

    private static async Task<SslStream> PeerHello(
        DataGridPeerFixture fixture,
        TcpClient tcp,
        CancellationToken token
    )
    {
        await tcp.ConnectAsync(IPAddress.Loopback, fixture.Configuration.GetMember(1).Port, token);
        var tls = new SslStream(tcp.GetStream());
        try
        {
            await tls.AuthenticateAsClientAsync(fixture.Credentials(0).ClientOptions(1), token);
            var hello = DataGridPeerCodec.Hello(
                fixture.Configuration,
                0,
                Guid.NewGuid(),
                fixture.Build
            );
            await DataGridPeerCodec.WriteAsync(tls, hello, token);
            await DataGridPeerCodec.ReadAsync(tls, fixture.Configuration, 1, token);
            return tls;
        }
        catch
        {
            await tls.DisposeAsync();
            throw;
        }
    }

    private static async Task<SslStream> ClientHello(
        DataGridPeerFixture fixture,
        TcpClient tcp,
        int port,
        X509Certificate2 certificate,
        Guid principal,
        CancellationToken token
    )
    {
        await tcp.ConnectAsync(IPAddress.Loopback, port, token);
        var tls = new SslStream(tcp.GetStream());
        try
        {
            await tls.AuthenticateAsClientAsync(
                new SslClientAuthenticationOptions
                {
                    EnabledSslProtocols = SslProtocols.Tls13,
                    TargetHost = fixture.Configuration.GetMember(0).DnsName,
                    ClientCertificates = new X509CertificateCollection { certificate },
                    CertificateChainPolicy = DataGridTlsSupport.ChainPolicy(
                        fixture.Root,
                        server: true
                    ),
                    AllowRenegotiation = false,
                    AllowTlsResume = false,
                    ApplicationProtocols = [DataGridClientServer.Protocol],
                    RemoteCertificateValidationCallback = (_, remote, _, errors) =>
                        errors == SslPolicyErrors.None
                        && remote is X509Certificate2 leaf
                        && CryptographicOperations.FixedTimeEquals(
                            leaf.GetCertHash(HashAlgorithmName.SHA256),
                            fixture.Fingerprints[0]
                        ),
                },
                token
            );
            var hello = DataGridClientCodec.Hello(
                fixture.Configuration.IncarnationId,
                principal,
                Guid.NewGuid(),
                fixture.Build,
                fixture.Configuration.ComputeDigest()
            );
            await DataGridTlsSupport.WriteAsync(tls, hello, token);
            DataGridClientCodec.ValidateHello(
                await DataGridTlsSupport.ReadAsync(
                    tls,
                    fixture.Configuration.IncarnationId,
                    fixture.Configuration.GetMember(0).MemberId,
                    token
                ),
                fixture.Configuration
            );
            return tls;
        }
        catch
        {
            await tls.DisposeAsync();
            throw;
        }
    }

    private static async Task Closed(Stream stream, CancellationToken token)
    {
        try
        {
            Assert.Equal(0, await stream.ReadAsync(new byte[1], token));
        }
        catch (IOException) { }
    }

    private static bool Rejection(DataGridMeasurement m, string resource, string reason) =>
        m.Name == "hostloom.datagrid.transport.admission.rejected"
        && Equals(m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ResourceTag), resource)
        && Equals(m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ReasonTag), reason);

    private static bool Outcome(DataGridMeasurement m, string outcome) =>
        m.Name == "hostloom.datagrid.transport.exchanges"
        && Equals(m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.OutcomeTag), outcome);

    private static void AssertPeak(
        DataGridMeterCapture capture,
        string resource,
        string role,
        int limit
    )
    {
        double active = 0;
        foreach (
            var m in capture.Measurements.Where(m =>
                m.Name == "hostloom.datagrid.transport.reservations.active"
                && Equals(
                    m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.ResourceTag),
                    resource
                )
                && Equals(m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.RoleTag), role)
                && m.Tags.GetValueOrDefault(DataGridTransportDiagnostics.DirectionTag) is "inbound"
            )
        )
        {
            active += m.Value;
            Assert.InRange(active, 0, limit);
        }
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "Live TLS 1.3 admission evidence requires Linux."
        );

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var timeout = CancellationTokenSource.CreateLinkedTokenSource(token);
        timeout.CancelAfter(TimeSpan.FromSeconds(15));
        return timeout;
    }

    private sealed class ClientReceiver : IDataGridClientReceiver
    {
        private int _calls;
        internal int Calls => Volatile.Read(ref _calls);

        public Task<DataGridClientReply> ExecuteAsync(
            Guid principal,
            DataGridClientRequest request,
            CancellationToken cancellationToken
        )
        {
            Interlocked.Increment(ref _calls);
            return Task.FromResult(
                DataGridClientReply.Failed(new(DataGridClientFailureCode.MissingMap, false))
            );
        }
    }

    private sealed class Receiver(DataGridBootstrapConfiguration configuration)
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
