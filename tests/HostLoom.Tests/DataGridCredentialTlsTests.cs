using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid;
using HostLoom.DataGrid.Client;
using HostLoom.DataGrid.Consensus;
using HostLoom.DataGrid.Runtime;
using HostLoom.DataGrid.Transport;
using Xunit;
using GridClient = HostLoom.DataGrid.Client.DataGridClient;

namespace HostLoom.Tests;

[SuppressMessage(
    "Security",
    "CA5398:Avoid hardcoded SslProtocols values",
    Justification = "The protocol requires exactly TLS 1.3 without fallback."
)]
public sealed class DataGridCredentialTlsTests
{
    [Fact(Timeout = 30000)]
    public async Task RevokedAuthenticatedClientIsClosedBeforeRequestAndNewHandshakeIsDenied()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        await using var lab = new Lab();
        var leader = await lab.Cluster.Leader(deadline.Token);
        using var tcp = new TcpClient();
        using var tls = await Connect(lab, leader, tcp, deadline.Token);
        var calls = 0;
        lab.Hook = (_, _, _, _) =>
        {
            Interlocked.Increment(ref calls);
            throw new InvalidOperationException("Revoked caller reached actor.");
        };
        var reloads = Enumerable
            .Range(0, 3)
            .Select(i => lab.Security[i].Reload(lab.Peer.Credentials(i), lab.Policy(denied: true)))
            .ToArray();
        await Closed(tls, deadline.Token);
        await Task.WhenAll(reloads.Select(reload => reload.Retired)).WaitAsync(deadline.Token);
        await using var client = new GridClient(lab.Options(leader));
        var result = await client.ExecuteAsync(Read(), deadline.Token);
        Assert.NotNull(result.Reply.Failure);
        Assert.False(result.MayHaveBeenAdmitted);
        Assert.Equal(0, calls);
        lab.Healthy();
    }

    [Fact(Timeout = 30000)]
    public async Task InvalidReloadPreservesOldAuthenticatedConnectionAndPolicy()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        await using var lab = new Lab();
        var leader = await lab.Cluster.Leader(deadline.Token);
        using var tcp = new TcpClient();
        using var tls = await Connect(lab, leader, tcp, deadline.Token);
        using var invalid = lab.Peer.Issue(
            lab.Peer.Configuration.GetMember(leader).DnsName,
            clientOnly: true
        );
        var pins = lab.Peer.Fingerprints.Select(bytes => (byte[])bytes.Clone()).ToArray();
        pins[leader] = invalid.GetCertHash(HashAlgorithmName.SHA256);
        Assert.Throws<AuthenticationException>(() =>
            lab.Security[leader]
                .Reload(
                    lab.Peer.Credentials(leader, certificate: invalid, fingerprints: pins),
                    lab.Policy()
                )
        );
        Assert.Equal(1, lab.Security[leader].Generation);
        var frame = DataGridClientCodec.EncodeRequest(
            Read(),
            lab.Peer.Configuration.IncarnationId,
            DataGridPeerFixture.Principal,
            Guid.NewGuid()
        );
        await DataGridTlsSupport.WriteAsync(tls, frame, deadline.Token);
        var reply = await DataGridTlsSupport.ReadAsync(
            tls,
            lab.Peer.Configuration.IncarnationId,
            lab.Peer.Configuration.GetMember(leader).MemberId,
            deadline.Token
        );
        Assert.Null(
            DataGridClientCodec
                .DecodeReply(reply, Read(), frame.AttemptId, lab.Peer.Configuration)
                .Failure
        );
        lab.Healthy();
    }

    [Fact(Timeout = 30000)]
    public async Task RevocationAfterCommitPreservesUncertaintyAndRetainedResultWhenReauthorized()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        await using var lab = new Lab();
        var leader = await lab.Cluster.Leader(deadline.Token);
        await using var client = new GridClient(lab.Options(leader));
        var session = await Open(client, deadline.Token);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        DataGridEntryVersion original = default;
        lab.Hook = async (member, principal, request, token) =>
        {
            var reply = await lab.Cluster.Nodes[member]!.ExecuteAsync(principal, request, token);
            if (request.Mutation is not null && reply.Result is { } result && original.IsEmpty)
            {
                original = result.GetItem(0).Version;
                entered.TrySetResult();
                await release.Task.WaitAsync(deadline.Token);
            }
            return reply;
        };
        var logical = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session));
        var call = client.ExecuteAsync(logical, deadline.Token);
        DataGridCredentialReload[] reloads = [];
        try
        {
            await entered.Task.WaitAsync(deadline.Token);
            reloads = Enumerable
                .Range(0, 3)
                .Select(i =>
                    lab.Security[i].Reload(lab.Peer.Credentials(i), lab.Policy(denied: true))
                )
                .ToArray();
            var outcome = await call;
            Assert.NotNull(outcome.Reply.Failure);
            Assert.True(outcome.MayHaveBeenAdmitted);
            Assert.False(reloads[leader].Retired.IsCompleted);
        }
        finally
        {
            release.TrySetResult();
        }
        await Task.WhenAll(reloads.Select(reload => reload.Retired)).WaitAsync(deadline.Token);
        foreach (var i in Enumerable.Range(0, 3))
            await lab.Security[i]
                .Reload(lab.Peer.Credentials(i), lab.Policy())
                .Retired.WaitAsync(deadline.Token);
        var retry = await client.ExecuteAsync(logical, deadline.Token);
        Assert.Equal(original, Stored(retry));
        lab.Healthy();
    }

    [Fact(Timeout = 30000)]
    public async Task SdkReloadClosesOldSubmittedExchangeWithoutRegeneratingLogicalIdentity()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        await using var lab = new Lab();
        var leader = await lab.Cluster.Leader(deadline.Token);
        await using var client = new GridClient(lab.Options(leader));
        var session = await Open(client, deadline.Token);
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var release = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        DataGridEntryVersion original = default;
        lab.Hook = async (member, principal, request, token) =>
        {
            var result = await lab.Cluster.Nodes[member]!.ExecuteAsync(principal, request, token);
            if (request.Mutation is not null && original.IsEmpty)
            {
                original = result.Result!.GetItem(0).Version;
                entered.TrySetResult();
                await release.Task.WaitAsync(deadline.Token);
            }
            return result;
        };
        var logical = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session));
        var call = client.ExecuteAsync(logical, deadline.Token);
        try
        {
            await entered.Task.WaitAsync(deadline.Token);
            var reload = client.ReloadCredentials(lab.Options(leader));
            var outcome = await call;
            Assert.Equal(DataGridClientFailureCode.OutcomeUnknown, outcome.Reply.Failure!.Code);
            Assert.True(outcome.MayHaveBeenAdmitted);
            Assert.Single(outcome.Attempts);
            await reload.Retired.WaitAsync(deadline.Token);
        }
        finally
        {
            release.TrySetResult();
        }
        Assert.Equal(original, Stored(await client.ExecuteAsync(logical, deadline.Token)));
        lab.Healthy();
    }

    [Fact(Timeout = 30000)]
    public async Task CoordinatedRootLeafPinAndClientRotationPreservesReplicatedState()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        await using var lab = new Lab();
        var leader = await lab.Cluster.Leader(deadline.Token);
        await using var client = new GridClient(lab.Options(leader));
        var session = await Open(client, deadline.Token);
        var logical = DataGridClientRequest.Mutate(DataGridClientCodecTests.Mutation(session));
        var original = Stored(await client.ExecuteAsync(logical, deadline.Token));
        using var replacement = new DataGridPeerFixture();
        using var newClient = replacement.Issue("catalog-rotated-client.example", clientOnly: true);
        var retired = new List<Task>();
        for (var i = 0; i < 3; i++)
        {
            var member = new DataGridPeerCredentials(
                lab.Peer.Configuration,
                i,
                replacement.Certificates[i],
                replacement.Root,
                replacement.Fingerprints,
                lab.Peer.Build
            );
            retired.Add(
                lab.Security[i]
                    .Reload(member, lab.Policy(root: replacement.Root, certificate: newClient))
                    .Retired
            );
        }
        retired.Add(
            client
                .ReloadCredentials(
                    lab.Options(leader, newClient, replacement.Root, replacement.Fingerprints)
                )
                .Retired
        );
        newClient.Dispose();
        replacement.Dispose(); // All active snapshots must own their material.
        await Task.WhenAll(retired).WaitAsync(deadline.Token);
        Assert.Equal(original, Stored(await client.ExecuteAsync(logical, deadline.Token)));
        var next = Stored(
            await client.ExecuteAsync(
                DataGridClientRequest.Mutate(
                    DataGridClientCodecTests.Mutation(
                        session,
                        2,
                        DataGridMutationKind.CompareExchange,
                        original,
                        9
                    )
                ),
                deadline.Token
            )
        );
        Assert.True(next.LogIndex > original.LogIndex);
        Assert.Equal(
            next,
            (await client.ExecuteAsync(Read(), deadline.Token)).Reply.Read!.GetItem(0)!.Version
        );
        Assert.All(lab.Security, security => Assert.Equal(2, security.Generation));
        lab.Healthy();
    }

    [Fact(Timeout = 30000)]
    public async Task PeerRevocationClosesAuthenticatedIdleConnectionBeforeRaftDispatch()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        using var fixture = new DataGridPeerFixture();
        await using var security = new DataGridSecurityContext(fixture.Credentials(1));
        var receiver = new PeerReceiver(fixture.Configuration);
        await using var server = new DataGridPeerTransport(
            security,
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(
            IPAddress.Loopback,
            fixture.Configuration.GetMember(1).Port,
            deadline.Token
        );
        using var tls = new SslStream(tcp.GetStream());
        await tls.AuthenticateAsClientAsync(
            fixture.Credentials(0).ClientOptions(1),
            deadline.Token
        );
        var hello = DataGridPeerCodec.Hello(
            fixture.Configuration,
            0,
            Guid.NewGuid(),
            fixture.Build
        );
        await DataGridPeerCodec.WriteAsync(tls, hello, deadline.Token);
        await DataGridPeerCodec.ReadAsync(tls, fixture.Configuration, 1, deadline.Token);
        var reload = security.Reload(
            fixture.Credentials(1, denied: [Convert.ToHexString(fixture.Fingerprints[0])])
        );
        await Closed(tls, deadline.Token);
        await reload.Retired.WaitAsync(deadline.Token);
        Assert.Equal(0, receiver.Calls);
        Assert.Null(server.FatalError);
    }

    [Theory(Timeout = 30000)]
    [InlineData(false)]
    [InlineData(true)]
    public async Task SnapshotSenderRevocationOrExpiryClosesBeforeInstallation(bool expire)
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        using var fixture = new DataGridPeerFixture();
        using var shortLeaf = fixture.Issue(
            "node-a.example",
            notAfter: DateTimeOffset.UtcNow.AddSeconds(4)
        );
        var pins = fixture.Fingerprints.Select(bytes => (byte[])bytes.Clone()).ToArray();
        pins[0] = shortLeaf.GetCertHash(HashAlgorithmName.SHA256);
        await using var security = new DataGridSecurityContext(
            fixture.Credentials(1, fingerprints: pins)
        );
        var entered = new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously);
        var receiver = new PeerReceiver(fixture.Configuration, entered);
        await using var server = new DataGridPeerTransport(
            security,
            fixture.Listeners[1],
            DataGridPeerFixture.Routes,
            receiver
        );
        server.Start();
        await using var client = new DataGridPeerTransport(
            fixture.Credentials(0, certificate: shortLeaf, fingerprints: pins),
            fixture.Listeners[0],
            DataGridPeerFixture.Routes,
            new PeerReceiver(fixture.Configuration)
        );
        var application = new DataGridLocalStateMachine(fixture.Configuration);
        application.Apply(1, DataGridCommittedCommand.AdvanceTime(new(1)));
        var image = application.SaveSnapshot(Guid.NewGuid(), 1);
        var request = new RaftMessage(
            RaftMessageKind.SnapshotRequest,
            0,
            1,
            3,
            new(1, 1),
            fixture.Configuration.IncarnationId,
            fixture.Configuration.ComputeDigest(),
            snapshot: image
        );
        var call = client.SendAsync(request, deadline.Token);
        await entered.Task.WaitAsync(deadline.Token);
        Assert.True(server.TransferActive);
        DataGridCredentialReload? reload = null;
        if (!expire)
            reload = security.Reload(
                fixture.Credentials(1, fingerprints: pins, denied: [Convert.ToHexString(pins[0])])
            );
        var error = await Record.ExceptionAsync(async () => await call);
        Assert.NotNull(error);
        using var expiryBound = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
        expiryBound.CancelAfter(
            shortLeaf.NotAfter.ToUniversalTime() - DateTime.UtcNow + TimeSpan.FromSeconds(5)
        );
        await receiver.Stopped.Task.WaitAsync(expiryBound.Token);
        reload ??= security.Reload(
            fixture.Credentials(1, fingerprints: pins, denied: [Convert.ToHexString(pins[0])])
        );
        await reload.Retired.WaitAsync(deadline.Token);
        Assert.False(server.TransferActive);
        Assert.Equal(0, receiver.Calls);
        Assert.Null(server.FatalError);
    }

    [Fact(Timeout = 30000)]
    public async Task RemoteCertificateExpiryClosesExistingClientWithinFiveSeconds()
    {
        RequireLinux();
        using var deadline = Deadline(TestContext.Current.CancellationToken);
        using var fixture = new DataGridPeerFixture();
        using var certificate = fixture.Issue(
            "catalog-expiring.example",
            clientOnly: true,
            notAfter: DateTimeOffset.UtcNow.AddSeconds(4)
        );
        var policy = new DataGridClientAccessPolicy(
            fixture.Configuration,
            fixture.Root,
            [
                new(
                    DataGridPeerFixture.Principal,
                    certificate.GetCertHash(HashAlgorithmName.SHA256),
                    true,
                    [new(DataGridPeerFixture.Map, DataGridMapActions.Read)]
                ),
            ]
        );
        await using var security = new DataGridSecurityContext(fixture.Credentials(0), policy);
        using var listener = new TcpListener(IPAddress.Loopback, 0);
        await using var server = new DataGridClientServer(
            security,
            listener,
            new MissingReceiver()
        );
        server.Start();
        using var tcp = new TcpClient();
        await tcp.ConnectAsync(
            IPAddress.Loopback,
            ((IPEndPoint)listener.LocalEndpoint).Port,
            deadline.Token
        );
        using var tls = new SslStream(tcp.GetStream());
        await tls.AuthenticateAsClientAsync(ClientOptions(fixture, 0, certificate), deadline.Token);
        await Hello(fixture, 0, tls, deadline.Token);
        using var expiryBound = CancellationTokenSource.CreateLinkedTokenSource(deadline.Token);
        expiryBound.CancelAfter(
            certificate.NotAfter.ToUniversalTime() - DateTime.UtcNow + TimeSpan.FromSeconds(5)
        );
        await Closed(tls, expiryBound.Token);
        Assert.Null(server.FatalError);
    }

    private static void RequireLinux() =>
        Assert.SkipUnless(
            OperatingSystem.IsLinux(),
            "TLS 1.3 credential lifecycle evidence requires Linux."
        );

    private static CancellationTokenSource Deadline(CancellationToken token)
    {
        var source = CancellationTokenSource.CreateLinkedTokenSource(token);
        source.CancelAfter(TimeSpan.FromSeconds(25));
        return source;
    }

    private static DataGridClientRequest Read() =>
        DataGridClientRequest.Read(DataGridPeerFixture.Map, [DataGridPeerFixture.Key]);

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

    private static DataGridEntryVersion Stored(DataGridClientOutcome result)
    {
        Assert.Null(result.Reply.Failure);
        Assert.Equal(DataGridMutationStatus.Stored, result.Reply.Result!.GetItem(0).Status);
        return result.Reply.Result.GetItem(0).Version;
    }

    private static async Task Closed(SslStream tls, CancellationToken token)
    {
        try
        {
            var bytes = new byte[1];
            Assert.Equal(0, await tls.ReadAsync(bytes, token));
        }
        catch (Exception exception) when (exception is IOException or AuthenticationException) { }
    }

    private static SslClientAuthenticationOptions ClientOptions(
        DataGridPeerFixture fixture,
        int member,
        X509Certificate2 certificate
    ) =>
        new()
        {
            EnabledSslProtocols = SslProtocols.Tls13,
            TargetHost = fixture.Configuration.GetMember(member).DnsName,
            ClientCertificates = new X509CertificateCollection { certificate },
            CertificateChainPolicy = DataGridTlsSupport.ChainPolicy(fixture.Root, server: true),
            AllowRenegotiation = false,
            AllowTlsResume = false,
            ApplicationProtocols = [DataGridClientServer.Protocol],
        };

    private static async Task<SslStream> Connect(
        Lab lab,
        int member,
        TcpClient tcp,
        CancellationToken token
    )
    {
        await tcp.ConnectAsync(IPAddress.Loopback, lab.Ports[member], token);
        var tls = new SslStream(tcp.GetStream());
        try
        {
            await tls.AuthenticateAsClientAsync(
                ClientOptions(lab.Peer, member, lab.Certificate),
                token
            );
            await Hello(lab.Peer, member, tls, token);
            return tls;
        }
        catch
        {
            await tls.DisposeAsync();
            throw;
        }
    }

    private static async Task Hello(
        DataGridPeerFixture fixture,
        int member,
        SslStream tls,
        CancellationToken token
    )
    {
        var hello = DataGridClientCodec.Hello(
            fixture.Configuration.IncarnationId,
            DataGridPeerFixture.Principal,
            Guid.NewGuid(),
            fixture.Build,
            fixture.Configuration.ComputeDigest()
        );
        await DataGridTlsSupport.WriteAsync(tls, hello, token);
        var response = await DataGridTlsSupport.ReadAsync(
            tls,
            fixture.Configuration.IncarnationId,
            fixture.Configuration.GetMember(member).MemberId,
            token
        );
        Assert.Equal(hello.AttemptId, response.AttemptId);
    }

    private sealed class MissingReceiver : IDataGridClientReceiver
    {
        public Task<DataGridClientReply> ExecuteAsync(
            Guid principal,
            DataGridClientRequest request,
            CancellationToken cancellationToken
        ) =>
            Task.FromResult(
                DataGridClientReply.Completed(
                    DataGridMessageKind.ReadResult,
                    read: new(new(1), [null])
                )
            );
    }

    private sealed class PeerReceiver(
        DataGridBootstrapConfiguration configuration,
        TaskCompletionSource? entered = null
    ) : IDataGridPeerReceiver
    {
        internal int Calls { get; private set; }
        internal TaskCompletionSource Stopped { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);

        public async Task<long> ObserveLeaderAsync(long term, CancellationToken cancellationToken)
        {
            if (entered is not null)
            {
                entered.TrySetResult();
                try
                {
                    await new TaskCompletionSource(
                        TaskCreationOptions.RunContinuationsAsynchronously
                    ).Task.WaitAsync(cancellationToken);
                }
                finally
                {
                    Stopped.TrySetResult();
                }
            }
            return term;
        }

        public Task<RaftMessage?> ReceiveAsync(
            RaftMessage message,
            CancellationToken cancellationToken
        )
        {
            Calls++;
            return Task.FromResult<RaftMessage?>(
                new(
                    message.Kind == RaftMessageKind.VoteRequest
                        ? RaftMessageKind.VoteResponse
                        : RaftMessageKind.AppendResponse,
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

    internal sealed class Lab : IAsyncDisposable
    {
        internal DataGridPeerFixture Peer { get; } = new();
        internal X509Certificate2 Certificate { get; }
        internal DataGridSecurityContext[] Security { get; }
        internal DataGridPeerTlsTests.LiveCluster Cluster { get; }
        internal DataGridClientServer[] Servers { get; }
        internal int[] Ports { get; }
        internal Func<
            int,
            Guid,
            DataGridClientRequest,
            CancellationToken,
            Task<DataGridClientReply>
        >? Hook { get; set; }

        internal Lab()
        {
            Certificate = Peer.Issue("catalog-client.example", clientOnly: true);
            Security = Enumerable
                .Range(0, 3)
                .Select(i => new DataGridSecurityContext(Peer.Credentials(i), Policy()))
                .ToArray();
            Cluster = new(Peer, Security);
            Cluster.StartAll();
            var listeners = Enumerable
                .Range(0, 3)
                .Select(_ => new TcpListener(IPAddress.Loopback, 0))
                .ToArray();
            Servers = Enumerable.Range(0, 3).Select(i => CreateServer(i, listeners[i])).ToArray();
            foreach (var server in Servers)
                server.Start();
            Ports = listeners
                .Select(listener => ((IPEndPoint)listener.LocalEndpoint).Port)
                .ToArray();
        }

        internal DataGridClientServer CreateServer(int member, TcpListener listener) =>
            new(Security[member], listener, new Receiver(this, member));

        internal DataGridClientAccessPolicy Policy(
            bool denied = false,
            X509Certificate2? root = null,
            X509Certificate2? certificate = null
        ) =>
            new(
                Peer.Configuration,
                root ?? Peer.Root,
                [
                    new(
                        DataGridPeerFixture.Principal,
                        (certificate ?? Certificate).GetCertHash(HashAlgorithmName.SHA256),
                        true,
                        [
                            new(
                                DataGridPeerFixture.Map,
                                DataGridMapActions.Read | DataGridMapActions.Mutate
                            ),
                        ]
                    ),
                ],
                denied
                    ? [Convert.ToHexString(Certificate.GetCertHash(HashAlgorithmName.SHA256))]
                    : null
            );

        internal DataGridClientOptions Options(
            int member,
            X509Certificate2? certificate = null,
            X509Certificate2? root = null,
            byte[][]? pins = null
        ) =>
            new(
                Peer.Configuration,
                DataGridPeerFixture.Principal,
                certificate ?? Certificate,
                root ?? Peer.Root,
                Enumerable
                    .Range(0, 3)
                    .Select(i => new DataGridClientEndpoint(
                        Peer.Configuration.GetMember(i).MemberId,
                        Peer.Configuration.GetMember(i).DnsName,
                        (ushort)Ports[i],
                        (pins ?? Peer.Fingerprints)[i],
                        IPAddress.Loopback
                    ))
                    .ToArray(),
                Peer.Build,
                member
            );

        internal void Healthy()
        {
            Assert.All(Servers, server => Assert.Null(server.FatalError));
            Assert.All(Cluster.Nodes, node => Assert.Null(node!.Failure));
        }

        public async ValueTask DisposeAsync()
        {
            await Task.WhenAll(Servers.Select(server => server.DisposeAsync().AsTask()));
            await Cluster.DisposeAsync();
            await Task.WhenAll(Security.Select(security => security.DisposeAsync().AsTask()));
            Certificate.Dispose();
            Peer.Dispose();
        }

        private sealed class Receiver(Lab lab, int member) : IDataGridClientReceiver
        {
            public Task<DataGridClientReply> ExecuteAsync(
                Guid principal,
                DataGridClientRequest request,
                CancellationToken cancellationToken
            ) =>
                lab.Hook is { } hook
                    ? hook(member, principal, request, cancellationToken)
                    : lab.Cluster.Nodes[member]!.ExecuteAsync(
                        principal,
                        request,
                        cancellationToken
                    );
        }
    }
}
