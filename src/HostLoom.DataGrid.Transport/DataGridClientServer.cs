using System.Diagnostics.CodeAnalysis;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;

namespace HostLoom.DataGrid.Transport;

internal interface IDataGridClientReceiver
{
    Task<DataGridClientReply> ExecuteAsync(
        Guid principal,
        DataGridClientRequest request,
        CancellationToken cancellationToken
    );
}

/// <summary>Role-separated unary client listener. Admission occurs only after TLS, hello, decoding and grants.</summary>
[SuppressMessage(
    "Security",
    "CA5398:Avoid hardcoded SslProtocols values",
    Justification = "The v1 contract requires TLS 1.3 and forbids downgrade."
)]
internal sealed class DataGridClientServer : IAsyncDisposable
{
    internal static readonly SslApplicationProtocol Protocol = new("hostloom-grid-client-v1");
    private readonly DataGridSecurityContext _security;
    private readonly bool _ownsSecurity;
    private readonly TcpListener _listener;
    private readonly IDataGridClientReceiver _receiver;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _connections = new(112, 112);
    private readonly SemaphoreSlim _handshakes = new(12, 12);
    private readonly SemaphoreSlim _requests = new(12, 12); // 24 MiB reserved encoded request/response work.
    private readonly DataGridPrincipalAdmission _principals = new();
    private readonly object _sync = new();
    private readonly Dictionary<long, Task> _sessions = [];
    private Task? _accept,
        _shutdown;
    private long _id;
    private readonly DataGridHandshakeRate _rate = new(8, DataGridTransportRole.Client);
    private int _rejected;

    internal DataGridClientServer(
        DataGridPeerCredentials member,
        DataGridClientAccessPolicy policy,
        TcpListener listener,
        IDataGridClientReceiver receiver
    )
        : this(
            CreateSecurity(member, policy, listener, receiver),
            listener,
            receiver,
            ownsSecurity: true
        ) { }

    internal DataGridClientServer(
        DataGridSecurityContext security,
        TcpListener listener,
        IDataGridClientReceiver receiver
    )
        : this(security, listener, receiver, ownsSecurity: false) { }

    private DataGridClientServer(
        DataGridSecurityContext security,
        TcpListener listener,
        IDataGridClientReceiver receiver,
        bool ownsSecurity
    )
    {
        if (!OperatingSystem.IsLinux())
            throw new PlatformNotSupportedException("TLS 1.3 server prototype requires Linux.");
        if (!security.HasClients)
            throw new ArgumentException("Client security policy required.", nameof(security));
        _security = security;
        _ownsSecurity = ownsSecurity;
        _listener = listener;
        _receiver = receiver;
    }

    private static DataGridSecurityContext CreateSecurity(
        DataGridPeerCredentials member,
        DataGridClientAccessPolicy policy,
        TcpListener listener,
        IDataGridClientReceiver receiver
    )
    {
        if (!OperatingSystem.IsLinux())
            throw new PlatformNotSupportedException("TLS 1.3 server prototype requires Linux.");
        ArgumentNullException.ThrowIfNull(policy);
        ArgumentNullException.ThrowIfNull(listener);
        ArgumentNullException.ThrowIfNull(receiver);
        return new(member, policy);
    }

    internal int RejectedConnections => Volatile.Read(ref _rejected);
    internal Exception? FatalError { get; private set; }

    internal void Start()
    {
        if (_accept is not null)
            throw new InvalidOperationException("Client listener already started.");
        _listener.Start(112);
        _accept = Accept();
    }

    private bool StartHandshake() => _rate.TryAcquire(DataGridTransportDirection.Inbound);

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The bounded session task owns the pre-reserved handshake lease and disposes it in finally, including failed TLS/hello."
    )]
    private async Task Accept()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var tcp = await _listener.AcceptTcpClientAsync(_stop.Token).ConfigureAwait(false);
                var connection = DataGridTransportLease.TryAcquire(
                    _connections,
                    DataGridTransportRole.Client,
                    DataGridTransportDirection.Inbound,
                    DataGridTransportResource.Connection
                );
                if (connection is null)
                {
                    tcp.Dispose();
                    Interlocked.Increment(ref _rejected);
                    continue;
                }
                if (!StartHandshake())
                {
                    tcp.Dispose();
                    connection.Dispose();
                    Interlocked.Increment(ref _rejected);
                    continue;
                }
                DataGridTransportLease handshake;
                try
                {
                    handshake = DataGridTransportLease.Acquire(
                        _handshakes,
                        DataGridTransportRole.Client,
                        DataGridTransportDirection.Inbound,
                        DataGridTransportResource.Handshake,
                        token: _stop.Token
                    );
                }
                catch (Exception exception)
                    when (exception is IOException or OperationCanceledException)
                {
                    tcp.Dispose();
                    connection.Dispose();
                    Interlocked.Increment(ref _rejected);
                    continue;
                }
                lock (_sync)
                {
                    var id = ++_id;
                    _sessions.Add(
                        id,
                        Task.Run(async () =>
                        {
                            Exception? failure = null;
                            try
                            {
                                await Serve(tcp, handshake).ConfigureAwait(false);
                            }
                            catch (Exception exception)
                                when (exception
                                        is IOException
                                            or AuthenticationException
                                            or OperationCanceledException
                                            or SocketException
                                            or InvalidDataException
                                            or ObjectDisposedException
                                )
                            {
                                failure = exception;
                                Interlocked.Increment(ref _rejected);
                            }
                            catch (Exception exception)
                            {
                                failure = exception;
                                FatalError = exception;
                                await _stop.CancelAsync().ConfigureAwait(false);
                            }
                            finally
                            {
                                DataGridTransportDiagnostics.Exchange(
                                    DataGridTransportRole.Client,
                                    DataGridTransportDirection.Inbound,
                                    failure
                                );
                                handshake.Dispose();
                                tcp.Dispose();
                                connection.Dispose();
                                lock (_sync)
                                    _sessions.Remove(id);
                            }
                        })
                    );
                }
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (Exception exception)
        {
            FatalError = exception;
            await _stop.CancelAsync().ConfigureAwait(false);
        }
    }

    private async Task Serve(TcpClient tcp, DataGridTransportLease handshake)
    {
        using var security = _security.Acquire(tcp);
        var member = security.Material.Member;
        var policy = security.Material.Clients!;
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(
            _stop.Token,
            security.Revoked
        );
        lifetime.CancelAfter(TimeSpan.FromSeconds(5));
        var token = lifetime.Token;
        tcp.NoDelay = true;
        using var tls = new SslStream(tcp.GetStream());
        await tls.AuthenticateAsServerAsync(
                new SslServerAuthenticationOptions
                {
                    EnabledSslProtocols = SslProtocols.Tls13,
                    ServerCertificate = member.Certificate,
                    ClientCertificateRequired = true,
                    CertificateChainPolicy = DataGridTlsSupport.ChainPolicy(
                        policy.Root,
                        server: false
                    ),
                    AllowRenegotiation = false,
                    AllowTlsResume = false,
                    ApplicationProtocols = [Protocol],
                    RemoteCertificateValidationCallback = (_, certificate, _, errors) =>
                        errors == SslPolicyErrors.None
                        && certificate is X509Certificate2 leaf
                        && policy.Resolve(leaf) >= 0,
                },
                token
            )
            .ConfigureAwait(false);
        if (
            !tls.IsMutuallyAuthenticated
            || tls.SslProtocol != SslProtocols.Tls13
            || tls.NegotiatedApplicationProtocol != Protocol
            || tls.RemoteCertificate is not X509Certificate2 remote
        )
            throw new AuthenticationException("Client TLS identity rejected.");
        var principalIndex = policy.Resolve(remote);
        if (principalIndex < 0)
            throw new AuthenticationException("Client binding rejected.");
        security.RemoteExpiresAt(remote.NotAfter.ToUniversalTime());
        var principal = policy.Principal(principalIndex);
        using var principalLease = _principals.Acquire(principal.PrincipalId, token);
        var hello = await DataGridTlsSupport
            .ReadAsync(tls, member.Configuration.IncarnationId, principal.PrincipalId, token)
            .ConfigureAwait(false);
        DataGridClientCodec.ValidateHello(hello, member.Configuration);
        await DataGridTlsSupport
            .WriteAsync(
                tls,
                DataGridClientCodec.Hello(
                    member.Configuration.IncarnationId,
                    member.Configuration.GetMember(member.LocalMember).MemberId,
                    hello.AttemptId,
                    member.BuildDigest,
                    member.Configuration.ComputeDigest()
                ),
                token
            )
            .ConfigureAwait(false);
        handshake.Dispose();
        using var requestLease = DataGridTransportLease.Acquire(
            _requests,
            DataGridTransportRole.Client,
            DataGridTransportDirection.Inbound,
            DataGridTransportResource.Request,
            2L * 1024 * 1024,
            token
        );
        var frame = await DataGridTlsSupport
            .ReadAsync(tls, member.Configuration.IncarnationId, principal.PrincipalId, token)
            .ConfigureAwait(false);
        if (policy.Resolve(remote) != principalIndex)
            throw new AuthenticationException("Client credential expired/revoked.");
        DataGridClientRequest request;
        try
        {
            request = DataGridClientCodec.DecodeRequest(frame);
        }
        catch (InvalidDataException)
        {
            DataGridTransportDiagnostics.Reject(
                DataGridTransportRole.Client,
                DataGridTransportDirection.Inbound,
                DataGridTransportResource.Request,
                DataGridTransportRejection.InvalidRequest
            );
            using var writer = new GridWriter(22);
            writer.Number((ulong)DataGridClientFailureCode.InvalidRequest, 2);
            writer.Number(0, 1);
            writer.Number(0, 1);
            writer.Id(Guid.Empty);
            writer.Number(0, 2);
            await DataGridTlsSupport
                .WriteAsync(
                    tls,
                    new(
                        DataGridMessageKind.Failure,
                        frame.IncarnationId,
                        member.Configuration.GetMember(member.LocalMember).MemberId,
                        frame.SessionNumber,
                        frame.RequestNumber,
                        frame.AttemptId,
                        writer.ToArray()
                    ),
                    token
                )
                .ConfigureAwait(false);
            return;
        }
        var authorized = principal.Allows(request);
        security.Check(); // Fully validated admission is ordered against generation publication.
        if (!authorized)
            DataGridTransportDiagnostics.Reject(
                DataGridTransportRole.Client,
                DataGridTransportDirection.Inbound,
                DataGridTransportResource.Request,
                DataGridTransportRejection.Unauthorized
            );
        var reply = authorized
            ? await _receiver
                .ExecuteAsync(principal.PrincipalId, request, token)
                .ConfigureAwait(false)
            : DataGridClientReply.Failed(new(DataGridClientFailureCode.Unauthorized, false));
        await DataGridTlsSupport
            .WriteAsync(
                tls,
                DataGridClientCodec.EncodeReply(
                    request,
                    reply,
                    frame.IncarnationId,
                    member.Configuration.GetMember(member.LocalMember).MemberId,
                    frame.AttemptId
                ),
                token
            )
            .ConfigureAwait(false);
    }

    public ValueTask DisposeAsync()
    {
        lock (_sync)
            return new(_shutdown ??= Shutdown());
    }

    private async Task Shutdown()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        if (_accept is not null)
            await _accept.ConfigureAwait(false);
        Task[] tasks;
        lock (_sync)
            tasks = _sessions.Values.ToArray();
        await Task.WhenAll(tasks).ConfigureAwait(false);
        if (_ownsSecurity)
            await _security.DisposeAsync().ConfigureAwait(false);
        _connections.Dispose();
        _requests.Dispose();
        _handshakes.Dispose();
        _stop.Dispose();
    }
}
