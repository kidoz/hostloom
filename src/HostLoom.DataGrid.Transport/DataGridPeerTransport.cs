using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid.Consensus;

namespace HostLoom.DataGrid.Transport;

internal interface IDataGridPeerReceiver
{
    Task<RaftMessage?> ReceiveAsync(RaftMessage message, CancellationToken cancellationToken);
    Task<long> ObserveLeaderAsync(long term, CancellationToken cancellationToken);
}

/// <summary>One bounded RPC per TLS connection. No retries are hidden below the Raft owner.</summary>
[SuppressMessage(
    "Security",
    "CA5398:Avoid hardcoded SslProtocols values",
    Justification = "The v1 peer contract requires exactly TLS 1.3 and verifies the negotiated protocol."
)]
internal sealed class DataGridPeerTransport : IAsyncDisposable
{
    private readonly DataGridSecurityContext _security;
    private readonly bool _ownsSecurity;
    private readonly DataGridBootstrapConfiguration _config;
    private readonly int _local;
    private readonly TcpListener _listener;
    private readonly IPAddress[] _addresses;
    private readonly IDataGridPeerReceiver _receiver;
    private readonly CancellationTokenSource _stop = new();
    private readonly SemaphoreSlim _handshakes = new(4, 4);
    private readonly SemaphoreSlim _connections = new(16, 16);
    private readonly SemaphoreSlim[] _members = [new(2, 2), new(2, 2), new(2, 2)];
    private readonly SemaphoreSlim _transfer = new(1, 1);
    private readonly object _sync = new();
    private readonly Dictionary<long, Task> _sessions = [];
    private Task? _accept;
    private long _sessionId;
    private long _rateTime = TimeProvider.System.GetTimestamp();
    private double _rateTokens = 24;
    private int _rejected;
    private Task? _shutdown;

    internal DataGridPeerTransport(
        DataGridPeerCredentials credentials,
        TcpListener listener,
        IReadOnlyList<IPAddress> peerAddresses,
        IDataGridPeerReceiver receiver
    )
        : this(
            CreateSecurity(credentials, listener, peerAddresses, receiver),
            listener,
            peerAddresses,
            receiver,
            ownsSecurity: true
        ) { }

    internal DataGridPeerTransport(
        DataGridSecurityContext security,
        TcpListener listener,
        IReadOnlyList<IPAddress> peerAddresses,
        IDataGridPeerReceiver receiver
    )
        : this(security, listener, peerAddresses, receiver, ownsSecurity: false) { }

    private DataGridPeerTransport(
        DataGridSecurityContext security,
        TcpListener listener,
        IReadOnlyList<IPAddress> peerAddresses,
        IDataGridPeerReceiver receiver,
        bool ownsSecurity
    )
    {
        ValidateRoutes(
            security.Configuration,
            security.LocalMember,
            listener,
            peerAddresses,
            receiver
        );
        _security = security;
        _ownsSecurity = ownsSecurity;
        _config = security.Configuration;
        _local = security.LocalMember;
        _listener = listener;
        _addresses = peerAddresses.ToArray();
        _receiver = receiver;
    }

    private static DataGridSecurityContext CreateSecurity(
        DataGridPeerCredentials credentials,
        TcpListener listener,
        IReadOnlyList<IPAddress> addresses,
        IDataGridPeerReceiver receiver
    )
    {
        ValidateRoutes(
            credentials.Configuration,
            credentials.LocalMember,
            listener,
            addresses,
            receiver
        );
        return new(credentials);
    }

    private static void ValidateRoutes(
        DataGridBootstrapConfiguration configuration,
        int local,
        TcpListener listener,
        IReadOnlyList<IPAddress> addresses,
        IDataGridPeerReceiver receiver
    )
    {
        if (!OperatingSystem.IsLinux())
            throw new PlatformNotSupportedException("TLS 1.3 server experiments require Linux.");
        ArgumentNullException.ThrowIfNull(listener);
        ArgumentNullException.ThrowIfNull(addresses);
        ArgumentNullException.ThrowIfNull(receiver);
        if (
            addresses.Count != 3
            || ((IPEndPoint)listener.LocalEndpoint).Port != configuration.GetMember(local).Port
        )
            throw new ArgumentException("Three routes and the configured listener port required.");
    }

    internal int RejectedConnections => Volatile.Read(ref _rejected);
    internal int ActiveConnections => 16 - _connections.CurrentCount;
    internal bool TransferActive => _transfer.CurrentCount == 0;
    internal Exception? FatalError { get; private set; }

    internal void Start()
    {
        if (_accept is not null)
            throw new InvalidOperationException("Transport already started.");
        _listener.Start(16);
        _accept = AcceptAsync();
    }

    internal async Task<RaftMessage> SendAsync(
        RaftMessage message,
        CancellationToken cancellationToken
    )
    {
        if (
            !message.Matches(_config)
            || message.From != _local
            || message.To is < 0 or > 2
            || message.To == _local
        )
            throw new InvalidDataException("Invalid outgoing peer route.");
        using var security = _security.Acquire();
        var credentials = security.Material.Member;
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(
            cancellationToken,
            _stop.Token,
            security.Revoked
        );
        lifetime.CancelAfter(
            message.Kind == RaftMessageKind.SnapshotRequest
                ? TimeSpan.FromSeconds(60)
                : TimeSpan.FromSeconds(5)
        );
        var token = lifetime.Token;
        if (!_connections.Wait(0, cancellationToken))
            throw new IOException("Peer connection capacity.");
        var memberHeld = false;
        try
        {
            memberHeld = _members[message.To].Wait(0, token);
            if (!memberHeld || !StartHandshake())
                throw new IOException("Peer admission capacity.");
            using var handshake = new DataGridTransportLease(_handshakes, token);
            using var client = new TcpClient { NoDelay = true };
            security.Attach(client);
            await client
                .ConnectAsync(_addresses[message.To], _config.GetMember(message.To).Port, token)
                .ConfigureAwait(false);
            using var tls = new SslStream(client.GetStream(), leaveInnerStreamOpen: false);
            await tls.AuthenticateAsClientAsync(credentials.ClientOptions(message.To), token)
                .ConfigureAwait(false);
            CheckTls(tls, message.To, security);
            var hello = DataGridPeerCodec.Hello(
                _config,
                _local,
                Guid.NewGuid(),
                credentials.BuildDigest
            );
            await Write(tls, hello, token).ConfigureAwait(false);
            var reply = await Read(tls, message.To, security, token).ConfigureAwait(false);
            if (reply.AttemptId != hello.AttemptId)
                throw new InvalidDataException("Mismatched hello attempt.");
            DataGridPeerCodec.ValidateHello(reply, _config, credentials.BuildDigest);
            handshake.Dispose();
            if (message.Kind == RaftMessageKind.SnapshotRequest)
                return await SendSnapshot(tls, message, security, token).ConfigureAwait(false);
            security.Check();
            await Write(tls, DataGridPeerCodec.Encode(message, _config), token)
                .ConfigureAwait(false);
            var response = await Read(tls, message.To, security, token).ConfigureAwait(false);
            if (
                response.AttemptId != DataGridPeerCodec.Attempt(message.Token)
                || response.Kind
                    != (
                        message.Kind == RaftMessageKind.VoteRequest
                            ? DataGridMessageKind.VoteResponse
                            : DataGridMessageKind.AppendResponse
                    )
            )
                throw new InvalidDataException("Mismatched peer response.");
            return DataGridPeerCodec.Decode(response, _config, message.To, _local, response: true);
        }
        catch (ObjectDisposedException) when (security.IsRetired)
        {
            throw new DataGridCredentialRetiredException();
        }
        finally
        {
            if (memberHeld)
                _members[message.To].Release();
            _connections.Release();
        }
    }

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The bounded session task owns the pre-reserved handshake lease and disposes it in finally, including failed TLS/hello."
    )]
    private async Task AcceptAsync()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener
                    .AcceptTcpClientAsync(_stop.Token)
                    .ConfigureAwait(false);
                if (!_connections.Wait(0))
                {
                    client.Dispose();
                    Interlocked.Increment(ref _rejected);
                    continue;
                }
                if (!StartHandshake())
                {
                    client.Dispose();
                    _connections.Release();
                    Interlocked.Increment(ref _rejected);
                    continue;
                }
                DataGridTransportLease handshake;
                try
                {
                    handshake = new(_handshakes, _stop.Token);
                }
                catch (Exception exception)
                    when (exception is IOException or OperationCanceledException)
                {
                    client.Dispose();
                    _connections.Release();
                    Interlocked.Increment(ref _rejected);
                    continue;
                }
                lock (_sync)
                {
                    var id = ++_sessionId;
                    _sessions.Add(
                        id,
                        Task.Run(async () =>
                        {
                            try
                            {
                                await Serve(client, handshake).ConfigureAwait(false);
                            }
                            catch (Exception exception)
                                when (exception
                                        is IOException
                                            or AuthenticationException
                                            or OperationCanceledException
                                            or InvalidDataException
                                            or SocketException
                                            or ObjectDisposedException
                                )
                            {
                                Interlocked.Increment(ref _rejected);
                            }
                            catch (Exception exception)
                            {
                                FatalError = exception;
                                await _stop.CancelAsync().ConfigureAwait(false);
                            }
                            finally
                            {
                                handshake.Dispose();
                                client.Dispose();
                                _connections.Release();
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

    private async Task Serve(TcpClient client, DataGridTransportLease handshake)
    {
        using var security = _security.Acquire(client);
        var credentials = security.Material.Member;
        using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(
            _stop.Token,
            security.Revoked
        );
        lifetime.CancelAfter(TimeSpan.FromSeconds(5)); // Includes TLS and validated hello.
        client.NoDelay = true;
        using var tls = new SslStream(client.GetStream(), leaveInnerStreamOpen: false);
        await tls.AuthenticateAsServerAsync(credentials.ServerOptions(), lifetime.Token)
            .ConfigureAwait(false);
        var remote = tls.RemoteCertificate is X509Certificate2 certificate
            ? credentials.Resolve(certificate)
            : -1;
        CheckTls(tls, remote, security);
        if (!_members[remote].Wait(0))
            throw new IOException("Member connection capacity.");
        try
        {
            var hello = await Read(tls, remote, security, lifetime.Token).ConfigureAwait(false);
            DataGridPeerCodec.ValidateHello(hello, _config, credentials.BuildDigest);
            await Write(
                    tls,
                    DataGridPeerCodec.Hello(
                        _config,
                        _local,
                        hello.AttemptId,
                        credentials.BuildDigest
                    ),
                    lifetime.Token
                )
                .ConfigureAwait(false);
            handshake.Dispose();
            var request = await Read(tls, remote, security, lifetime.Token).ConfigureAwait(false);
            if (request.Kind == DataGridMessageKind.SnapshotStart)
            {
                lifetime.CancelAfter(TimeSpan.FromSeconds(60));
                await ReceiveSnapshot(tls, request, remote, security, lifetime.Token)
                    .ConfigureAwait(false);
                return;
            }
            var message = DataGridPeerCodec.Decode(
                request,
                _config,
                remote,
                _local,
                response: false
            );
            security.Check();
            var response = await _receiver
                .ReceiveAsync(message, lifetime.Token)
                .ConfigureAwait(false);
            if (response is not null)
                await Write(tls, DataGridPeerCodec.Encode(response, _config), lifetime.Token)
                    .ConfigureAwait(false);
        }
        finally
        {
            _members[remote].Release();
        }
    }

    private void CheckTls(
        SslStream tls,
        int remote,
        DataGridCredentialStore<DataGridSecuritySnapshot>.Lease security
    )
    {
        security.Check();
        var credentials = security.Material.Member;
        if (
            remote is < 0 or > 2
            || remote == _local
            || !tls.IsMutuallyAuthenticated
            || !tls.IsEncrypted
            || tls.SslProtocol != SslProtocols.Tls13
            || tls.NegotiatedApplicationProtocol != DataGridPeerCredentials.Protocol
            || tls.RemoteCertificate is not X509Certificate2 certificate
            || credentials.Resolve(certificate) != remote
        )
            throw new AuthenticationException("Member TLS binding rejected.");
        security.RemoteExpiresAt(certificate.NotAfter.ToUniversalTime());
    }

    private ValueTask<DataGridProtocolFrame> Read(
        SslStream tls,
        int remote,
        DataGridCredentialStore<DataGridSecuritySnapshot>.Lease security,
        CancellationToken token
    )
    {
        CheckTls(tls, remote, security);
        return DataGridPeerCodec.ReadAsync(tls, _config, remote, token);
    }

    private static ValueTask Write(
        SslStream tls,
        DataGridProtocolFrame frame,
        CancellationToken token
    ) => DataGridPeerCodec.WriteAsync(tls, frame, token);

    private bool StartHandshake()
    {
        lock (_sync)
        {
            var now = TimeProvider.System.GetTimestamp();
            _rateTokens = Math.Min(
                24,
                _rateTokens + TimeProvider.System.GetElapsedTime(_rateTime, now).TotalSeconds * 24
            );
            _rateTime = now;
            if (_rateTokens < 1)
                return false;
            _rateTokens--;
            return true;
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_sync)
            return new(_shutdown ??= ShutdownAsync());
    }

    private async Task ShutdownAsync()
    {
        await _stop.CancelAsync().ConfigureAwait(false);
        _listener.Stop();
        if (_accept is not null)
            await _accept.ConfigureAwait(false);
        Task[] sessions;
        lock (_sync)
            sessions = _sessions.Values.ToArray();
        await Task.WhenAll(sessions).ConfigureAwait(false);
        // Outgoing calls are owned and drained by the runtime before releasing this transport.
        if (_ownsSecurity)
            await _security.DisposeAsync().ConfigureAwait(false);
        _stop.Dispose();
        _connections.Dispose();
        _handshakes.Dispose();
        _transfer.Dispose();
        foreach (var member in _members)
            member.Dispose();
    }

    private async Task<RaftMessage> SendSnapshot(
        SslStream tls,
        RaftMessage message,
        DataGridCredentialStore<DataGridSecuritySnapshot>.Lease security,
        CancellationToken token
    )
    {
        var snapshot = message.Snapshot ?? throw new InvalidDataException("Missing snapshot.");
        using var start = new GridWriter(80);
        start.Number((ulong)message.Term, 8);
        start.Raw(snapshot.Descriptor.Encode());
        var attempt = DataGridPeerCodec.Attempt(message.Token);
        await Write(
                tls,
                DataGridPeerCodec.Frame(
                    _config,
                    _local,
                    DataGridMessageKind.SnapshotStart,
                    attempt,
                    start.ToArray()
                ),
                token
            )
            .ConfigureAwait(false);
        var result = ReadSnapshotResponse(
            await Read(tls, message.To, security, token).ConfigureAwait(false),
            attempt,
            snapshot.Descriptor.SnapshotId,
            message.Term
        );
        if (!result.Accepted)
            return SnapshotReply(message, result.Term, false);
        if (result.Installed || result.Offset != 0)
            throw new InvalidDataException("Invalid snapshot start acknowledgement.");
        var bytes = snapshot.GetContent();
        for (var offset = 0; offset < bytes.Length; )
        {
            security.Check();
            var count = Math.Min(65536, bytes.Length - offset);
            using var chunk = new GridWriter(65564);
            chunk.Id(snapshot.Descriptor.SnapshotId);
            chunk.Number((ulong)offset, 8);
            chunk.Number((ulong)count, 4);
            chunk.Raw(bytes.AsSpan(offset, count));
            await Write(
                    tls,
                    DataGridPeerCodec.Frame(
                        _config,
                        _local,
                        DataGridMessageKind.SnapshotChunk,
                        attempt,
                        chunk.ToArray()
                    ),
                    token
                )
                .ConfigureAwait(false);
            result = ReadSnapshotResponse(
                await Read(tls, message.To, security, token).ConfigureAwait(false),
                attempt,
                snapshot.Descriptor.SnapshotId,
                message.Term
            );
            if (!result.Accepted)
                return SnapshotReply(message, result.Term, false);
            offset += count;
            if (result.Offset != offset || result.Installed != (offset == bytes.Length))
                throw new InvalidDataException("Invalid snapshot progress acknowledgement.");
        }
        return SnapshotReply(message, result.Term, true);
    }

    private async Task ReceiveSnapshot(
        SslStream tls,
        DataGridProtocolFrame start,
        int remote,
        DataGridCredentialStore<DataGridSecuritySnapshot>.Lease security,
        CancellationToken token
    )
    {
        var (term, descriptor) = SnapshotStart(start);
        var attempt = start.AttemptId;
        _ = DataGridPeerCodec.Token(attempt);
        if (!_transfer.Wait(0, token))
            throw new IOException("Snapshot reservation unavailable.");
        try
        {
            security.Check();
            var current = await _receiver.ObserveLeaderAsync(term, token).ConfigureAwait(false);
            if (current != term)
            {
                await SnapshotAck(
                        tls,
                        attempt,
                        descriptor.SnapshotId,
                        current,
                        false,
                        false,
                        0,
                        token
                    )
                    .ConfigureAwait(false);
                return;
            }
            var bytes = new byte[checked((int)descriptor.ByteCount)];
            var offset = 0;
            await SnapshotAck(tls, attempt, descriptor.SnapshotId, current, true, false, 0, token)
                .ConfigureAwait(false);
            while (offset < bytes.Length)
            {
                var frame = await Read(tls, remote, security, token).ConfigureAwait(false);
                offset = CopyChunk(frame, attempt, descriptor.SnapshotId, bytes, offset);
                security.Check();
                current = await _receiver.ObserveLeaderAsync(term, token).ConfigureAwait(false);
                if (current != term)
                {
                    await SnapshotAck(
                            tls,
                            attempt,
                            descriptor.SnapshotId,
                            current,
                            false,
                            false,
                            0,
                            token
                        )
                        .ConfigureAwait(false);
                    return;
                }
                if (offset < bytes.Length)
                {
                    await SnapshotAck(
                            tls,
                            attempt,
                            descriptor.SnapshotId,
                            current,
                            true,
                            false,
                            offset,
                            token
                        )
                        .ConfigureAwait(false);
                    continue;
                }
                DataGridLocalStateMachine.Restore(_config, descriptor, bytes);
                var request = new RaftMessage(
                    RaftMessageKind.SnapshotRequest,
                    remote,
                    _local,
                    term,
                    DataGridPeerCodec.Token(attempt),
                    _config.IncarnationId,
                    _config.ComputeDigest(),
                    snapshot: new(descriptor, bytes)
                );
                security.Check();
                var reply = await _receiver.ReceiveAsync(request, token).ConfigureAwait(false);
                if (reply is null)
                    return;
                await SnapshotAck(
                        tls,
                        attempt,
                        descriptor.SnapshotId,
                        reply.Term,
                        reply.Accepted,
                        reply.Accepted,
                        reply.Accepted ? offset : 0,
                        token
                    )
                    .ConfigureAwait(false);
            }
        }
        finally
        {
            _transfer.Release();
        }
    }

    internal static (long Term, DataGridSnapshotDescriptor Descriptor) SnapshotStart(
        DataGridProtocolFrame frame
    )
    {
        if (frame.Kind != DataGridMessageKind.SnapshotStart)
            throw new InvalidDataException("Snapshot start required.");
        var reader = new GridReader(frame.GetPayload());
        var term = reader.Signed();
        var descriptor = DataGridSnapshotDescriptor.ReadFrom(
            reader.Take(DataGridSnapshotDescriptor.EncodedByteCount)
        );
        reader.End();
        if (
            term <= 0
            || descriptor.LastIncludedTerm > term
            || descriptor.ByteCount > RaftDurableImage.MaxImageBytes
        )
            throw new InvalidDataException("Invalid snapshot term/capacity.");
        return (term, descriptor);
    }

    internal static int CopyChunk(
        DataGridProtocolFrame frame,
        Guid attempt,
        Guid snapshot,
        byte[] target,
        int next
    )
    {
        if (frame.Kind != DataGridMessageKind.SnapshotChunk || frame.AttemptId != attempt)
            throw new InvalidDataException("Unexpected snapshot chunk.");
        var reader = new GridReader(frame.GetPayload());
        if (reader.Id() != snapshot)
            throw new InvalidDataException("Foreign snapshot chunk.");
        var offset = reader.Number(8);
        var count = reader.Count(4, 65536);
        var chunk = reader.Take(count);
        reader.End();
        if (
            count == 0
            || offset > (ulong)next
            || offset > (ulong)target.Length
            || (ulong)count > (ulong)target.Length - offset
        )
            throw new InvalidDataException("Invalid snapshot offset/length.");
        if (offset < (ulong)next)
        {
            if (
                offset + (ulong)count > (ulong)next
                || !chunk.SequenceEqual(target.AsSpan((int)offset, count))
            )
                throw new InvalidDataException("Changed snapshot retry bytes.");
            return next;
        }
        chunk.CopyTo(target.AsSpan(next));
        return checked(next + count);
    }

    private ValueTask SnapshotAck(
        SslStream tls,
        Guid attempt,
        Guid snapshot,
        long term,
        bool accepted,
        bool installed,
        int offset,
        CancellationToken token
    )
    {
        using var writer = new GridWriter(34);
        writer.Number((ulong)term, 8);
        writer.Id(snapshot);
        writer.Number(accepted ? 1u : 0u, 1);
        writer.Number(installed ? 1u : 0u, 1);
        writer.Number((ulong)offset, 8);
        return Write(
            tls,
            DataGridPeerCodec.Frame(
                _config,
                _local,
                DataGridMessageKind.SnapshotResponse,
                attempt,
                writer.ToArray()
            ),
            token
        );
    }

    private static (long Term, bool Accepted, bool Installed, long Offset) ReadSnapshotResponse(
        DataGridProtocolFrame frame,
        Guid attempt,
        Guid snapshot,
        long expectedTerm
    )
    {
        if (frame.Kind != DataGridMessageKind.SnapshotResponse || frame.AttemptId != attempt)
            throw new InvalidDataException("Unexpected snapshot response.");
        var reader = new GridReader(frame.GetPayload());
        var term = reader.Signed();
        if (reader.Id() != snapshot)
            throw new InvalidDataException("Foreign snapshot response.");
        var accepted = reader.Flag();
        var installed = reader.Flag();
        var offset = reader.Signed();
        reader.End();
        if (
            term < expectedTerm
            || accepted && term != expectedTerm
            || offset < 0
            || installed && !accepted
            || !accepted && offset != 0
        )
            throw new InvalidDataException("Invalid snapshot response fields.");
        return (term, accepted, installed, offset);
    }

    private RaftMessage SnapshotReply(RaftMessage request, long term, bool accepted) =>
        new(
            RaftMessageKind.SnapshotResponse,
            request.To,
            _local,
            term,
            request.Token,
            _config.IncarnationId,
            _config.ComputeDigest(),
            index: accepted ? request.Snapshot!.Descriptor.LastIncludedIndex : 0,
            accepted: accepted
        );
}
