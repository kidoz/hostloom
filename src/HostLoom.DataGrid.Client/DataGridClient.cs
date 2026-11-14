using System.Diagnostics;
using System.Diagnostics.CodeAnalysis;
using System.Net.Security;
using System.Net.Sockets;
using System.Security.Authentication;
using System.Security.Cryptography;
using System.Security.Cryptography.X509Certificates;
using HostLoom.DataGrid.Transport;

namespace HostLoom.DataGrid.Client;

/// <summary>Unary TLS client: at most three physical attempts in five seconds, retaining one logical request.</summary>
[SuppressMessage(
    "Security",
    "CA5398:Avoid hardcoded SslProtocols values",
    Justification = "The v1 contract requires TLS 1.3 with no downgrade."
)]
public sealed class DataGridClient : IAsyncDisposable
{
    private readonly DataGridBootstrapConfiguration _configuration;
    private readonly Guid _principal;
    private readonly IReadOnlyList<DataGridClientEndpoint> _routes;
    private readonly byte[] _build;
    private readonly DataGridCredentialStore<DataGridOwnedClientCredentials> _security;
    private readonly DataGridClientHistory? _history;
    private readonly SemaphoreSlim _calls = new(8, 8);
    private readonly CancellationTokenSource _disabled = new();
    private readonly object _sync = new();
    private Task? _shutdown;
    private long _rateTime = TimeProvider.System.GetTimestamp();
    private double _tokens = 2;
    private int _preferred;

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "The store owns validated client credentials through the last captured call."
    )]
    public DataGridClient(DataGridClientOptions options, DataGridClientHistory? history = null)
    {
        ArgumentNullException.ThrowIfNull(options);
        _configuration = options.Configuration;
        _principal = options.PrincipalId;
        _routes = options.Endpoints;
        _build = options.Build;
        _security = new(DataGridOwnedClientCredentials.Create(options), 8);
        _history = history;
        _preferred = options.InitialMember;
    }

    /// <summary>Atomically replaces trust/pins/client material, closes old exchanges and preserves logical identities. Await Retired for old-material cleanup.</summary>
    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "Publish owns the replacement even when publication is rejected."
    )]
    public DataGridCredentialReload ReloadCredentials(DataGridClientOptions options)
    {
        ArgumentNullException.ThrowIfNull(options);
        if (
            options.PrincipalId != _principal
            || !options
                .Configuration.ComputeDigest()
                .AsSpan()
                .SequenceEqual(_configuration.ComputeDigest())
            || !options.Build.AsSpan().SequenceEqual(_build)
            || Enumerable
                .Range(0, 3)
                .Any(i =>
                    options.Endpoints[i].Port != _routes[i].Port
                    || !Equals(options.Endpoints[i].Address, _routes[i].Address)
                )
        )
            throw new ArgumentException(
                "Credential reload cannot change principal, configuration, build or routes.",
                nameof(options)
            );
        var replacement = DataGridOwnedClientCredentials.Create(options);
        lock (_sync)
        {
            if (_shutdown is not null || _disabled.IsCancellationRequested)
            {
                replacement.Dispose();
                throw new ObjectDisposedException(nameof(DataGridClient));
            }
            return _security.Publish(replacement);
        }
    }

    public void Disable()
    {
        lock (_sync)
            if (_shutdown is null)
                _disabled.Cancel();
    }

    public async Task<DataGridClientOutcome> ExecuteAsync(
        DataGridClientRequest request,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        var operation = DataGridClientDiagnostics.Operation(request.Kind);
        var started = Stopwatch.GetTimestamp();
        var outcome = "faulted";
        var mayHaveBeenAdmitted = false;
        DataGridClientDiagnostics.Begin(operation);
        try
        {
            var result = await ExecuteCore(request, cancellationToken).ConfigureAwait(false);
            outcome = DataGridClientDiagnostics.Outcome(result.Reply.Failure?.Code);
            mayHaveBeenAdmitted = result.MayHaveBeenAdmitted;
            return result;
        }
        catch (OperationCanceledException)
        {
            outcome = "canceled";
            throw;
        }
        finally
        {
            DataGridClientDiagnostics.End(
                operation,
                outcome,
                mayHaveBeenAdmitted,
                Stopwatch.GetElapsedTime(started).TotalSeconds
            );
        }
    }

    private async Task<DataGridClientOutcome> ExecuteCore(
        DataGridClientRequest request,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(request);
        cancellationToken.ThrowIfCancellationRequested();
        var historyId = _history?.Begin(_configuration.IncarnationId, _principal, request);
        var attempts = new List<DataGridClientAttempt>(3);
        var mayHaveBeenAdmitted = false;
        Guid currentAttempt = default,
            currentMember = default;
        var submitted = false;
        var recorded = true;
        var replyValidated = false;
        DataGridClientReply? observedReply = null;
        long? attemptStarted = null;
        void RecordAttempt()
        {
            if (!recorded)
            {
                attempts.Add(
                    new(currentAttempt, currentMember, submitted)
                    {
                        Reply = observedReply,
                        InvocationOrder = attemptStarted,
                        CompletionOrder = _history?.AttemptEvent(),
                    }
                );
                DataGridClientDiagnostics.Attempt(
                    DataGridClientDiagnostics.Operation(request.Kind),
                    submitted
                );
                recorded = true;
            }
        }
        DataGridClientOutcome Finish(DataGridClientReply reply)
        {
            RecordAttempt();
            var outcome = new DataGridClientOutcome(reply, mayHaveBeenAdmitted, attempts);
            if (historyId is { } id)
                _history!.Complete(id, outcome);
            return outcome;
        }
        DataGridClientReply Unavailable() =>
            DataGridClientReply.Failed(
                new(
                    mayHaveBeenAdmitted && request.Kind != DataGridMessageKind.Read
                        ? DataGridClientFailureCode.OutcomeUnknown
                        : DataGridClientFailureCode.QuorumUnavailable,
                    mayHaveBeenAdmitted
                )
            );
        CancellationToken disabledToken;
        lock (_sync)
        {
            if (_shutdown is not null || _disabled.IsCancellationRequested)
                return Finish(Unavailable());
            if (!_calls.Wait(0, cancellationToken))
                return Finish(
                    DataGridClientReply.Failed(
                        new(DataGridClientFailureCode.CapacityExceeded, false)
                    )
                );
            disabledToken = _disabled.Token;
        }
        try
        {
            using var security = _security.Acquire();
            var options = security.Material.Options;
            using var deadline = new CancellationTokenSource(TimeSpan.FromSeconds(5));
            using var lifetime = CancellationTokenSource.CreateLinkedTokenSource(
                cancellationToken,
                disabledToken,
                deadline.Token,
                security.Revoked
            );
            var token = lifetime.Token;
            void RecordTimeout()
            {
                if (
                    deadline.IsCancellationRequested
                    && !cancellationToken.IsCancellationRequested
                    && !disabledToken.IsCancellationRequested
                    && !security.Revoked.IsCancellationRequested
                )
                    DataGridClientDiagnostics.TimedOut(
                        DataGridClientDiagnostics.Operation(request.Kind),
                        mayHaveBeenAdmitted
                    );
            }
            var member = Volatile.Read(ref _preferred);
            var redirects = 0;
            for (var attempt = 0; attempt < 3; attempt++)
            {
                var attemptId = Guid.NewGuid();
                currentAttempt = attemptId;
                currentMember = options.Endpoints[member].MemberId;
                submitted = false;
                recorded = false;
                replyValidated = false;
                observedReply = null;
                attemptStarted = _history?.AttemptEvent();
                try
                {
                    await WaitForReconnect(token).ConfigureAwait(false);
                    var endpoint = options.Endpoints[member];
                    using var tcp = new TcpClient { NoDelay = true };
                    security.Attach(tcp);
                    if (endpoint.Address is { } address)
                        await tcp.ConnectAsync(address, endpoint.Port, token).ConfigureAwait(false);
                    else
                        await tcp.ConnectAsync(endpoint.DnsName, endpoint.Port, token)
                            .ConfigureAwait(false);
                    using var tls = new SslStream(tcp.GetStream());
                    await tls.AuthenticateAsClientAsync(
                            new SslClientAuthenticationOptions
                            {
                                EnabledSslProtocols = SslProtocols.Tls13,
                                TargetHost = endpoint.DnsName,
                                ClientCertificates = new X509CertificateCollection
                                {
                                    options.Certificate,
                                },
                                CertificateChainPolicy = DataGridTlsSupport.ChainPolicy(
                                    options.Root,
                                    server: true
                                ),
                                AllowRenegotiation = false,
                                AllowTlsResume = false,
                                ApplicationProtocols = [DataGridClientServer.Protocol],
                                RemoteCertificateValidationCallback = (_, certificate, _, errors) =>
                                    errors == SslPolicyErrors.None
                                    && certificate is X509Certificate2 leaf
                                    && DataGridTlsSupport.Current(leaf)
                                    && CryptographicOperations.FixedTimeEquals(
                                        leaf.GetCertHash(HashAlgorithmName.SHA256),
                                        endpoint.GetCertificateSha256()
                                    )
                                    && leaf.MatchesHostname(
                                        endpoint.DnsName,
                                        allowWildcards: false,
                                        allowCommonName: false
                                    ),
                            },
                            token
                        )
                        .ConfigureAwait(false);
                    if (
                        !tls.IsMutuallyAuthenticated
                        || tls.SslProtocol != SslProtocols.Tls13
                        || tls.NegotiatedApplicationProtocol != DataGridClientServer.Protocol
                        || tls.RemoteCertificate is not X509Certificate2 remote
                    )
                        throw new AuthenticationException("Client TLS binding rejected.");
                    security.RemoteExpiresAt(remote.NotAfter.ToUniversalTime());
                    var hello = DataGridClientCodec.Hello(
                        options.Configuration.IncarnationId,
                        options.PrincipalId,
                        Guid.NewGuid(),
                        options.Build,
                        options.Configuration.ComputeDigest()
                    );
                    await DataGridTlsSupport.WriteAsync(tls, hello, token).ConfigureAwait(false);
                    var helloReply = await DataGridTlsSupport
                        .ReadAsync(
                            tls,
                            options.Configuration.IncarnationId,
                            endpoint.MemberId,
                            token
                        )
                        .ConfigureAwait(false);
                    if (helloReply.AttemptId != hello.AttemptId)
                        throw new InvalidDataException("Wrong hello attempt.");
                    DataGridClientCodec.ValidateHello(helloReply, options.Configuration);
                    var frame = DataGridClientCodec.EncodeRequest(
                        request,
                        options.Configuration.IncarnationId,
                        options.PrincipalId,
                        attemptId
                    );
                    security.Check();
                    submitted = true;
                    await DataGridTlsSupport.WriteAsync(tls, frame, token).ConfigureAwait(false);
                    var response = await DataGridTlsSupport
                        .ReadAsync(
                            tls,
                            options.Configuration.IncarnationId,
                            endpoint.MemberId,
                            token
                        )
                        .ConfigureAwait(false);
                    security.Check();
                    var reply = DataGridClientCodec.DecodeReply(
                        response,
                        request,
                        attemptId,
                        options.Configuration
                    );
                    replyValidated = true;
                    observedReply = reply;
                    if (reply.Failure is not { } failure)
                    {
                        mayHaveBeenAdmitted = true;
                        RecordAttempt();
                        Volatile.Write(ref _preferred, member);
                        return Finish(reply);
                    }
                    mayHaveBeenAdmitted |= failure.MayHaveBeenAdmitted;
                    RecordAttempt();
                    if (failure.Code == DataGridClientFailureCode.Redirect && ++redirects <= 2)
                        member = Enumerable
                            .Range(0, 3)
                            .First(i => options.Endpoints[i].MemberId == failure.RedirectMember);
                    else if (
                        failure.Code
                            is DataGridClientFailureCode.QuorumUnavailable
                                or DataGridClientFailureCode.OutcomeUnknown
                        || failure.Code == DataGridClientFailureCode.CapacityExceeded
                            && !failure.MayHaveBeenAdmitted
                    )
                        member = (member + 1) % 3;
                    else
                        return Finish(reply);
                    if (attempt == 2)
                        return Finish(mayHaveBeenAdmitted ? Unavailable() : reply);
                    var delay = Math.Max(
                        failure.RetryAfterMilliseconds,
                        RandomNumberGenerator.GetInt32(50, Math.Min(501, 101 << attempt))
                    );
                    await Task.Delay(delay, token).ConfigureAwait(false);
                }
                catch (OperationCanceledException) when (token.IsCancellationRequested)
                {
                    mayHaveBeenAdmitted |= submitted && !replyValidated;
                    RecordTimeout();
                    return Finish(Unavailable());
                }
                catch (Exception exception)
                    when (exception is DataGridCredentialRetiredException
                        || exception is ObjectDisposedException && security.IsRetired
                    )
                {
                    mayHaveBeenAdmitted |= submitted && !replyValidated;
                    return Finish(Unavailable());
                }
                catch (AuthenticationException)
                {
                    mayHaveBeenAdmitted |= submitted && !replyValidated;
                    return Finish(
                        DataGridClientReply.Failed(
                            new(DataGridClientFailureCode.Unauthorized, mayHaveBeenAdmitted)
                        )
                    );
                }
                catch (InvalidDataException)
                {
                    mayHaveBeenAdmitted |= submitted && !replyValidated;
                    return Finish(
                        DataGridClientReply.Failed(
                            new(DataGridClientFailureCode.ProtocolMismatch, mayHaveBeenAdmitted)
                        )
                    );
                }
                catch (Exception exception) when (exception is IOException or SocketException)
                {
                    mayHaveBeenAdmitted |= submitted && !replyValidated;
                    member = (member + 1) % 3;
                    if (attempt < 2)
                    {
                        try
                        {
                            await Task.Delay(
                                    RandomNumberGenerator.GetInt32(
                                        50,
                                        Math.Min(501, 101 << attempt)
                                    ),
                                    token
                                )
                                .ConfigureAwait(false);
                        }
                        catch (OperationCanceledException) when (token.IsCancellationRequested)
                        {
                            RecordTimeout();
                            return Finish(Unavailable());
                        }
                    }
                }
                finally
                {
                    RecordAttempt();
                }
            }
            return Finish(Unavailable());
        }
        catch (AuthenticationException)
        {
            return Finish(
                DataGridClientReply.Failed(new(DataGridClientFailureCode.Unauthorized, false))
            );
        }
        catch (IOException)
        {
            return Finish(
                DataGridClientReply.Failed(new(DataGridClientFailureCode.CapacityExceeded, false))
            );
        }
        finally
        {
            _calls.Release();
        }
    }

    private async Task WaitForReconnect(CancellationToken token)
    {
        while (true)
        {
            double wait;
            lock (_sync)
            {
                var now = TimeProvider.System.GetTimestamp();
                _tokens = Math.Min(
                    2,
                    _tokens + TimeProvider.System.GetElapsedTime(_rateTime, now).TotalSeconds * 2
                );
                _rateTime = now;
                if (_tokens >= 1)
                {
                    _tokens--;
                    return;
                }
                wait = (1 - _tokens) / 2;
            }
            await Task.Delay(TimeSpan.FromSeconds(wait), token).ConfigureAwait(false);
        }
    }

    public ValueTask DisposeAsync()
    {
        lock (_sync)
            return new(_shutdown ??= Shutdown());
    }

    private async Task Shutdown()
    {
        await _disabled.CancelAsync().ConfigureAwait(false);
        for (var i = 0; i < 8; i++)
            await _calls.WaitAsync().ConfigureAwait(false);
        await _security.DisposeAsync().ConfigureAwait(false);
        _calls.Dispose();
        _disabled.Dispose();
    }
}
