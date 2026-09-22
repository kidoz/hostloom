using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;

namespace HostLoom.IntegrationTests.Infrastructure;

/// <summary>
/// A loopback-only TCP fault boundary in front of one upstream server, such as the Redis,
/// Valkey, or RabbitMQ from the compose file. It forwards bytes without parsing the protocol
/// and disconnects or stalls only the clients connected through it, never the server.
/// </summary>
internal sealed class TcpFaultProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly object _sync = new();
    private readonly Dictionary<TcpClient, Session> _clients = [];
    private readonly List<Task> _sessions = [];
    private readonly Task _accept;
    private readonly string _upstreamHost;
    private readonly int _upstreamPort;
    private readonly TaskCompletionSource _refused = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private bool _enabled = true;
    private TaskCompletionSource? _hold;

    /// <summary>Listens on a free loopback port and forwards every session it accepts upstream.</summary>
    public TcpFaultProxy(string upstreamHost, int upstreamPort)
    {
        _upstreamHost = upstreamHost;
        _upstreamPort = upstreamPort;
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        Configuration = Host + ":" + Port;
        _accept = AcceptAsync();
    }

    /// <summary>The proxy endpoint as <c>host:port</c>, the form a Redis configuration string takes.</summary>
    public string Configuration { get; }

    public static string Host => "127.0.0.1";

    public int Port { get; }

    /// <summary>
    /// Completes the first time the disabled proxy refuses a connection, such as a client's
    /// reconnection attempt, so an outage can be held across one without sleeping.
    /// </summary>
    public Task Refused => _refused.Task;

    /// <summary>
    /// Stops forwarding server replies on every current session that has carried a
    /// subscription acknowledgement, without closing anything: the fault of a pub/sub socket
    /// that dies silently, as an idle firewall or NAT drop produces. Sessions opened afterwards
    /// are not affected, so a replacement subscriber recovers. Applies to pub/sub protocols such
    /// as Redis and Valkey, whose subscription replies carry the bytes <c>subscribe</c>.
    /// </summary>
    public void StallSubscribers()
    {
        lock (_sync)
        {
            foreach (var session in _clients.Values)
            {
                if (session.SawSubscribe)
                {
                    session.Stalled = true;
                }
            }
        }
    }

    /// <summary>
    /// Holds every server reply on every session, current and future, until
    /// <see cref="ReleaseReplies"/>; requests still reach the server. Models a reply that lands
    /// after the caller stopped waiting.
    /// </summary>
    public void HoldReplies() =>
        Interlocked.CompareExchange(
            ref _hold,
            new TaskCompletionSource(TaskCreationOptions.RunContinuationsAsynchronously),
            null
        );

    public void ReleaseReplies() => Interlocked.Exchange(ref _hold, null)?.TrySetResult();

    /// <summary>
    /// Disabling closes every current session and refuses new ones, which a client sees as its
    /// server going away; enabling accepts and forwards sessions again.
    /// </summary>
    public void SetEnabled(bool enabled)
    {
        lock (_sync)
        {
            _enabled = enabled;
            if (!enabled)
                foreach (var client in _clients.Keys)
                    client.Dispose();
        }
    }

    [SuppressMessage(
        "Reliability",
        "CA2025",
        Justification = "Session tasks are retained and joined during DisposeAsync; disconnecting their sockets is the fault under test."
    )]
    private async Task AcceptAsync()
    {
        try
        {
            while (!_shutdown.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_shutdown.Token);
                lock (_sync)
                {
                    if (!_enabled)
                    {
                        client.Dispose();
                        _refused.TrySetResult();
                        continue;
                    }
                    var session = new Session();
                    _clients.Add(client, session);
                    _sessions.Add(ForwardAsync(client, session));
                }
            }
        }
        catch (OperationCanceledException) when (_shutdown.IsCancellationRequested) { }
    }

    [SuppressMessage(
        "Reliability",
        "CA2025",
        Justification = "Both copy tasks are joined with WhenAll before the connection and cancellation source are disposed."
    )]
    private async Task ForwardAsync(TcpClient client, Session session)
    {
        using var upstream = new TcpClient();
        using var stopped = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);
        try
        {
            await upstream.ConnectAsync(_upstreamHost, _upstreamPort, stopped.Token);
            var incoming = client.GetStream();
            var outgoing = upstream.GetStream();
            var requests = incoming.CopyToAsync(outgoing, stopped.Token);
            var replies = PumpRepliesAsync(outgoing, incoming, session, stopped.Token);
            await Task.WhenAny(requests, replies);
            await stopped.CancelAsync();
            await Task.WhenAll(requests, replies);
        }
        catch (Exception exception)
            when (exception
                    is IOException
                        or SocketException
                        or OperationCanceledException
                        or ObjectDisposedException
            )
        {
            // Closing a proxy socket is the injected fault; both pumps are observed above.
        }
        finally
        {
            client.Dispose();
            lock (_sync)
                _clients.Remove(client);
        }
    }

    /// <summary>
    /// Server-to-client copy that notices pub/sub subscription acknowledgements and can be held
    /// or stalled.
    /// </summary>
    private async Task PumpRepliesAsync(
        NetworkStream source,
        NetworkStream target,
        Session session,
        CancellationToken token
    )
    {
        var buffer = new byte[16 * 1024];
        while (true)
        {
            var read = await source.ReadAsync(buffer, token);
            if (read == 0)
            {
                return;
            }

            if (!session.SawSubscribe && buffer.AsSpan(0, read).IndexOf("subscribe"u8) >= 0)
            {
                session.SawSubscribe = true;
            }

            if (session.Stalled)
            {
                // Drained and dropped: the server sees a healthy reader, the client sees silence.
                continue;
            }

            if (Volatile.Read(ref _hold) is { } hold)
            {
                await hold.Task.WaitAsync(token);
            }

            await target.WriteAsync(buffer.AsMemory(0, read), token);
        }
    }

    private sealed class Session
    {
        public volatile bool SawSubscribe;

        public volatile bool Stalled;
    }

    public async ValueTask DisposeAsync()
    {
        ReleaseReplies();
        SetEnabled(false);
        await _shutdown.CancelAsync();
        await _accept;
        _listener.Dispose();
        await Task.WhenAll(_sessions);
        _shutdown.Dispose();
    }
}
