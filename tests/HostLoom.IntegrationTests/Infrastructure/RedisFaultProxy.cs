using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;

namespace HostLoom.IntegrationTests.Infrastructure;

/// <summary>
/// A loopback-only fault boundary in front of the Redis from the compose file, or of any other
/// upstream. Disconnects or stalls its own clients, never the server.
/// </summary>
internal sealed class RedisFaultProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly object _sync = new();
    private readonly Dictionary<TcpClient, Session> _clients = [];
    private readonly List<Task> _sessions = [];
    private readonly Task _accept;
    private readonly string _upstreamHost;
    private readonly int _upstreamPort;
    private bool _enabled = true;

    public RedisFaultProxy()
        : this(RedisAvailability.Host, RedisAvailability.Port) { }

    public RedisFaultProxy(string upstreamHost, int upstreamPort)
    {
        _upstreamHost = upstreamHost;
        _upstreamPort = upstreamPort;
        _listener.Start();
        Port = ((IPEndPoint)_listener.LocalEndpoint).Port;
        Configuration = Host + ":" + Port;
        _accept = AcceptAsync();
    }

    public string Configuration { get; }

    public static string Host => "127.0.0.1";

    public int Port { get; }

    /// <summary>
    /// Stops forwarding server replies on every current session that has carried a
    /// subscription acknowledgement, without closing anything: the fault of a pub/sub socket
    /// that dies silently, as an idle firewall or NAT drop produces. Sessions opened afterwards
    /// are not affected, so a replacement subscriber recovers.
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

    /// <summary>Server-to-client copy that notices subscription acknowledgements and can be stalled.</summary>
    private static async Task PumpRepliesAsync(
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
        SetEnabled(false);
        await _shutdown.CancelAsync();
        await _accept;
        _listener.Dispose();
        await Task.WhenAll(_sessions);
        _shutdown.Dispose();
    }
}
