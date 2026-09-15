using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;

namespace HostLoom.IntegrationTests.Infrastructure;

/// <summary>A loopback-only fault boundary. Disconnects its own clients, never the Redis server.</summary>
internal sealed class RedisFaultProxy : IAsyncDisposable
{
    private readonly TcpListener _listener = new(IPAddress.Loopback, 0);
    private readonly CancellationTokenSource _shutdown = new();
    private readonly object _sync = new();
    private readonly HashSet<TcpClient> _clients = [];
    private readonly List<Task> _sessions = [];
    private readonly Task _accept;
    private bool _enabled = true;

    public RedisFaultProxy()
    {
        _listener.Start();
        Configuration = "127.0.0.1:" + ((IPEndPoint)_listener.LocalEndpoint).Port;
        _accept = AcceptAsync();
    }

    public string Configuration { get; }

    public void SetEnabled(bool enabled)
    {
        lock (_sync)
        {
            _enabled = enabled;
            if (!enabled)
                foreach (var client in _clients)
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
                    _clients.Add(client);
                    _sessions.Add(ForwardAsync(client));
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
    private async Task ForwardAsync(TcpClient client)
    {
        using var upstream = new TcpClient();
        using var stopped = CancellationTokenSource.CreateLinkedTokenSource(_shutdown.Token);
        try
        {
            await upstream.ConnectAsync(
                RedisAvailability.Host,
                RedisAvailability.Port,
                stopped.Token
            );
            var incoming = client.GetStream();
            var outgoing = upstream.GetStream();
            var requests = incoming.CopyToAsync(outgoing, stopped.Token);
            var replies = outgoing.CopyToAsync(incoming, stopped.Token);
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
