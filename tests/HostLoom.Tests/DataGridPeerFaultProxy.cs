using System.Diagnostics.CodeAnalysis;
using System.Net;
using System.Net.Sockets;

namespace HostLoom.Tests;

/// <summary>Owned directed loopback link: encrypted bytes only, two sessions, no retained task chain.</summary>
internal sealed class DataGridPeerFaultProxy : IAsyncDisposable
{
    private readonly TcpListener _listener;
    private readonly IPEndPoint _target;
    private readonly SemaphoreSlim _slots = new(2, 2);
    private readonly CancellationTokenSource _stop = new();
    private readonly object _sync = new();
    private readonly Dictionary<long, (TcpClient Client, Task Task)> _sessions = [];
    private readonly Task _accept;
    private readonly TaskCompletionSource _refused = new(
        TaskCreationOptions.RunContinuationsAsynchronously
    );
    private bool _enabled = true;
    private long _id,
        _bytes;
    internal Exception? Failure { get; private set; }
    internal Task Refused => _refused.Task;
    internal long ForwardedBytes => Interlocked.Read(ref _bytes);

    internal static IPAddress Backend(int member) => IPAddress.Parse($"127.0.0.{2 + member}");

    internal static IPAddress Route(int source) => IPAddress.Parse($"127.0.0.{10 + source}");

    internal DataGridPeerFaultProxy(int source, int target, int port)
    {
        _target = new(Backend(target), port);
        _listener = new(Route(source), port);
        _listener.Start(2);
        _accept = Accept();
    }

    internal void SetEnabled(bool enabled)
    {
        lock (_sync)
        {
            _enabled = enabled;
            if (!enabled)
                foreach (var session in _sessions.Values)
                    session.Client.Dispose();
        }
    }

    [SuppressMessage(
        "Reliability",
        "CA2000:Dispose objects before losing scope",
        Justification = "A bounded tracked session task owns each accepted socket and releases it in finally."
    )]
    private async Task Accept()
    {
        try
        {
            while (!_stop.IsCancellationRequested)
            {
                var client = await _listener.AcceptTcpClientAsync(_stop.Token);
                lock (_sync)
                {
                    if (!_enabled || !_slots.Wait(0))
                    {
                        client.Dispose();
                        _refused.TrySetResult();
                        continue;
                    }
                    var id = ++_id;
                    _sessions.Add(
                        id,
                        (
                            client,
                            Task.Run(async () =>
                            {
                                try
                                {
                                    await Forward(client);
                                }
                                catch (Exception exception)
                                {
                                    Failure = exception;
                                    await _stop.CancelAsync();
                                }
                                finally
                                {
                                    client.Dispose();
                                    lock (_sync)
                                    {
                                        _sessions.Remove(id);
                                        _slots.Release();
                                    }
                                }
                            })
                        )
                    );
                }
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
        catch (Exception exception)
        {
            Failure = exception;
            await _stop.CancelAsync();
        }
    }

    private async Task Forward(TcpClient client)
    {
        using var upstream = new TcpClient { NoDelay = true };
        using var stopped = CancellationTokenSource.CreateLinkedTokenSource(_stop.Token);
        stopped.CancelAfter(TimeSpan.FromSeconds(60));
        try
        {
            client.NoDelay = true;
            await upstream.ConnectAsync(_target, stopped.Token);
            var requests = Copy(client.GetStream(), upstream.GetStream(), stopped.Token);
            var replies = Copy(upstream.GetStream(), client.GetStream(), stopped.Token);
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
            ) { }
    }

    private async Task Copy(NetworkStream source, NetworkStream target, CancellationToken token)
    {
        var buffer = new byte[16 * 1024];
        while (true)
        {
            var count = await source.ReadAsync(buffer, token);
            if (count == 0)
                return;
            await target.WriteAsync(buffer.AsMemory(0, count), token);
            Interlocked.Add(ref _bytes, count);
        }
    }

    public async ValueTask DisposeAsync()
    {
        SetEnabled(false);
        await _stop.CancelAsync();
        _listener.Stop();
        await _accept;
        Task[] tasks;
        lock (_sync)
            tasks = _sessions.Values.Select(session => session.Task).ToArray();
        await Task.WhenAll(tasks);
        _slots.Dispose();
        _stop.Dispose();
        _listener.Dispose();
    }
}
