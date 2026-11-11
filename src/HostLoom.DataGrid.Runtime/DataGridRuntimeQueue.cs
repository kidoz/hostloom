namespace HostLoom.DataGrid.Runtime;

internal enum DataGridRuntimeAdmissionFailure
{
    None,
    Stopped,
    Capacity,
}

internal sealed class DataGridRuntimeAdmissionException(DataGridRuntimeAdmissionFailure failure)
    : IOException("Runtime admission refused.")
{
    internal DataGridRuntimeAdmissionFailure Failure { get; } = failure;
}

internal sealed record DataGridRuntimeWork(
    Action Action,
    Action<Exception> Reject,
    int Bytes,
    bool Control,
    Guid Principal = default
);

/// <summary>Bounded actor lanes with FIFO per principal and round-robin client turns.</summary>
internal sealed class DataGridRuntimeQueue : IDisposable
{
    internal const int ControlLimit = 256,
        ClientLimit = 128,
        PrincipalLimit = 8;
    internal const long ByteLimit = 16L * 1024 * 1024,
        PrincipalByteLimit = 4L * 1024 * 1024;
    private readonly object _sync = new();
    private readonly Queue<DataGridRuntimeWork> _control = new();
    private readonly Dictionary<Guid, ClientQueue> _clients = [];
    private readonly Queue<Guid> _ready = new();
    private readonly SemaphoreSlim _available = new(0, ControlLimit + ClientLimit);
    private long _controlBytes,
        _clientBytes;
    private int _controlRun,
        _clientCount;
    private bool _completed;

    internal bool TryWrite(DataGridRuntimeWork work, bool stopping = false) =>
        TryWrite(work, out _, stopping);

    internal bool TryWrite(
        DataGridRuntimeWork work,
        out DataGridRuntimeAdmissionFailure failure,
        bool stopping = false
    )
    {
        lock (_sync)
        {
            _clients.TryGetValue(work.Principal, out var client);
            ref var bytes = ref (work.Control ? ref _controlBytes : ref _clientBytes);
            var reason =
                _completed || stopping ? "stopped"
                : work.Bytes < 0 || bytes + work.Bytes > ByteLimit ? "bytes"
                : (work.Control ? _control.Count : _clientCount)
                == (work.Control ? ControlLimit : ClientLimit)
                    ? "items"
                : !work.Control
                && work.Principal != Guid.Empty
                && (
                    (client?.Items.Count ?? 0) == PrincipalLimit
                    || (client?.Bytes ?? 0) + work.Bytes > PrincipalByteLimit
                )
                    ? "principal"
                : null;
            if (reason is not null)
            {
                failure =
                    reason == "stopped"
                        ? DataGridRuntimeAdmissionFailure.Stopped
                        : DataGridRuntimeAdmissionFailure.Capacity;
                DataGridRuntimeDiagnostics.Reject(work.Control, reason);
                return false;
            }
            failure = DataGridRuntimeAdmissionFailure.None;
            if (work.Control)
                _control.Enqueue(work);
            else
            {
                if (client is null)
                {
                    client = new();
                    _clients.Add(work.Principal, client);
                    _ready.Enqueue(work.Principal);
                }
                client.Items.Enqueue(work);
                client.Bytes += work.Bytes;
                _clientCount++;
            }
            bytes += work.Bytes;
            DataGridRuntimeDiagnostics.QueueChange(work.Control, 1, work.Bytes);
            _available.Release();
            return true;
        }
    }

    internal async ValueTask<DataGridRuntimeWork> ReadAsync(CancellationToken token)
    {
        await _available.WaitAsync(token).ConfigureAwait(false);
        lock (_sync)
        {
            DataGridRuntimeWork work;
            if (_controlRun < 8 && _control.TryDequeue(out work!))
                _controlRun++;
            else if (_ready.Count != 0)
            {
                var principal = _ready.Dequeue();
                var client = _clients[principal];
                work = client.Items.Dequeue();
                client.Bytes -= work.Bytes;
                _clientCount--;
                if (client.Items.Count == 0)
                    _clients.Remove(principal);
                else
                    _ready.Enqueue(principal);
                _controlRun = 0;
            }
            else
            {
                work = _control.Dequeue();
                _controlRun = 1;
            }
            Release(work);
            return work;
        }
    }

    private void Release(DataGridRuntimeWork work)
    {
        if (work.Control)
            _controlBytes -= work.Bytes;
        else
            _clientBytes -= work.Bytes;
        DataGridRuntimeDiagnostics.QueueChange(work.Control, -1, -work.Bytes);
    }

    // Called after the reader stops. Rejection callbacks run outside the lock in the owner.
    internal DataGridRuntimeWork[] Complete()
    {
        lock (_sync)
        {
            _completed = true;
            var remaining = _control
                .Concat(_clients.Values.SelectMany(client => client.Items))
                .ToArray();
            _control.Clear();
            _clients.Clear();
            _ready.Clear();
            _clientCount = 0;
            foreach (var work in remaining)
                Release(work);
            return remaining;
        }
    }

    public void Dispose()
    {
        Complete();
        _available.Dispose();
    }

    private sealed class ClientQueue
    {
        internal Queue<DataGridRuntimeWork> Items { get; } = new();
        internal long Bytes { get; set; }
    }
}
