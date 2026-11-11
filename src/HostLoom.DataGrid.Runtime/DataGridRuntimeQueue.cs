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
    bool Control
);

/// <summary>Two bounded actor lanes. One reader; up to eight control turns before a waiting client.</summary>
internal sealed class DataGridRuntimeQueue : IDisposable
{
    internal const int ControlLimit = 256,
        ClientLimit = 128;
    internal const long ByteLimit = 16L * 1024 * 1024;
    private readonly object _sync = new();
    private readonly Queue<DataGridRuntimeWork> _control = new(),
        _clients = new();
    private readonly SemaphoreSlim _available = new(0, ControlLimit + ClientLimit);
    private long _controlBytes,
        _clientBytes;
    private int _controlRun;
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
            var queue = work.Control ? _control : _clients;
            ref var bytes = ref (work.Control ? ref _controlBytes : ref _clientBytes);
            var reason =
                _completed || stopping ? "stopped"
                : work.Bytes < 0 || bytes + work.Bytes > ByteLimit ? "bytes"
                : queue.Count == (work.Control ? ControlLimit : ClientLimit) ? "items"
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
            queue.Enqueue(work);
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
            else if (_clients.TryDequeue(out work!))
                _controlRun = 0;
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
            var remaining = _control.Concat(_clients).ToArray();
            _control.Clear();
            _clients.Clear();
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
}
