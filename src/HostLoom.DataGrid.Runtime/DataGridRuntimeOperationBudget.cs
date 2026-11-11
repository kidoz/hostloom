namespace HostLoom.DataGrid.Runtime;

/// <summary>One charge from before enqueue through the owner's terminal completion, not caller cancellation.</summary>
internal sealed class DataGridRuntimeOperationBudget
{
    private readonly object _sync = new();
    private readonly Dictionary<Guid, Usage> _principals = [];
    private int _count;
    private long _bytes;
    private bool _completed;

    internal Lease? TryAcquire(
        Guid principal,
        int bytes,
        out DataGridRuntimeAdmissionFailure failure,
        bool stopping = false
    )
    {
        lock (_sync)
        {
            var usage = _principals.GetValueOrDefault(principal);
            var reason =
                _completed || stopping ? "stopped"
                : bytes < 0 || _bytes + bytes > DataGridRuntimeQueue.ByteLimit ? "bytes"
                : _count == DataGridRuntimeQueue.ClientLimit ? "items"
                : principal != Guid.Empty
                && (
                    usage.Count == DataGridRuntimeQueue.PrincipalLimit
                    || usage.Bytes + bytes > DataGridRuntimeQueue.PrincipalByteLimit
                )
                    ? "principal"
                : null;
            if (reason is not null)
            {
                failure =
                    reason == "stopped"
                        ? DataGridRuntimeAdmissionFailure.Stopped
                        : DataGridRuntimeAdmissionFailure.Capacity;
                DataGridRuntimeDiagnostics.Reject(false, reason);
                return null;
            }
            failure = DataGridRuntimeAdmissionFailure.None;
            _principals[principal] = new(usage.Count + 1, usage.Bytes + bytes);
            _count++;
            _bytes += bytes;
            DataGridRuntimeDiagnostics.AdmissionChange(1, bytes);
            return new(this, principal, bytes);
        }
    }

    // Stop new charges without releasing work that the actor still owns.
    internal void Complete()
    {
        lock (_sync)
            _completed = true;
    }

    internal sealed class Lease(DataGridRuntimeOperationBudget owner, Guid principal, int bytes)
        : IDisposable
    {
        private DataGridRuntimeOperationBudget? _owner = owner;

        public void Dispose()
        {
            var held = Interlocked.Exchange(ref _owner, null);
            if (held is null)
                return;
            lock (held._sync)
            {
                var usage = held._principals[principal];
                if (usage.Count == 1)
                    held._principals.Remove(principal);
                else
                    held._principals[principal] = new(usage.Count - 1, usage.Bytes - bytes);
                held._count--;
                held._bytes -= bytes;
                DataGridRuntimeDiagnostics.AdmissionChange(-1, -bytes);
            }
        }
    }

    private readonly record struct Usage(int Count, long Bytes);
}
