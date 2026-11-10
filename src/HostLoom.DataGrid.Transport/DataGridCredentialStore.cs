using System.Net.Sockets;
using System.Security.Authentication;

namespace HostLoom.DataGrid.Transport;

/// <summary>Publication has completed when returned. Retired completes after all old users release material.</summary>
public sealed record DataGridCredentialReload(long Generation, Task Retired);

internal interface IDataGridCredentialMaterial : IDisposable
{
    long ValidUntilUtcTicks { get; }
}

internal sealed class DataGridCredentialRetiredException()
    : AuthenticationException("Credential generation retired.");

/// <summary>Bounded current/retiring material with connection retirement and explicit admission ordering.</summary>
internal sealed class DataGridCredentialStore<T> : IAsyncDisposable
    where T : IDataGridCredentialMaterial
{
    private readonly object _sync = new();
    private readonly HashSet<Lease> _leases = [];
    private readonly CancellationTokenSource _stop = new();
    private readonly Task _expiry;
    private readonly int _limit;
    private Generation _current;
    private Generation? _retiring;
    private Task? _shutdown;
    private bool _disposed;

    internal DataGridCredentialStore(T material, int limit)
    {
        _limit = limit;
        _current = new(material, 1);
        _expiry = Expire();
    }

    internal long Version
    {
        get
        {
            lock (_sync)
                return _current.Number;
        }
    }

    internal Lease Acquire(TcpClient? socket = null)
    {
        lock (_sync)
        {
            ObjectDisposedException.ThrowIf(_disposed, this);
            if (_current.Material.ValidUntilUtcTicks <= DateTime.UtcNow.Ticks)
                throw new AuthenticationException("Local credentials expired.");
            if (_leases.Count == _limit)
                throw new IOException("Credential lease capacity.");
            var lease = new Lease(this, _current, socket);
            _leases.Add(lease);
            _current.Readers++;
            return lease;
        }
    }

    // Replacement ownership always transfers, including a rejected publication.
    internal DataGridCredentialReload Publish(T replacement)
    {
        Generation old;
        Lease[] readers;
        try
        {
            lock (_sync)
            {
                ObjectDisposedException.ThrowIf(_disposed, this);
                if (_retiring is { } pending && !pending.Drained.Task.IsCompleted)
                    throw new IOException("Previous credential generation is still retiring.");
                if (replacement.ValidUntilUtcTicks <= DateTime.UtcNow.Ticks)
                    throw new AuthenticationException("Replacement credentials expired.");
                if (_current.Number == long.MaxValue)
                    throw new InvalidOperationException("Credential generation exhausted.");
                old = _current;
                _current = new(replacement, old.Number + 1);
                _retiring = old;
                old.Retired = true;
                readers = _leases.Where(lease => lease.Generation == old).ToArray();
                foreach (var lease in readers)
                    lease.CloseSocket();
                Cleanup(old);
            }
        }
        catch
        {
            replacement.Dispose();
            throw;
        }
        foreach (var lease in readers)
            lease.Cancel();
        return new(old.Number + 1, old.Drained.Task);
    }

    private static void Cleanup(Generation generation)
    {
        if (!generation.Retired || generation.Readers != 0 || generation.Cleaned)
            return;
        generation.Cleaned = true;
        try
        {
            generation.Material.Dispose();
        }
        catch (Exception exception)
        {
            generation.Failure ??= exception;
        }
        if (generation.Failure is { } failure)
            generation.Drained.TrySetException(failure);
        else
            generation.Drained.TrySetResult();
    }

    private async Task Expire()
    {
        using var timer = new PeriodicTimer(TimeSpan.FromSeconds(1));
        try
        {
            while (await timer.WaitForNextTickAsync(_stop.Token).ConfigureAwait(false))
            {
                Lease[] expired;
                lock (_sync)
                {
                    expired = _leases
                        .Where(lease =>
                            !lease.Expired && lease.ValidUntilUtcTicks <= DateTime.UtcNow.Ticks
                        )
                        .ToArray();
                    foreach (var lease in expired)
                    {
                        lease.Expired = true;
                        lease.CloseSocket();
                    }
                }
                foreach (var lease in expired)
                    lease.Cancel();
            }
        }
        catch (OperationCanceledException) when (_stop.IsCancellationRequested) { }
    }

    internal sealed class Lease : IDisposable
    {
        private readonly DataGridCredentialStore<T> _owner;
        private readonly CancellationTokenSource _revoked = new();
        private Task? _cancellation;
        internal Generation Generation { get; }
        private TcpClient? _socket;
        internal bool Expired { get; set; }
        private bool _released,
            _cleaned,
            _cancelFinished;
        internal long ValidUntilUtcTicks { get; private set; }
        internal T Material => Generation.Material;
        internal CancellationToken Revoked { get; }
        internal bool IsRetired
        {
            get
            {
                lock (_owner._sync)
                    return _released || Expired || Generation.Retired || _owner._disposed;
            }
        }

        internal Lease(DataGridCredentialStore<T> owner, Generation generation, TcpClient? socket)
        {
            _owner = owner;
            Generation = generation;
            _socket = socket;
            ValidUntilUtcTicks = generation.Material.ValidUntilUtcTicks;
            Revoked = _revoked.Token;
        }

        internal void Attach(TcpClient socket)
        {
            lock (_owner._sync)
            {
                try
                {
                    Check();
                    _socket = socket;
                }
                catch
                {
                    socket.Dispose();
                    throw;
                }
            }
        }

        internal void RemoteExpiresAt(DateTime utc)
        {
            lock (_owner._sync)
            {
                Check();
                ValidUntilUtcTicks = Math.Min(ValidUntilUtcTicks, utc.Ticks);
                Check();
            }
        }

        // This is the admission boundary. Work admitted before publication may still commit.
        internal void Check()
        {
            lock (_owner._sync)
                if (
                    _released
                    || Expired
                    || Generation.Retired
                    || _owner._disposed
                    || ValidUntilUtcTicks <= DateTime.UtcNow.Ticks
                )
                    throw new DataGridCredentialRetiredException();
        }

        internal void CloseSocket()
        {
            var socket = _socket;
            _socket = null;
            try
            {
                socket?.Dispose();
            }
            catch (Exception exception)
                when (exception is SocketException or ObjectDisposedException) { }
        }

        internal void Cancel()
        {
            lock (_owner._sync)
                if (!_cleaned && _cancellation is null)
                    _cancellation = CancelCore();
        }

        private async Task CancelCore()
        {
            Exception? failure = null;
            try
            {
                await _revoked.CancelAsync().ConfigureAwait(false);
            }
            catch (Exception exception)
            {
                failure = exception;
            }
            lock (_owner._sync)
            {
                _cancelFinished = true;
                Generation.Failure ??= failure;
                Finish();
            }
        }

        private void Finish()
        {
            if (!_released || _cleaned || _cancellation is not null && !_cancelFinished)
                return;
            _cleaned = true;
            _revoked.Dispose();
            _owner._leases.Remove(this);
            Generation.Readers--;
            Cleanup(Generation);
        }

        public void Dispose()
        {
            lock (_owner._sync)
            {
                if (_released)
                    return;
                _released = true;
                _socket = null;
                Finish();
            }
        }
    }

    internal sealed class Generation(T material, long number)
    {
        internal T Material { get; } = material;
        internal long Number { get; } = number;
        internal TaskCompletionSource Drained { get; } =
            new(TaskCreationOptions.RunContinuationsAsynchronously);
        internal int Readers { get; set; }
        internal bool Retired { get; set; }
        internal bool Cleaned { get; set; }
        internal Exception? Failure { get; set; }
    }

    public ValueTask DisposeAsync()
    {
        lock (_sync)
            return new(_shutdown ??= Shutdown());
    }

    private async Task Shutdown()
    {
        Generation[] generations;
        Lease[] readers;
        lock (_sync)
        {
            _disposed = true;
            generations = _retiring is { } old && old != _current ? [old, _current] : [_current];
            foreach (var generation in generations)
                generation.Retired = true;
            readers = _leases.ToArray();
            foreach (var lease in readers)
                lease.CloseSocket();
            foreach (var generation in generations)
                Cleanup(generation);
        }
        foreach (var lease in readers)
            lease.Cancel();
        await _stop.CancelAsync().ConfigureAwait(false);
        await _expiry.ConfigureAwait(false);
        try
        {
            await Task.WhenAll(generations.Select(generation => generation.Drained.Task))
                .ConfigureAwait(false);
        }
        finally
        {
            _stop.Dispose();
        }
    }
}
