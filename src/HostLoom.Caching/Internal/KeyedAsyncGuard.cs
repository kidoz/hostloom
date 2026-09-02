namespace HostLoom.Caching.Internal;

/// <summary>
/// Per-key asynchronous mutual exclusion for single-flight. Guards are reference-counted so one
/// is never disposed while a caller awaits it, and an idle guard is reclaimed only after
/// <see cref="CacheL1Options.GuardIdleTime"/>. Guarding one key never blocks another.
/// </summary>
internal sealed class KeyedAsyncGuard : IDisposable
{
    private readonly Lock _gate = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly TimeProvider _time;

    public KeyedAsyncGuard(TimeProvider time) => _time = time;

    /// <summary>Guards that are held or awaited right now.</summary>
    public int ActiveCount
    {
        get
        {
            lock (_gate)
            {
                var active = 0;
                foreach (var entry in _entries.Values)
                {
                    if (entry.References > 0)
                    {
                        active++;
                    }
                }

                return active;
            }
        }
    }

    public async ValueTask<Releaser> AcquireAsync(string key, CancellationToken cancellationToken)
    {
        Entry entry;
        lock (_gate)
        {
            if (!_entries.TryGetValue(key, out entry!))
            {
                entry = new Entry();
                _entries[key] = entry;
            }

            entry.References++;
        }

        var waited = false;
        try
        {
            if (!entry.Semaphore.Wait(0, CancellationToken.None))
            {
                waited = true;
                await entry.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            Release(entry, held: false);
            throw;
        }

        return new Releaser(this, entry, waited);
    }

    /// <summary>Disposes guards that have been idle for at least <paramref name="idle"/>.</summary>
    public void Reclaim(TimeSpan idle)
    {
        var cutoff = _time.GetUtcNow() - idle;
        List<Entry>? reclaimed = null;
        lock (_gate)
        {
            List<string>? keys = null;
            foreach (var (key, entry) in _entries)
            {
                if (entry.References == 0 && entry.IdleSince <= cutoff)
                {
                    (keys ??= []).Add(key);
                    (reclaimed ??= []).Add(entry);
                }
            }

            if (keys is not null)
            {
                foreach (var key in keys)
                {
                    _entries.Remove(key);
                }
            }
        }

        if (reclaimed is not null)
        {
            foreach (var entry in reclaimed)
            {
                entry.Semaphore.Dispose();
            }
        }
    }

    public void Dispose()
    {
        List<Entry> entries;
        lock (_gate)
        {
            entries = [.. _entries.Values];
            _entries.Clear();
        }

        foreach (var entry in entries)
        {
            // A guard still awaited is left to the garbage collector; disposing a SemaphoreSlim
            // with waiters is a no-op for the wait handle that was never allocated.
            if (entry.References == 0)
            {
                entry.Semaphore.Dispose();
            }
        }
    }

    private void Release(Entry entry, bool held)
    {
        if (held)
        {
            entry.Semaphore.Release();
        }

        lock (_gate)
        {
            entry.References--;
            if (entry.References == 0)
            {
                entry.IdleSince = _time.GetUtcNow();
            }
        }
    }

    internal sealed class Entry
    {
        public readonly SemaphoreSlim Semaphore = new(1, 1);
        public int References;
        public DateTimeOffset IdleSince;
    }

    public readonly struct Releaser(KeyedAsyncGuard owner, Entry entry, bool waited) : IDisposable
    {
        /// <summary>Whether another caller held the guard first, so the tiers may have been filled.</summary>
        public bool Waited { get; } = waited;

        public void Dispose() => owner.Release(entry, held: true);
    }
}
