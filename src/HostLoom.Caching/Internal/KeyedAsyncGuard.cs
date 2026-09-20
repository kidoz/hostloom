namespace HostLoom.Caching.Internal;

/// <summary>
/// Per-key asynchronous mutual exclusion for single-flight. Guards are reference-counted so one
/// is never disposed while a caller awaits it, and an idle guard normally survives until
/// <see cref="CacheL1Options.GuardIdleTime"/> has passed. The map is bounded: once it holds
/// <c>maxGuards</c> keys, idle guards are reclaimed at once rather than after the idle window,
/// and a key that still finds no room (every tracked guard is held or awaited) is guarded by one
/// of a fixed set of striped semaphores instead. Memory therefore grows with in-flight callers,
/// never with the number of distinct keys a process has seen. Guarding one key never blocks
/// another while the map has room; under overflow, keys sharing a stripe wait for each other.
/// </summary>
internal sealed class KeyedAsyncGuard : IDisposable
{
    /// <summary>Striped semaphores used once the per-key map is full of held guards.</summary>
    internal const int StripeCount = 256;

    private readonly Lock _gate = new();
    private readonly Dictionary<string, Entry> _entries = new(StringComparer.Ordinal);
    private readonly TimeProvider _time;
    private readonly int _maxGuards;
    private Entry[]? _stripes;
    private int _stripedCallers;
    private long _overflows;

    /// <param name="time">Clock for idle accounting.</param>
    /// <param name="maxGuards">Most keys tracked with a guard of their own before overflow.</param>
    public KeyedAsyncGuard(TimeProvider time, int maxGuards = int.MaxValue)
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(maxGuards, 1);
        _time = time;
        _maxGuards = maxGuards;
    }

    /// <summary>Guards that are held or awaited right now, striped ones counted per caller.</summary>
    public int ActiveCount
    {
        get
        {
            lock (_gate)
            {
                var active = _stripedCallers;
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

    /// <summary>Keys that currently have a guard of their own, held or idle.</summary>
    internal int TrackedCount
    {
        get
        {
            lock (_gate)
            {
                return _entries.Count;
            }
        }
    }

    /// <summary>Acquisitions that fell back to a stripe because the map was full of held guards.</summary>
    internal long Overflows => Interlocked.Read(ref _overflows);

    public async ValueTask<Releaser> AcquireAsync(string key, CancellationToken cancellationToken)
    {
        var lease = Take(key);
        var waited = false;
        try
        {
            if (!lease.Primary.Semaphore.Wait(0, CancellationToken.None))
            {
                waited = true;
                await lease.Primary.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
            }
        }
        catch
        {
            Release(lease, heldPrimary: false, heldStripe: false);
            throw;
        }

        if (lease.Stripe is { } stripe)
        {
            try
            {
                if (!stripe.Semaphore.Wait(0, CancellationToken.None))
                {
                    waited = true;
                    await stripe.Semaphore.WaitAsync(cancellationToken).ConfigureAwait(false);
                }
            }
            catch
            {
                Release(lease, heldPrimary: true, heldStripe: false);
                throw;
            }
        }

        return new Releaser(this, lease, waited);
    }

    /// <summary>
    /// Acquires the guard for <paramref name="key"/> only when nobody holds it, without waiting.
    /// Lets a caller decide whether to become the one refresher or to take a stale value instead.
    /// </summary>
    public bool TryAcquire(string key, out Releaser releaser)
    {
        var lease = Take(key);
        if (!lease.Primary.Semaphore.Wait(0, CancellationToken.None))
        {
            Release(lease, heldPrimary: false, heldStripe: false);
            releaser = default;
            return false;
        }

        if (lease.Stripe is { } stripe && !stripe.Semaphore.Wait(0, CancellationToken.None))
        {
            Release(lease, heldPrimary: true, heldStripe: false);
            releaser = default;
            return false;
        }

        releaser = new Releaser(this, lease, waited: false);
        return true;
    }

    /// <summary>Disposes guards that have been idle for at least <paramref name="idle"/>.</summary>
    public void Reclaim(TimeSpan idle)
    {
        var cutoff = _time.GetUtcNow() - idle;
        List<Entry>? reclaimed;
        lock (_gate)
        {
            reclaimed = ReclaimIdleLocked(cutoff);
        }

        DisposeAll(reclaimed);
    }

    public void Dispose()
    {
        List<Entry> entries;
        lock (_gate)
        {
            entries = [.. _entries.Values];
            if (_stripes is { } stripes)
            {
                entries.AddRange(stripes);
                _stripes = null;
            }

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

    private Lease Take(string key)
    {
        List<Entry>? reclaimed = null;
        Lease lease;
        lock (_gate)
        {
            if (_entries.TryGetValue(key, out var existing))
            {
                existing.References++;
                Entry? stripe = null;
                if (existing.RequiresStripe)
                {
                    stripe = StripeFor(key);
                    stripe.References++;
                }

                lease = new Lease(existing, stripe, Striped: false);
            }
            else
            {
                if (_entries.Count >= _maxGuards)
                {
                    // Full: reclaim every idle guard now instead of waiting for the idle window.
                    reclaimed = ReclaimIdleLocked(DateTimeOffset.MaxValue);
                }

                if (_entries.Count >= _maxGuards)
                {
                    // Still full, so every tracked guard is in use. Fall back to a stripe rather
                    // than growing without bound; keys sharing the stripe serialise on it.
                    var stripe = StripeFor(key);
                    stripe.References++;
                    _stripedCallers++;
                    _overflows++;
                    lease = new Lease(stripe, null, Striped: true);
                }
                else
                {
                    var entry = new Entry();
                    _entries[key] = entry;
                    entry.References++;
                    Entry? stripe = null;
                    if (_stripedCallers > 0)
                    {
                        // A caller for this same key may be guarded by its stripe right now.
                        // Every caller through this entry takes the stripe as well, for as
                        // long as the entry lives, so the two stay exclusive across the switch
                        // from striped to per-key guarding.
                        entry.RequiresStripe = true;
                        stripe = StripeFor(key);
                        stripe.References++;
                    }

                    lease = new Lease(entry, stripe, Striped: false);
                }
            }
        }

        DisposeAll(reclaimed);
        return lease;
    }

    private Entry StripeFor(string key)
    {
        var stripes = _stripes;
        if (stripes is null)
        {
            stripes = new Entry[StripeCount];
            for (var i = 0; i < stripes.Length; i++)
            {
                stripes[i] = new Entry();
            }

            _stripes = stripes;
        }

        return stripes[(int)((uint)key.GetHashCode(StringComparison.Ordinal) % StripeCount)];
    }

    private List<Entry>? ReclaimIdleLocked(DateTimeOffset cutoff)
    {
        List<Entry>? reclaimed = null;
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

        return reclaimed;
    }

    private static void DisposeAll(List<Entry>? entries)
    {
        if (entries is null)
        {
            return;
        }

        foreach (var entry in entries)
        {
            entry.Semaphore.Dispose();
        }
    }

    private void Release(Lease lease, bool heldPrimary, bool heldStripe)
    {
        if (heldStripe)
        {
            lease.Stripe!.Semaphore.Release();
        }

        if (heldPrimary)
        {
            lease.Primary.Semaphore.Release();
        }

        lock (_gate)
        {
            lease.Primary.References--;
            if (lease.Primary.References == 0)
            {
                lease.Primary.IdleSince = _time.GetUtcNow();
            }

            if (lease.Stripe is { } stripe)
            {
                stripe.References--;
            }

            if (lease.Striped)
            {
                _stripedCallers--;
            }
        }
    }

    internal sealed class Entry
    {
        public readonly SemaphoreSlim Semaphore = new(1, 1);
        public int References;
        public DateTimeOffset IdleSince;

        /// <summary>Created while striped callers existed, so its users also take the key's stripe.</summary>
        public bool RequiresStripe;
    }

    /// <summary>
    /// What one acquisition holds: its primary guard (a per-key entry, or a stripe when
    /// <paramref name="Striped"/>), and the key's stripe as a secondary guard when the entry
    /// was created while striped callers existed.
    /// </summary>
    internal readonly record struct Lease(Entry Primary, Entry? Stripe, bool Striped);

    public readonly struct Releaser(KeyedAsyncGuard owner, Lease lease, bool waited) : IDisposable
    {
        /// <summary>Whether another caller held the guard first, so the tiers may have been filled.</summary>
        public bool Waited { get; } = waited;

        public void Dispose() =>
            owner.Release(lease, heldPrimary: true, heldStripe: lease.Stripe is not null);
    }
}
