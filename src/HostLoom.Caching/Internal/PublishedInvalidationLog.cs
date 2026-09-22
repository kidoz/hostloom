namespace HostLoom.Caching.Internal;

/// <summary>
/// What an instance published and has not yet seen come back. A channel echoes every publish to
/// its publisher; applying the echo as a fresh invalidation would suppress the very refill that
/// follows a removal, so an echo is recognised and skipped. Bounded by count and by
/// <see cref="CacheInvalidationOptions.Timeout"/>: an echo later than that is treated as someone
/// else's message.
/// </summary>
internal sealed class PublishedInvalidationLog(TimeProvider time, CacheInvalidationOptions options)
{
    private const int Capacity = 64;
    private readonly Lock _gate = new();
    private readonly Queue<Published> _published = new();

    /// <summary>Remembers a message about to be published; call it before the publish, since the echo can arrive first.</summary>
    public void Remember(CacheInvalidation invalidation)
    {
        var remembered = new Published(
            Fingerprint(invalidation),
            invalidation,
            time.GetTimestamp()
        );
        lock (_gate)
        {
            _published.Enqueue(remembered);
            while (_published.Count > Capacity)
            {
                _published.Dequeue();
            }
        }
    }

    /// <summary>Forgets a message whose publish failed: no echo will come.</summary>
    public void Forget(CacheInvalidation invalidation)
    {
        lock (_gate)
        {
            if (_published.Count == 0)
            {
                return;
            }

            var kept = _published.Where(entry => !ReferenceEquals(entry.Message, invalidation));
            var remaining = kept.ToArray();
            _published.Clear();
            foreach (var entry in remaining)
            {
                _published.Enqueue(entry);
            }
        }
    }

    /// <summary>
    /// Whether <paramref name="invalidation"/> is the echo of a message remembered within the
    /// publish timeout, consuming the remembered publish when it is.
    /// </summary>
    public bool IsOwnEcho(CacheInvalidation invalidation)
    {
        var hash = Fingerprint(invalidation);
        lock (_gate)
        {
            if (_published.Count == 0)
            {
                return false;
            }

            var matched = false;
            var remaining = new List<Published>(_published.Count);
            while (_published.TryDequeue(out var entry))
            {
                if (time.GetElapsedTime(entry.PublishedAt) > options.Timeout)
                {
                    continue;
                }

                if (!matched && entry.Hash == hash && SameMessage(entry.Message, invalidation))
                {
                    matched = true;
                    continue;
                }

                remaining.Add(entry);
            }

            foreach (var entry in remaining)
            {
                _published.Enqueue(entry);
            }

            return matched;
        }
    }

    private static int Fingerprint(CacheInvalidation invalidation)
    {
        var hash = new HashCode();
        hash.Add(invalidation.FlushAll);
        hash.Add(invalidation.Keys.Count);
        foreach (var key in invalidation.Keys)
        {
            hash.Add(key, StringComparer.Ordinal);
        }

        hash.Add(invalidation.Tags.Count);
        foreach (var tag in invalidation.Tags)
        {
            hash.Add(tag, StringComparer.Ordinal);
        }

        return hash.ToHashCode();
    }

    private static bool SameMessage(CacheInvalidation left, CacheInvalidation right) =>
        left.FlushAll == right.FlushAll
        && left.Keys.SequenceEqual(right.Keys, StringComparer.Ordinal)
        && left.Tags.SequenceEqual(right.Tags, StringComparer.Ordinal);

    private readonly record struct Published(int Hash, CacheInvalidation Message, long PublishedAt);
}
