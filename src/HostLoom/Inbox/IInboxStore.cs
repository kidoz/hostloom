namespace HostLoom;

/// <summary>
/// Remembers which deliveries a subscription has already accepted, so a redelivered event runs
/// its handlers once. One member, the atomic set-if-absent every idempotency store reduces to.
/// </summary>
public interface IInboxStore
{
    /// <summary>
    /// Records <paramref name="key"/> for <paramref name="window"/>. Returns <see langword="true"/>
    /// when the key was not present, so the delivery is the first, and <see langword="false"/>
    /// when it was, so the delivery is a duplicate. A backend failure is thrown; the filter then
    /// runs the handlers anyway, because processing twice is recoverable and dropping is not.
    /// </summary>
    ValueTask<bool> TryRecordAsync(
        string key,
        TimeSpan window,
        CancellationToken cancellationToken = default
    );
}

/// <summary>Builds an <see cref="IInboxStore"/> from a set-if-absent delegate.</summary>
public static class InboxStore
{
    /// <summary>
    /// Wraps <paramref name="tryRecord"/>, for example a cache's set-if-absent with the key as its
    /// own value: <c>(key, window, ct) =&gt; cache.SetIfAbsentAsync(key, key, new CacheEntryOptions(window) { OnUnavailable = UnavailableBehavior.Throw }, ct)</c>.
    /// </summary>
    public static IInboxStore FromClaim(
        Func<string, TimeSpan, CancellationToken, ValueTask<bool>> tryRecord
    )
    {
        ArgumentNullException.ThrowIfNull(tryRecord);
        return new DelegateInboxStore(tryRecord);
    }

    private sealed class DelegateInboxStore(
        Func<string, TimeSpan, CancellationToken, ValueTask<bool>> tryRecord
    ) : IInboxStore
    {
        public ValueTask<bool> TryRecordAsync(
            string key,
            TimeSpan window,
            CancellationToken cancellationToken = default
        ) => tryRecord(key, window, cancellationToken);
    }
}
