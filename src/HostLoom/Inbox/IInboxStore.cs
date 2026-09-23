namespace HostLoom;

/// <summary>
/// Remembers which deliveries a subscription has already accepted, so a redelivered event runs
/// its handlers once. <see cref="TryRecordAsync"/> is the atomic set-if-absent every idempotency
/// store reduces to; <see cref="ReleaseAsync"/> forgets a key whose handlers did not complete.
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

    /// <summary>
    /// Forgets <paramref name="key"/>, which <see cref="TryRecordAsync"/> recorded for a delivery
    /// whose handlers then threw or were cancelled, so the transport's redelivery runs them again.
    /// The filter calls it once, best-effort, with a short timeout of its own rather than the
    /// delivery's token, and logs a failure without letting it replace the handlers' exception.
    /// Removing a key that is no longer present must succeed.
    /// </summary>
    /// <remarks>
    /// The default does nothing, so a store written before this member existed still compiles.
    /// Such a store keeps a failed run's key: every redelivery of that delivery inside the window
    /// is then treated as a duplicate and acknowledged without running the handlers, which loses
    /// the event. Override it in every store that can delete a key.
    /// </remarks>
    ValueTask ReleaseAsync(string key, CancellationToken cancellationToken = default) =>
        ValueTask.CompletedTask;
}

/// <summary>Builds an <see cref="IInboxStore"/> from delegates.</summary>
public static class InboxStore
{
    /// <summary>
    /// Wraps <paramref name="tryRecord"/> alone. The store cannot release a key, so a delivery
    /// whose handlers fail keeps its key and its redeliveries are dropped inside the window;
    /// prefer <see cref="FromClaim(Func{string, TimeSpan, CancellationToken, ValueTask{bool}}, Func{string, CancellationToken, ValueTask})"/>.
    /// </summary>
    public static IInboxStore FromClaim(
        Func<string, TimeSpan, CancellationToken, ValueTask<bool>> tryRecord
    )
    {
        ArgumentNullException.ThrowIfNull(tryRecord);
        return new DelegateInboxStore(tryRecord, release: null);
    }

    /// <summary>
    /// Wraps <paramref name="tryRecord"/> and <paramref name="release"/>, for example a cache's
    /// set-if-absent with the key as its own value and the cache's remove:
    /// <code>
    /// InboxStore.FromClaim(
    ///     (key, window, ct) =&gt; cache.SetIfAbsentAsync(key, key, new CacheEntryOptions(window) { OnUnavailable = UnavailableBehavior.Throw }, ct),
    ///     (key, ct) =&gt; cache.RemoveAsync(key, ct))
    /// </code>
    /// </summary>
    public static IInboxStore FromClaim(
        Func<string, TimeSpan, CancellationToken, ValueTask<bool>> tryRecord,
        Func<string, CancellationToken, ValueTask> release
    )
    {
        ArgumentNullException.ThrowIfNull(tryRecord);
        ArgumentNullException.ThrowIfNull(release);
        return new DelegateInboxStore(tryRecord, release);
    }

    private sealed class DelegateInboxStore(
        Func<string, TimeSpan, CancellationToken, ValueTask<bool>> tryRecord,
        Func<string, CancellationToken, ValueTask>? release
    ) : IInboxStore
    {
        public ValueTask<bool> TryRecordAsync(
            string key,
            TimeSpan window,
            CancellationToken cancellationToken = default
        ) => tryRecord(key, window, cancellationToken);

        public ValueTask ReleaseAsync(string key, CancellationToken cancellationToken = default) =>
            release is null ? ValueTask.CompletedTask : release(key, cancellationToken);
    }
}
