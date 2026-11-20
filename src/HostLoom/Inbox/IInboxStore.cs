namespace HostLoom;

/// <summary>Remembers accepted deliveries for a subscription's deduplication window.</summary>
public interface IInboxStore
{
    /// <summary>
    /// Atomically records the delivery's unique <paramref name="claimToken"/> with the key and
    /// expiry. Returns false if an unexpired record already exists. A backend failure is thrown.
    /// </summary>
    ValueTask<bool> TryRecordAsync(
        string key,
        Guid claimToken,
        TimeSpan window,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Atomically deletes the record only when it still contains this delivery's claim token.
    /// A missing key or a different owner is a successful no-op. A record-only store keeps
    /// the default implementation and suppresses failed deliveries until expiry.
    /// </summary>
    ValueTask ReleaseAsync(
        string key,
        Guid claimToken,
        CancellationToken cancellationToken = default
    ) => ValueTask.CompletedTask;
}

/// <summary>Builds an <see cref="IInboxStore"/> from delegates.</summary>
public static class InboxStore
{
    /// <summary>Wraps a record-only store. Failed deliveries keep their key until expiry.</summary>
    public static IInboxStore FromClaim(
        Func<string, TimeSpan, CancellationToken, ValueTask<bool>> tryRecord
    )
    {
        ArgumentNullException.ThrowIfNull(tryRecord);
        return new DelegateInboxStore(
            (key, _, window, ct) => tryRecord(key, window, ct),
            release: null
        );
    }

    /// <summary>Wraps an atomic insert-if-absent and an atomic compare-and-delete by claim token.</summary>
    public static IInboxStore FromClaim(
        Func<string, Guid, TimeSpan, CancellationToken, ValueTask<bool>> tryRecord,
        Func<string, Guid, CancellationToken, ValueTask> release
    )
    {
        ArgumentNullException.ThrowIfNull(tryRecord);
        ArgumentNullException.ThrowIfNull(release);
        return new DelegateInboxStore(tryRecord, release);
    }

    private sealed class DelegateInboxStore(
        Func<string, Guid, TimeSpan, CancellationToken, ValueTask<bool>> tryRecord,
        Func<string, Guid, CancellationToken, ValueTask>? release
    ) : IInboxStore
    {
        public ValueTask<bool> TryRecordAsync(
            string key,
            Guid claimToken,
            TimeSpan window,
            CancellationToken cancellationToken = default
        ) => tryRecord(key, claimToken, window, cancellationToken);

        public ValueTask ReleaseAsync(
            string key,
            Guid claimToken,
            CancellationToken cancellationToken = default
        ) =>
            release is null ? ValueTask.CompletedTask : release(key, claimToken, cancellationToken);
    }
}
