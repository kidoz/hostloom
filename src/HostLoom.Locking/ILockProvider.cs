namespace HostLoom.Locking;

/// <summary>
/// The backend contract behind <see cref="IDistributedLock"/>. Keys arrive fully prefixed
/// (<c>{namespace}:lock:{key}</c>); the provider sees no consumer keys and no options. Contention
/// and owner mismatch are <see langword="false"/>; a backend failure is
/// <see cref="LockProviderException"/>; cancellation is <see cref="OperationCanceledException"/>.
/// </summary>
public interface ILockProvider
{
    /// <summary>
    /// Takes <paramref name="key"/> for <paramref name="owner"/> for <paramref name="lease"/>, or
    /// returns <see langword="false"/> when another owner holds an unexpired lease. Honour
    /// <paramref name="cancellationToken"/> as any .NET API does.
    /// </summary>
    /// <remarks>
    /// The composed lock never cancels an acquisition it has sent before the lease has run out:
    /// the caller's token and <see cref="LockOptions.MaxWait"/> only end its wait for the reply,
    /// so a grant that arrives after the caller stopped waiting still reaches the lock, which
    /// releases it for <paramref name="owner"/>. The lock releases once as well after a
    /// <see cref="LockProviderException"/> of kind <see cref="LockFailureKind.Timeout"/> or
    /// <see cref="LockFailureKind.Other"/>, because the command may have been applied; report
    /// <see cref="LockFailureKind.Unavailable"/> only when it cannot have reached the backend.
    /// </remarks>
    ValueTask<bool> TryAcquireAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Releases <paramref name="key"/> when <paramref name="owner"/> still holds it. Returns
    /// <see langword="false"/> when the key is absent, expired, or held by another owner.
    /// </summary>
    ValueTask<bool> ReleaseAsync(
        string key,
        string owner,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Moves the lease end of <paramref name="key"/> to <paramref name="lease"/> from now when
    /// <paramref name="owner"/> still holds it. Returns <see langword="false"/> otherwise.
    /// </summary>
    ValueTask<bool> ExtendAsync(
        string key,
        string owner,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    );
}
