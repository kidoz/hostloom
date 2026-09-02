namespace HostLoom.Locking;

/// <summary>
/// The lock consumers inject: acquire a lease on a key, run an action, release. The lock is
/// coordination, not correctness, for persisted state — database transactions, row locks, unique
/// constraints, and idempotency records own correctness. A lease can be lost while an action is
/// still running; see <see cref="ILockHandle.LostToken"/> and <see cref="LockOptions.OnLost"/>.
/// </summary>
public interface IDistributedLock
{
    /// <summary>
    /// Acquires <paramref name="key"/>, runs <paramref name="action"/>, and releases the lease in a
    /// <c>finally</c>. The action's exception propagates unchanged. The token handed to the action
    /// is the caller's token, linked to the handle's <see cref="ILockHandle.LostToken"/> when
    /// <see cref="LockOptions.OnLost"/> is <see cref="LostLeaseBehavior.Cancel"/>.
    /// </summary>
    /// <exception cref="LockNotAcquiredException">
    /// The key stayed held by another owner past the retry policy or <see cref="LockOptions.MaxWait"/>.
    /// </exception>
    /// <exception cref="LockProviderUnavailableException">The provider failed while acquiring.</exception>
    /// <exception cref="LockReentrancyException">
    /// The key is already held by the current asynchronous flow and
    /// <see cref="LockingOptions.DetectReentrancy"/> is on.
    /// </exception>
    ValueTask<T> ExecuteWithLockAsync<T>(
        string key,
        Func<CancellationToken, ValueTask<T>> action,
        LockOptions? options = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Acquires <paramref name="key"/>, runs <paramref name="action"/>, and releases the lease in a
    /// <c>finally</c>. See the generic overload for the exceptions.
    /// </summary>
    ValueTask ExecuteWithLockAsync(
        string key,
        Func<CancellationToken, ValueTask> action,
        LockOptions? options = null,
        CancellationToken cancellationToken = default
    );

    /// <summary>
    /// Tries to acquire <paramref name="key"/> and returns a handle, or <see langword="null"/> when
    /// the key stayed held by another owner. With <paramref name="options"/> omitted the call makes
    /// one attempt and never waits (skip-if-busy); pass <see cref="LockOptions.MaxWait"/> or a
    /// <see cref="LockOptions.Retry"/> policy to wait. Dispose the handle to release.
    /// </summary>
    /// <exception cref="LockProviderUnavailableException">The provider failed while acquiring.</exception>
    /// <exception cref="LockReentrancyException">The key is already held by the current flow.</exception>
    ValueTask<ILockHandle?> TryAcquireAsync(
        string key,
        LockOptions? options = null,
        CancellationToken cancellationToken = default
    );
}
