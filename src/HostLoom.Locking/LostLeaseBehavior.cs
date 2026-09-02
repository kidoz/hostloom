namespace HostLoom.Locking;

/// <summary>What <see cref="IDistributedLock.ExecuteWithLockAsync{T}"/> does when the lease is lost mid-action.</summary>
public enum LostLeaseBehavior
{
    /// <summary>
    /// The action keeps running; <see cref="ILockHandle.IsHeld"/>, <see cref="ILockHandle.LostToken"/>,
    /// the <c>hostloom.lock.lost</c> counter, and a warning log report the loss. Today's behaviour.
    /// </summary>
    Observe,

    /// <summary>The token handed to the action is cancelled when the lease is lost.</summary>
    Cancel,
}
