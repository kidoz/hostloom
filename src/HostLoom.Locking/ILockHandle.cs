namespace HostLoom.Locking;

/// <summary>
/// One acquired lease. Disposing releases it; a release by anyone other than the owner is
/// refused by the provider and reported here as a lost lease rather than thrown.
/// </summary>
public interface ILockHandle : IAsyncDisposable
{
    /// <summary>The consumer key, without the namespace prefix.</summary>
    string Key { get; }

    /// <summary>
    /// <see langword="true"/> while the lease is believed held. Turns <see langword="false"/> when
    /// the lease expires on the local clock, when the provider reports an owner mismatch on extend
    /// or release, or after <see cref="IAsyncDisposable.DisposeAsync"/>.
    /// </summary>
    bool IsHeld { get; }

    /// <summary>When the current lease ends unless extended.</summary>
    DateTimeOffset LeaseEnd { get; }

    /// <summary>
    /// Cancelled when the lease is lost. Never cancelled by a normal release, so an action that
    /// honours it stops only when exclusivity is no longer guaranteed.
    /// </summary>
    CancellationToken LostToken { get; }

    /// <summary>
    /// Extends the lease to <paramref name="lease"/> from now, capped by
    /// <see cref="LockingOptions.MaxLease"/>. Returns <see langword="false"/> when the lease was
    /// already lost or the provider refused; a provider failure is logged and reported as
    /// <see langword="false"/> rather than thrown.
    /// </summary>
    ValueTask<bool> ExtendAsync(TimeSpan lease, CancellationToken cancellationToken = default);
}
