namespace HostLoom.NodeLeasing;

/// <summary>
/// Which small integer node ID this instance holds in a pool. At most one instance holds an ID
/// while clocks and the lock backend behave; on lease expiry there is a window, bounded by the
/// lease plus clock skew, in which the previous holder may still believe it holds the ID. Work
/// that must stop when the assignment ends honours <see cref="AssignmentToken"/>; nothing else
/// bounds a stale holder.
/// </summary>
public interface INodeLeasing
{
    /// <summary>The pool the ID is leased from, such as <c>orders-workers</c>.</summary>
    string Pool { get; }

    /// <summary>
    /// The ID this instance holds, between zero and <c>NodeLeasing:PoolSize</c> minus one;
    /// <see langword="null"/> while unassigned.
    /// </summary>
    int? NodeId { get; }

    /// <summary>
    /// <see langword="true"/> while the assignment is decided by a lease on a shared backend, so
    /// at most one instance holds an ID. <see langword="false"/> when the lock does not
    /// coordinate (<c>Locking:Enabled = false</c>): <see cref="NodeId"/> then follows
    /// <see cref="NodeLeasingOptions.WhenUncoordinated"/> on every instance alike, and nothing
    /// excludes another instance from holding the same ID.
    /// </summary>
    bool IsCoordinated { get; }

    /// <summary>
    /// Rises on every acquisition. It counts this instance's acquisitions and is not a fencing
    /// token: the lock provider does not issue one, so a storage layer cannot use it to reject a
    /// stale holder's writes.
    /// </summary>
    long Term { get; }

    /// <summary>
    /// Cancelled when the assignment is lost, released, or stopped, and already cancelled while
    /// this instance holds no ID. A loop that needs an ID runs until it cancels.
    /// </summary>
    CancellationToken AssignmentToken { get; }

    /// <summary>
    /// Completes with the assigned ID when this instance acquires one, at once when it already
    /// holds one.
    /// </summary>
    /// <remarks>Pending and future waits are canceled after stop. Calls after disposal throw <see cref="ObjectDisposedException"/>.</remarks>
    ValueTask<int> WaitForAssignmentAsync(CancellationToken cancellationToken = default);

    /// <summary>Observes every change; the returned disposable unsubscribes.</summary>
    IDisposable OnChange(Action<NodeLeaseChange> listener);
}

/// <summary>One assignment transition.</summary>
/// <param name="Pool">The pool.</param>
/// <param name="NodeId">The ID after the change; <see langword="null"/> once unassigned.</param>
/// <param name="Term">The term after the change.</param>
/// <param name="Reason">What caused it.</param>
public sealed record NodeLeaseChange(
    string Pool,
    int? NodeId,
    long Term,
    NodeLeaseChangeReason Reason
);

/// <summary>Why an assignment changed.</summary>
public enum NodeLeaseChangeReason
{
    /// <summary>The lease was taken; this instance now holds the ID.</summary>
    Acquired,

    /// <summary>A renewal was refused or the lease ran out; this instance no longer holds the ID.</summary>
    Lost,

    /// <summary>This instance released the ID on request and stays a candidate.</summary>
    Released,

    /// <summary>The elector stopped and released the lease.</summary>
    Stopped,
}
