namespace HostLoom.Leadership;

/// <summary>
/// Whether this instance currently leads a role. At most one instance leads a role while clocks
/// and the lock backend behave; on lease expiry there is a window, bounded by the lease plus
/// clock skew, in which the previous leader may still believe it leads. Work that must stop when
/// leadership ends honours <see cref="LeadershipToken"/>; nothing else bounds a stale leader.
/// </summary>
public interface ILeadership
{
    /// <summary>The role, such as <c>scheduler</c> or <c>billing</c>.</summary>
    string Role { get; }

    /// <summary><see langword="true"/> while this instance believes it leads the role.</summary>
    bool IsLeader { get; }

    /// <summary>
    /// Rises on every acquisition. It counts this instance's acquisitions and is not a fencing
    /// token: the lock provider does not issue one, so a storage layer cannot use it to reject a
    /// stale leader's writes.
    /// </summary>
    long Term { get; }

    /// <summary>
    /// Cancelled when leadership is lost, resigned, or stopped, and already cancelled while this
    /// instance is not the leader. A leader-only loop runs until it cancels.
    /// </summary>
    CancellationToken LeadershipToken { get; }

    /// <summary>Completes when this instance becomes leader, at once when it already is.</summary>
    ValueTask WaitForLeadershipAsync(CancellationToken cancellationToken = default);

    /// <summary>Observes every change; the returned disposable unsubscribes.</summary>
    IDisposable OnChange(Action<LeadershipChange> listener);
}

/// <summary>One leadership transition.</summary>
/// <param name="Role">The role.</param>
/// <param name="IsLeader">Whether this instance leads after the change.</param>
/// <param name="Term">The term after the change.</param>
/// <param name="Reason">What caused it.</param>
public sealed record LeadershipChange(
    string Role,
    bool IsLeader,
    long Term,
    LeadershipChangeReason Reason
);

/// <summary>Why leadership changed.</summary>
public enum LeadershipChangeReason
{
    /// <summary>The lease was taken; this instance now leads.</summary>
    Acquired,

    /// <summary>A renewal failed or the lease expired; this instance no longer leads.</summary>
    Lost,

    /// <summary>This instance stepped down on request and stays a candidate.</summary>
    Resigned,

    /// <summary>The elector stopped and released the lease.</summary>
    Stopped,
}
