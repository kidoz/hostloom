namespace HostLoom.Leadership;

/// <summary>
/// What an elector does when its lock does not coordinate across instances, which is the case
/// with <c>Locking:Enabled = false</c>: the lock hands out a placeholder lease that every
/// instance is granted at once, so a lease no longer proves that this instance is the only
/// leader. Set through <see cref="LeadershipOptions.WhenUncoordinated"/>.
/// </summary>
public enum UncoordinatedLeadership
{
    /// <summary>
    /// Never lead. The elector stays a candidate, leader-only work waits, exclusive schedules
    /// guarded by the leader are skipped on every instance, and the condition is logged and
    /// reported by the probe. The default, so several replicas over a disabled lock cannot all
    /// lead at once without anyone having asked for it.
    /// </summary>
    Follow,

    /// <summary>
    /// Lead at once, without coordination. Meant for a single-instance deployment or local
    /// development, where the only instance should behave as the leader; every instance
    /// configured this way leads at the same time.
    /// </summary>
    Lead,
}
