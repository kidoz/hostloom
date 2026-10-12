namespace HostLoom.NodeLeasing;

/// <summary>
/// What an elector does when its lock does not coordinate across instances, which is the case
/// with <c>Locking:Enabled = false</c>: the lock hands out a placeholder lease that every
/// instance is granted at once, so a lease no longer proves that this instance is the only
/// holder of an ID. Set through <see cref="NodeLeasingOptions.WhenUncoordinated"/>.
/// </summary>
public enum UncoordinatedNodeAssignment
{
    /// <summary>
    /// Never assign an ID. The elector stays a candidate, work that needs an ID waits, and the
    /// condition is logged and reported by the probe. The default, so several replicas over a
    /// disabled lock cannot all take an ID without anyone having asked for it.
    /// </summary>
    None,

    /// <summary>
    /// Assign ID zero at once, without coordination. Meant for a single-instance deployment or
    /// local development, where the only instance should have a deterministic ID; every instance
    /// configured this way holds ID zero at the same time.
    /// </summary>
    AssignZero,
}
