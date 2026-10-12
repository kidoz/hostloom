using System.Globalization;

namespace HostLoom.NodeLeasing;

/// <summary>Execution-free description of an elector, safe to call from a health or debug endpoint.</summary>
public static class NodeLeasingProbe
{
    /// <summary>Describes <paramref name="elector"/>: pool, state, ID, term, coordination, lease, renewal, and retry cadence.</summary>
    public static NodeLeaseDescription Describe(NodeIdElector elector)
    {
        ArgumentNullException.ThrowIfNull(elector);
        var invariant = CultureInfo.InvariantCulture;
        var options = elector.Options;
        var state = elector.Status;
        var coordinated = elector.IsCoordinated;
        var assignment = elector.NodeId is { } id
            ? $"node {id.ToString(invariant)}"
            : "no assignment";
        List<string> lines =
        [
            $"Pool = {elector.Pool}: {StatusName(state)}, {assignment}, term {elector.Term.ToString(invariant)}",
            string.Create(
                invariant,
                $"NodeLeasing:PoolSize = {options.PoolSize}, NodeLeasing:Lease = {options.Lease}, renewed every NodeLeasing:RenewInterval = {options.RenewInterval}"
            ),
            string.Create(
                invariant,
                $"NodeLeasing:RetryInterval = {options.RetryInterval}, jitter NodeLeasing:RetryJitter = {options.RetryJitter}"
            ),
            coordinated ? "Coordination = lease on the lock backend (Locking:Enabled = true)"
            : options.WhenUncoordinated == UncoordinatedNodeAssignment.AssignZero
                ? "Coordination = none (Locking:Enabled = false): NodeLeasing:WhenUncoordinated = AssignZero, this instance holds node 0 without coordination"
            : "Coordination = none (Locking:Enabled = false): NodeLeasing:WhenUncoordinated = None, this instance is never assigned",
            "Guarantee = at most one holder per node ID while clocks and the lock backend behave; no fencing",
        ];
        return new NodeLeaseDescription(
            elector.Pool,
            state,
            coordinated,
            elector.NodeId,
            elector.Term,
            options.Lease,
            options.RenewInterval,
            lines
        );
    }

    internal static string StatusName(NodeLeasingStatus status) =>
        status switch
        {
            NodeLeasingStatus.Idle => "idle",
            NodeLeasingStatus.Candidate => "candidate",
            NodeLeasingStatus.Assigned => "assigned",
            _ => "stopped",
        };
}

/// <summary>What <see cref="NodeLeasingProbe.Describe"/> reports.</summary>
/// <param name="Pool">The pool.</param>
/// <param name="Status">Candidate, assigned, or stopped.</param>
/// <param name="Coordinated"><see cref="INodeLeasing.IsCoordinated"/>: <see langword="false"/> over a disabled lock, where the assignment follows <see cref="NodeLeasingOptions.WhenUncoordinated"/>.</param>
/// <param name="NodeId">The ID held; <see langword="null"/> while unassigned.</param>
/// <param name="Term">The current term.</param>
/// <param name="Lease"><see cref="NodeLeasingOptions.Lease"/>.</param>
/// <param name="RenewInterval"><see cref="NodeLeasingOptions.RenewInterval"/>.</param>
/// <param name="Lines">Human-readable lines, each naming the option that decided it.</param>
public sealed record NodeLeaseDescription(
    string Pool,
    NodeLeasingStatus Status,
    bool Coordinated,
    int? NodeId,
    long Term,
    TimeSpan Lease,
    TimeSpan RenewInterval,
    IReadOnlyList<string> Lines
);

/// <summary>Where an elector stands.</summary>
public enum NodeLeasingStatus
{
    /// <summary>Not started yet.</summary>
    Idle,

    /// <summary>Started and scanning for a free ID.</summary>
    Candidate,

    /// <summary>Holding and renewing the lease on an ID.</summary>
    Assigned,

    /// <summary>Stopped; the lease it held was released.</summary>
    Stopped,
}
