using System.Globalization;

namespace HostLoom.Leadership;

/// <summary>Execution-free description of an elector, safe to call from a health or debug endpoint.</summary>
public static class LeadershipProbe
{
    /// <summary>Describes <paramref name="elector"/>: role, state, term, lease, renewal, and retry cadence.</summary>
    public static LeadershipDescription Describe(LeaderElector elector)
    {
        ArgumentNullException.ThrowIfNull(elector);
        var invariant = CultureInfo.InvariantCulture;
        var options = elector.Options;
        var state = elector.Status;
        List<string> lines =
        [
            $"Role = {elector.Role}: {StatusName(state)}, term {elector.Term.ToString(invariant)}",
            string.Create(
                invariant,
                $"Leadership:Lease = {options.Lease}, renewed every Leadership:RenewInterval = {options.RenewInterval}"
            ),
            string.Create(
                invariant,
                $"Leadership:RetryInterval = {options.RetryInterval}, jitter Leadership:RetryJitter = {options.RetryJitter}"
            ),
            "Guarantee = at most one leader while clocks and the lock backend behave; no fencing",
        ];
        return new LeadershipDescription(
            elector.Role,
            state,
            elector.Term,
            options.Lease,
            options.RenewInterval,
            lines
        );
    }

    internal static string StatusName(LeadershipStatus status) =>
        status switch
        {
            LeadershipStatus.Idle => "idle",
            LeadershipStatus.Candidate => "candidate",
            LeadershipStatus.Leader => "leader",
            _ => "stopped",
        };
}

/// <summary>What <see cref="LeadershipProbe.Describe"/> reports.</summary>
/// <param name="Role">The role.</param>
/// <param name="Status">Candidate, leader, or stopped.</param>
/// <param name="Term">The current term.</param>
/// <param name="Lease"><see cref="LeadershipOptions.Lease"/>.</param>
/// <param name="RenewInterval"><see cref="LeadershipOptions.RenewInterval"/>.</param>
/// <param name="Lines">Human-readable lines, each naming the option that decided it.</param>
public sealed record LeadershipDescription(
    string Role,
    LeadershipStatus Status,
    long Term,
    TimeSpan Lease,
    TimeSpan RenewInterval,
    IReadOnlyList<string> Lines
);

/// <summary>Where an elector stands.</summary>
public enum LeadershipStatus
{
    /// <summary>Not started yet.</summary>
    Idle,

    /// <summary>Started and trying to acquire the lease.</summary>
    Candidate,

    /// <summary>Holding and renewing the lease.</summary>
    Leader,

    /// <summary>Stopped; the lease it held was released.</summary>
    Stopped,
}
