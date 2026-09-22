namespace HostLoom.Leadership;

/// <summary>
/// How one role is elected. <see cref="Validate"/> reports every violation with the option key it
/// names, so a container-free composition fails the same way a hosted one does.
/// </summary>
public sealed class LeadershipOptions
{
    /// <summary>
    /// The longest wait the elector can schedule on its clock: <c>uint.MaxValue - 1</c>
    /// milliseconds, the bound <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/>
    /// accepts. <see cref="Validate"/> keeps every cadence within it so the loop can never fail on
    /// an out-of-range delay.
    /// </summary>
    public static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    /// How long the lease lasts without a renewal, and so how long a crashed leader keeps the role.
    /// Must be at most <c>Locking:MaxLease</c>: a hosted elector fails at startup otherwise, while a
    /// container-free one is handed the lock's cap as its lease.
    /// </summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>How often the leader renews. Must leave at least one more renewal inside the lease.</summary>
    public TimeSpan RenewInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long a candidate waits between attempts, and a resigned leader before it runs again.</summary>
    public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Uniform additive jitter on <see cref="RetryInterval"/>, so candidates do not attempt in lockstep.</summary>
    public TimeSpan RetryJitter { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// What the elector does when the lock does not coordinate across instances
    /// (<c>Locking:Enabled = false</c>). <see cref="UncoordinatedLeadership.Follow"/>, the
    /// default, never leads, so replicas over a disabled lock cannot all lead at once;
    /// <see cref="UncoordinatedLeadership.Lead"/> leads without coordination for a single-instance
    /// or development deployment. Either way the condition is logged once per role and reported by
    /// <see cref="LeadershipProbe"/> and <see cref="ILeadership.IsCoordinated"/>.
    /// </summary>
    public UncoordinatedLeadership WhenUncoordinated { get; set; } = UncoordinatedLeadership.Follow;

    /// <summary>Every violation, each naming the option key at fault. Empty when the options are usable.</summary>
    public IReadOnlyList<string> Validate()
    {
        List<string> problems = [];
        if (Lease <= TimeSpan.Zero)
        {
            problems.Add("Leadership:Lease must be positive.");
        }

        if (RenewInterval <= TimeSpan.Zero)
        {
            problems.Add("Leadership:RenewInterval must be positive.");
        }
        else if (RenewInterval > MaxDelay)
        {
            problems.Add(
                $"Leadership:RenewInterval must be at most {MaxDelay}, the longest wait the elector can schedule."
            );
        }
        else if (RenewInterval * 2 > Lease)
        {
            problems.Add(
                "Leadership:RenewInterval must be at most half of Leadership:Lease, so a missed renewal still leaves time for another."
            );
        }

        if (RetryInterval <= TimeSpan.Zero)
        {
            problems.Add("Leadership:RetryInterval must be positive.");
        }

        if (RetryJitter < TimeSpan.Zero)
        {
            problems.Add("Leadership:RetryJitter must not be negative.");
        }
        else if (
            RetryInterval > TimeSpan.Zero
            && (RetryInterval > MaxDelay || RetryJitter > MaxDelay - RetryInterval)
        )
        {
            // Compared as a difference rather than a sum, so two large values cannot overflow.
            problems.Add(
                $"Leadership:RetryInterval plus Leadership:RetryJitter must be at most {MaxDelay}, the longest wait the elector can schedule."
            );
        }

        if (!Enum.IsDefined(WhenUncoordinated))
        {
            problems.Add("Leadership:WhenUncoordinated must be Follow or Lead.");
        }

        return problems;
    }
}
