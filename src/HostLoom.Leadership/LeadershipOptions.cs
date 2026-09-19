namespace HostLoom.Leadership;

/// <summary>
/// How one role is elected. <see cref="Validate"/> reports every violation with the option key it
/// names, so a container-free composition fails the same way a hosted one does.
/// </summary>
public sealed class LeadershipOptions
{
    /// <summary>
    /// How long the lease lasts without a renewal, and so how long a crashed leader keeps the role.
    /// Keep it within <c>Locking:MaxLease</c>; the lock caps a longer lease silently.
    /// </summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>How often the leader renews. Must leave at least one more renewal inside the lease.</summary>
    public TimeSpan RenewInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long a candidate waits between attempts, and a resigned leader before it runs again.</summary>
    public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Uniform additive jitter on <see cref="RetryInterval"/>, so candidates do not attempt in lockstep.</summary>
    public TimeSpan RetryJitter { get; set; } = TimeSpan.FromSeconds(1);

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

        return problems;
    }
}
