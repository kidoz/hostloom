namespace HostLoom.NodeLeasing;

/// <summary>
/// How one pool's IDs are leased. <see cref="Validate"/> reports every violation with the option
/// key it names, so a container-free composition fails the same way a hosted one does.
/// </summary>
public sealed class NodeLeasingOptions
{
    /// <summary>The smallest pool: one ID.</summary>
    public const int MinPoolSize = 1;

    /// <summary>The largest pool: IDs zero to 65534, so a generated ID always fits a Snowflake-style worker field.</summary>
    public const int MaxPoolSize = 65535;

    /// <summary>
    /// The longest wait the elector can schedule on its clock: <c>uint.MaxValue - 1</c>
    /// milliseconds, the bound <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/>
    /// accepts. <see cref="Validate"/> keeps every cadence within it so the loop can never fail on
    /// an out-of-range delay.
    /// </summary>
    public static readonly TimeSpan MaxDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);

    /// <summary>
    /// How many IDs the pool hands out: zero to <see cref="PoolSize"/> minus one. Must be
    /// between <see cref="MinPoolSize"/> and <see cref="MaxPoolSize"/>. Every instance of the
    /// deployment configures the same size; a smaller pool on one instance overlaps, never
    /// partitions, the larger one.
    /// </summary>
    public int PoolSize { get; set; } = 1024;

    /// <summary>
    /// How long the lease on an ID lasts without a renewal, and so how long a crashed holder
    /// keeps the ID. Must be at most <c>Locking:MaxLease</c>: a hosted elector fails at startup
    /// otherwise, while a container-free one is handed the lock's cap as its lease.
    /// </summary>
    public TimeSpan Lease { get; set; } = TimeSpan.FromSeconds(15);

    /// <summary>How often the holder renews. Must leave at least one more renewal inside the lease.</summary>
    public TimeSpan RenewInterval { get; set; } = TimeSpan.FromSeconds(5);

    /// <summary>How long a candidate waits between scans, and a holder that released or lost its ID before it scans again.</summary>
    public TimeSpan RetryInterval { get; set; } = TimeSpan.FromSeconds(2);

    /// <summary>Uniform additive jitter on <see cref="RetryInterval"/>, so candidates do not scan in lockstep.</summary>
    public TimeSpan RetryJitter { get; set; } = TimeSpan.FromSeconds(1);

    /// <summary>
    /// What the elector does when the lock does not coordinate across instances
    /// (<c>Locking:Enabled = false</c>). <see cref="UncoordinatedNodeAssignment.None"/>, the
    /// default, never assigns an ID, so replicas over a disabled lock cannot all take one at
    /// once; <see cref="UncoordinatedNodeAssignment.AssignZero"/> assigns ID zero without
    /// coordination for a single-instance or development deployment. Either way the condition is
    /// logged once per pool and reported by <see cref="NodeLeasingProbe"/> and
    /// <see cref="INodeLeasing.IsCoordinated"/>.
    /// </summary>
    public UncoordinatedNodeAssignment WhenUncoordinated { get; set; } =
        UncoordinatedNodeAssignment.None;

    /// <summary>Every violation, each naming the option key at fault. Empty when the options are usable.</summary>
    public IReadOnlyList<string> Validate()
    {
        List<string> problems = [];
        if (PoolSize is < MinPoolSize or > MaxPoolSize)
        {
            problems.Add($"NodeLeasing:PoolSize must be between {MinPoolSize} and {MaxPoolSize}.");
        }

        if (Lease <= TimeSpan.Zero)
        {
            problems.Add("NodeLeasing:Lease must be positive.");
        }

        if (RenewInterval <= TimeSpan.Zero)
        {
            problems.Add("NodeLeasing:RenewInterval must be positive.");
        }
        else if (RenewInterval > MaxDelay)
        {
            problems.Add(
                $"NodeLeasing:RenewInterval must be at most {MaxDelay}, the longest wait the elector can schedule."
            );
        }
        else if (RenewInterval * 2 > Lease)
        {
            problems.Add(
                "NodeLeasing:RenewInterval must be at most half of NodeLeasing:Lease, so a missed renewal still leaves time for another."
            );
        }

        if (RetryInterval <= TimeSpan.Zero)
        {
            problems.Add("NodeLeasing:RetryInterval must be positive.");
        }

        if (RetryJitter < TimeSpan.Zero)
        {
            problems.Add("NodeLeasing:RetryJitter must not be negative.");
        }
        else if (
            RetryInterval > TimeSpan.Zero
            && (RetryInterval > MaxDelay || RetryJitter > MaxDelay - RetryInterval)
        )
        {
            // Compared as a difference rather than a sum, so two large values cannot overflow.
            problems.Add(
                $"NodeLeasing:RetryInterval plus NodeLeasing:RetryJitter must be at most {MaxDelay}, the longest wait the elector can schedule."
            );
        }

        if (!Enum.IsDefined(WhenUncoordinated))
        {
            problems.Add("NodeLeasing:WhenUncoordinated must be None or AssignZero.");
        }

        return problems;
    }
}
