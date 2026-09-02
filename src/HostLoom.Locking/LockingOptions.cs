namespace HostLoom.Locking;

/// <summary>
/// Defaults for every lock in one namespace. Durations are <see cref="TimeSpan"/>; per-call
/// overrides come only through <see cref="LockOptions"/>. <see cref="Validate"/> reports every
/// violation with the option key it names, so a container-free composition fails the same way a
/// hosted one does.
/// </summary>
public sealed class LockingOptions
{
    /// <summary>Required. Prefixes every provider key as <c>{namespace}:lock:{key}</c>; must match <c>[a-z0-9-]+</c>.</summary>
    public string Namespace { get; set; } = "";

    /// <summary>
    /// <see langword="false"/> is single-instance mode: a startup warning, the probe reporting
    /// <c>(disabled)</c>, the gauge <c>hostloom.lock.enabled = 0</c>, and every action running
    /// immediately without a provider.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>Lease length when a call gives none.</summary>
    public TimeSpan DefaultLease { get; set; } = TimeSpan.FromSeconds(30);

    /// <summary>Upper bound on any lease, including extensions.</summary>
    public TimeSpan MaxLease { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>How long automatic extension keeps a lease alive before letting it expire.</summary>
    public TimeSpan MaxHold { get; set; } = TimeSpan.FromMinutes(10);

    /// <summary>
    /// Retry shape between acquisition attempts. The default reproduces the platform's behaviour:
    /// ten retries at a linear 50 ms step with up to 50 ms of additive jitter, about 3 s in total.
    /// </summary>
    public LockRetryPolicy Retry { get; set; } =
        LockRetryPolicy
            .Linear(retryLimit: 10, step: TimeSpan.FromMilliseconds(50))
            .WithJitter(TimeSpan.FromMilliseconds(50));

    /// <summary>Heartbeat every lease at half its length until <see cref="MaxHold"/>.</summary>
    public bool AutoExtend { get; set; }

    /// <summary>
    /// Throw <see cref="LockReentrancyException"/> when an action started through
    /// <see cref="IDistributedLock.ExecuteWithLockAsync{T}"/> acquires the key it already holds,
    /// instead of waiting out the lease.
    /// </summary>
    public bool DetectReentrancy { get; set; } = true;

    /// <summary>Longest consumer key accepted, before the namespace prefix.</summary>
    public int MaxKeyLength { get; set; } = 512;

    /// <summary>
    /// Every violation, each naming the option key at fault. Empty when the options are usable.
    /// </summary>
    public IReadOnlyList<string> Validate()
    {
        List<string> problems = [];
        if (!LockKey.IsValidNamespace(Namespace))
        {
            problems.Add(
                "Locking:Namespace is required and must match [a-z0-9-]+ so provider keys stay "
                    + "unambiguous across services."
            );
        }

        if (DefaultLease <= TimeSpan.Zero)
        {
            problems.Add("Locking:DefaultLease must be positive.");
        }

        if (MaxLease < DefaultLease)
        {
            problems.Add("Locking:MaxLease must be at least Locking:DefaultLease.");
        }

        if (MaxHold <= TimeSpan.Zero)
        {
            problems.Add("Locking:MaxHold must be positive.");
        }

        if (Retry is null)
        {
            problems.Add(
                "Locking:Retry is required; use LockRetryPolicy.Immediate(0) to disable retries."
            );
        }

        if (MaxKeyLength <= 0)
        {
            problems.Add("Locking:MaxKeyLength must be positive.");
        }

        return problems;
    }
}
