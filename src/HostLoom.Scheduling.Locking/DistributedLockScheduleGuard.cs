using HostLoom.Locking;

namespace HostLoom.Scheduling.Locking;

/// <summary>
/// An <see cref="IScheduleGuard"/> over <see cref="IDistributedLock"/>: a claim is one
/// skip-if-busy acquisition of the key <c>schedule:{name}</c> for the schedule's lease, with
/// automatic extension so a run longer than the lease keeps its claim up to
/// <c>Locking:MaxHold</c>. A lost lease cancels the run through the claim's
/// <see cref="IScheduleClaim.LostToken"/>. Composes without a container:
/// <c>new DistributedLockScheduleGuard(distributedLock)</c>.
/// </summary>
public sealed class DistributedLockScheduleGuard(IDistributedLock distributedLock) : IScheduleGuard
{
    /// <summary>The prefix every claim key carries before the schedule name.</summary>
    public const string KeyPrefix = "schedule:";

    private readonly IDistributedLock _lock =
        distributedLock ?? throw new ArgumentNullException(nameof(distributedLock));

    /// <summary>
    /// Follows <see cref="IDistributedLock.IsCoordinated"/>. Over a disabled lock every claim is
    /// granted on every instance, which is the lock's single-instance mode; the scheduler warns
    /// once and the probe reports the guard as uncoordinated so that mode is never silent.
    /// </summary>
    public bool IsCoordinated => _lock.IsCoordinated;

    /// <summary>The lock key a schedule claims: <c>schedule:{name}</c>, before the lock's own namespace prefix.</summary>
    public static string KeyFor(string schedule)
    {
        ScheduleName.Validate(schedule);
        return KeyPrefix + schedule;
    }

    /// <inheritdoc />
    /// <exception cref="LockProviderUnavailableException">The lock provider failed; the scheduler skips the run.</exception>
    public async ValueTask<IScheduleClaim?> TryClaimAsync(
        string schedule,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(lease, TimeSpan.Zero);
        var handle = await _lock
            .TryAcquireAsync(
                KeyFor(schedule),
                new LockOptions
                {
                    Lease = lease,
                    MaxWait = TimeSpan.Zero,
                    AutoExtend = true,
                },
                cancellationToken
            )
            .ConfigureAwait(false);
        return handle is null ? null : new LockClaim(handle);
    }

    private sealed class LockClaim(ILockHandle handle) : IScheduleClaim
    {
        public bool IsHeld => handle.IsHeld;

        public CancellationToken LostToken => handle.LostToken;

        public ValueTask DisposeAsync() => handle.DisposeAsync();
    }
}
