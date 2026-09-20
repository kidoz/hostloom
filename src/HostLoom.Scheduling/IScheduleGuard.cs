namespace HostLoom.Scheduling;

/// <summary>
/// Decides which instance runs an exclusive schedule. A guard never waits: a claim is granted or
/// refused at once, and a refused claim skips the run. A backend failure is thrown and the run is
/// skipped as <see cref="ScheduleRunOutcome.GuardFailed"/>, because a job that must not run twice
/// cannot run when nothing can say whether another instance already does.
/// </summary>
public interface IScheduleGuard
{
    /// <summary>
    /// <see langword="true"/> while a claim excludes other instances. <see langword="false"/>
    /// when the guard rests on a lock or a leadership that does not coordinate
    /// (<c>Locking:Enabled = false</c>): a claim then says nothing about other instances, the
    /// scheduler warns once at construction, and <see cref="SchedulingProbe"/> reports the guard
    /// as uncoordinated.
    /// </summary>
    bool IsCoordinated { get; }

    /// <summary>
    /// Claims <paramref name="schedule"/> for <paramref name="lease"/>, or returns
    /// <see langword="null"/> when another instance holds the claim.
    /// </summary>
    ValueTask<IScheduleClaim?> TryClaimAsync(
        string schedule,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    );
}

/// <summary>One granted claim. Disposing releases it.</summary>
public interface IScheduleClaim : IAsyncDisposable
{
    /// <summary><see langword="true"/> while the claim is believed held.</summary>
    bool IsHeld { get; }

    /// <summary>
    /// Cancelled when the claim is lost before release, so the run stops when exclusivity is no
    /// longer guaranteed. Never cancelled by a normal release.
    /// </summary>
    CancellationToken LostToken { get; }
}
