using HostLoom.Leadership;

namespace HostLoom.Scheduling.Leadership;

/// <summary>
/// An <see cref="IScheduleGuard"/> that grants a claim while this instance leads the role and
/// refuses it otherwise, so an exclusive schedule runs on the leader without a backend round trip
/// per occurrence. The claim's lost token is the leadership token: a run in progress is cancelled
/// when leadership ends. Followers record every occurrence as skipped. Composes without a
/// container: <c>new LeaderScheduleGuard(leadership)</c>.
/// </summary>
public sealed class LeaderScheduleGuard(ILeadership leadership) : IScheduleGuard
{
    private readonly ILeadership _leadership =
        leadership ?? throw new ArgumentNullException(nameof(leadership));

    /// <summary>The role the guard follows.</summary>
    public string Role => _leadership.Role;

    /// <summary>
    /// Follows <see cref="ILeadership.IsCoordinated"/>. Over a disabled lock, whether a claim is
    /// granted follows <c>Leadership:WhenUncoordinated</c>: <c>Follow</c> refuses every claim on
    /// every instance, <c>Lead</c> grants every claim on every instance.
    /// </summary>
    public bool IsCoordinated => _leadership.IsCoordinated;

    /// <inheritdoc />
    public ValueTask<IScheduleClaim?> TryClaimAsync(
        string schedule,
        TimeSpan lease,
        CancellationToken cancellationToken = default
    )
    {
        cancellationToken.ThrowIfCancellationRequested();
        var token = _leadership.LeadershipToken;
        // CA2000: ownership of the claim transfers to the scheduler, which disposes it.
#pragma warning disable CA2000
        return ValueTask.FromResult<IScheduleClaim?>(
            token.IsCancellationRequested ? null : new LeaderClaim(token)
        );
#pragma warning restore CA2000
    }

    /// <summary>A claim that lasts as long as the leadership it was granted under. Disposing releases nothing.</summary>
    private sealed class LeaderClaim(CancellationToken leadership) : IScheduleClaim
    {
        public bool IsHeld => !leadership.IsCancellationRequested;

        public CancellationToken LostToken => leadership;

        public ValueTask DisposeAsync() => ValueTask.CompletedTask;
    }
}
