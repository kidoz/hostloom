namespace HostLoom.Locking;

/// <summary>
/// The end of a lease the backend may have started, counted from the moment the request was
/// sent. A provider starts the lease when it accepts the request, so the round trip is spent
/// lease: counting from the request, not the reply, is what keeps a local deadline from
/// outliving the backend's key. Every remaining-lease computation in the kernel goes through here.
/// </summary>
/// <param name="RequestedAt">
/// The timestamp taken before the provider call, the earliest instant the backend can have
/// started the lease.
/// </param>
/// <param name="Lease">The lease the provider was asked for.</param>
internal readonly record struct LeaseDeadline(long RequestedAt, TimeSpan Lease)
{
    /// <summary>A deadline for a request about to be sent now.</summary>
    public static LeaseDeadline StartingNow(TimeProvider clock, TimeSpan lease) =>
        new(clock.GetTimestamp(), lease);

    /// <summary>What is left of the lease; zero or negative once it has run out.</summary>
    public TimeSpan Remaining(TimeProvider clock) => Lease - clock.GetElapsedTime(RequestedAt);

    /// <summary>Whether the lease has run out, so a grant for it can no longer be used.</summary>
    public bool IsSpent(TimeProvider clock) => Remaining(clock) <= TimeSpan.Zero;
}
