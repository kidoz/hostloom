namespace HostLoom.DataGrid;

/// <summary>An immutable deadline in committed grid time. The default value never expires.</summary>
/// <remarks>
/// Expiration is logical absence at or after the deadline, independent of physical reclamation.
/// A replacement creates a new deadline. Replays must retain the original deadline rather than
/// recompute it from a later timestamp. No sliding renewal or wall-clock access is provided.
/// </remarks>
public readonly record struct DataGridExpiration
{
    private DataGridExpiration(DataGridTimestamp deadline) => Deadline = deadline;

    /// <summary>The absolute grid deadline, or null for no expiration.</summary>
    public DataGridTimestamp? Deadline { get; }

    public static DataGridExpiration Never => default;

    /// <summary>Creates an absolute deadline, including an already-due deadline when restoring state.</summary>
    public static DataGridExpiration At(DataGridTimestamp deadline) => new(deadline);

    /// <summary>
    /// Converts a positive TTL to a deadline from the command's effective committed time without
    /// rounding. An unrepresentable deadline is rejected, never wrapped or clamped.
    /// </summary>
    public static DataGridExpiration After(DataGridTimestamp effectiveTime, TimeSpan timeToLive)
    {
        ArgumentOutOfRangeException.ThrowIfLessThanOrEqual(timeToLive, TimeSpan.Zero);
        if (timeToLive.Ticks > DateTimeOffset.MaxValue.Ticks - effectiveTime.UtcTicks)
            throw new ArgumentOutOfRangeException(nameof(timeToLive));
        return At(new DataGridTimestamp(effectiveTime.UtcTicks + timeToLive.Ticks));
    }

    public bool IsExpired(DataGridTimestamp effectiveTime) =>
        Deadline is { } deadline && deadline.UtcTicks <= effectiveTime.UtcTicks;

    /// <summary>
    /// Returns exact positive remaining grid lifetime, zero when due, or null for no expiration.
    /// A map read must omit an expired entry rather than return its payload with a zero TTL.
    /// </summary>
    public TimeSpan? GetRemainingTimeToLive(DataGridTimestamp effectiveTime) =>
        Deadline is { } deadline
            ? TimeSpan.FromTicks(Math.Max(0, deadline.UtcTicks - effectiveTime.UtcTicks))
            : null;
}
