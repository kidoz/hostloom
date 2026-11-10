namespace HostLoom.DataGrid;

/// <summary>A deterministic grid time measured in UTC ticks since 0001-01-01.</summary>
/// <remarks>
/// A tick is 100 nanoseconds. The default value is the origin. Command application must use
/// committed timestamps, never sample a local clock. This type does not establish consensus.
/// </remarks>
public readonly record struct DataGridTimestamp
{
    /// <summary>Creates a timestamp in the range supported by <see cref="DateTimeOffset"/>.</summary>
    public DataGridTimestamp(long utcTicks)
    {
        ArgumentOutOfRangeException.ThrowIfNegative(utcTicks);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(utcTicks, DateTimeOffset.MaxValue.Ticks);
        UtcTicks = utcTicks;
    }

    public long UtcTicks { get; }

    /// <summary>Converts an explicit UTC instant without rounding.</summary>
    public static DataGridTimestamp FromUtc(DateTimeOffset value) => new(value.UtcTicks);

    /// <summary>
    /// Samples the leader's injected clock to propose a command timestamp. The result must be
    /// committed before application; sampling alone cannot advance the replicated state.
    /// </summary>
    public static DataGridTimestamp Sample(TimeProvider timeProvider)
    {
        ArgumentNullException.ThrowIfNull(timeProvider);
        return FromUtc(timeProvider.GetUtcNow());
    }

    /// <summary>Applies the replicated time rule: a stale or equal proposal cannot move time back.</summary>
    public DataGridTimestamp AdvanceTo(DataGridTimestamp proposed) =>
        proposed.UtcTicks > UtcTicks ? proposed : this;
}
