namespace HostLoom.Conformance;

/// <summary>
/// The clock a scenario advances. In-process backends run on a manual clock so expiry and leases
/// need no waiting; a real backend expires keys on its own clock, so the same scenario waits the
/// same span in wall-clock time.
/// </summary>
public abstract class ConformanceClock
{
    /// <summary>The provider handed to the kernels under test.</summary>
    public abstract TimeProvider Provider { get; }

    /// <summary>Moves time forward by <paramref name="delta"/> as the backend perceives it.</summary>
    public abstract Task AdvanceAsync(TimeSpan delta);
}

/// <summary>A deterministic clock: time moves only when a scenario advances it.</summary>
public sealed class ManualConformanceClock : ConformanceClock
{
    /// <summary>Creates the clock over a new or supplied manual provider.</summary>
    public ManualConformanceClock(ManualTimeProvider? provider = null) =>
        Manual = provider ?? new ManualTimeProvider();

    /// <summary>The underlying manual provider.</summary>
    public ManualTimeProvider Manual { get; }

    /// <inheritdoc />
    public override TimeProvider Provider => Manual;

    /// <inheritdoc />
    public override Task AdvanceAsync(TimeSpan delta)
    {
        Manual.Advance(delta);
        return Task.CompletedTask;
    }
}

/// <summary>
/// The system clock: advancing waits the span plus a small grace so timers and server-side
/// expiries have fired by the time the scenario looks.
/// </summary>
public sealed class RealConformanceClock(TimeSpan? grace = null) : ConformanceClock
{
    private readonly TimeSpan _grace = grace ?? TimeSpan.FromMilliseconds(250);

    /// <inheritdoc />
    public override TimeProvider Provider => TimeProvider.System;

    /// <inheritdoc />
    public override Task AdvanceAsync(TimeSpan delta) => Task.Delay(delta + _grace);
}
