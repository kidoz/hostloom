namespace HostLoom.AspNetCore.WebSockets;

internal static class TimerLimits
{
    /// <summary>
    /// The longest single delay <see cref="Task.Delay(TimeSpan, TimeProvider, CancellationToken)"/>
    /// and <see cref="CancellationTokenSource.CancelAfter(TimeSpan)"/> accept, about 49.7 days.
    /// Longer waits must be chunked.
    /// </summary>
    public static readonly TimeSpan MaximumDelay = TimeSpan.FromMilliseconds(uint.MaxValue - 1);
}
