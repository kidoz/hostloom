namespace HostLoom.Scheduling.DependencyInjection;

/// <summary>
/// Registration-time state shared by every <see cref="SchedulingBuilder"/> over one service
/// collection: the names taken so far, the schedules that need a guard, and the guard chosen.
/// </summary>
internal sealed class SchedulingRegistration
{
    /// <summary>Schedule names registered, so a repeat can be refused at registration time.</summary>
    public HashSet<string> Names { get; } = new(StringComparer.Ordinal);

    /// <summary>Names of the schedules registered as exclusive.</summary>
    public List<string> ExclusiveSchedules { get; } = [];

    /// <summary>The name given to <see cref="SchedulingBuilder.UseGuard{TGuard}(string)"/>, or <see langword="null"/>.</summary>
    public string? GuardName { get; set; }
}
