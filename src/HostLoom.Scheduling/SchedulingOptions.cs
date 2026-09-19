namespace HostLoom.Scheduling;

/// <summary>
/// Defaults for every schedule in one scheduler. <see cref="Validate"/> reports every violation
/// with the option key it names, so a container-free composition fails the same way a hosted one
/// does.
/// </summary>
public sealed class SchedulingOptions
{
    /// <summary>
    /// <see langword="false"/> runs nothing: a startup warning, the probe reporting
    /// <c>(disabled)</c>, and every schedule idle. Use it to keep a deployment from running jobs
    /// without removing their registrations.
    /// </summary>
    public bool Enabled { get; set; } = true;

    /// <summary>How long an exclusive claim is held when a schedule gives no lease.</summary>
    public TimeSpan DefaultLease { get; set; } = TimeSpan.FromMinutes(5);

    /// <summary>Every violation, each naming the option key at fault. Empty when the options are usable.</summary>
    public IReadOnlyList<string> Validate()
    {
        List<string> problems = [];
        if (DefaultLease <= TimeSpan.Zero)
        {
            problems.Add("Scheduling:DefaultLease must be positive.");
        }

        return problems;
    }
}
