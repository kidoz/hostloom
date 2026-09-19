namespace HostLoom.Scheduling;

/// <summary>
/// One schedule: a name, the trigger deciding when it runs, the work to run, and its options.
/// Definitions are immutable; a <see cref="Scheduler"/> takes a set of them.
/// </summary>
public sealed class ScheduleDefinition
{
    /// <summary>Creates the definition.</summary>
    /// <param name="name">The schedule name; see <see cref="ScheduleName.Validate"/>.</param>
    /// <param name="trigger">When the schedule runs.</param>
    /// <param name="run">The work. It receives the run and a token that is cancelled on stop, timeout, or claim loss.</param>
    /// <param name="options">Per-schedule settings; defaults apply when omitted.</param>
    /// <exception cref="ArgumentException">The name or the options are not acceptable.</exception>
    public ScheduleDefinition(
        string name,
        ScheduleTrigger trigger,
        Func<ScheduledRun, CancellationToken, ValueTask> run,
        ScheduleOptions? options = null
    )
    {
        ScheduleName.Validate(name);
        ArgumentNullException.ThrowIfNull(trigger);
        ArgumentNullException.ThrowIfNull(run);
        options ??= new ScheduleOptions();
        var problems = options.Validate(name);
        if (problems.Count > 0)
        {
            throw new ArgumentException(string.Join(" ", problems), nameof(options));
        }

        Name = name;
        Trigger = trigger;
        Run = run;
        Options = options;
    }

    /// <summary>The schedule name, unique within a scheduler.</summary>
    public string Name { get; }

    /// <summary>When the schedule runs.</summary>
    public ScheduleTrigger Trigger { get; }

    /// <summary>The work each run executes.</summary>
    public Func<ScheduledRun, CancellationToken, ValueTask> Run { get; }

    /// <summary>Per-schedule settings.</summary>
    public ScheduleOptions Options { get; }
}
