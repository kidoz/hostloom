using System.Globalization;

namespace HostLoom.Scheduling;

/// <summary>
/// Execution-free description of a scheduler, in the spirit of <c>HostLoomProbe</c>. Safe to
/// call from a health or debug endpoint on every request; it reads state and runs nothing.
/// </summary>
public static class SchedulingProbe
{
    /// <summary>
    /// Describes <paramref name="scheduler"/>: whether it is enabled, its guard, and one entry
    /// per schedule with the trigger, exclusivity, lease, timeout, next due time, and last
    /// outcome. Each line names the option that decided it.
    /// </summary>
    public static SchedulingDescription Describe(Scheduler scheduler)
    {
        ArgumentNullException.ThrowIfNull(scheduler);
        var invariant = CultureInfo.InvariantCulture;
        var guard = scheduler.Guard is { } g ? g.GetType().Name : "(none)";
        List<string> lines =
        [
            scheduler.Enabled
                ? "Scheduling:Enabled = true"
                : "Scheduling:Enabled = false: (disabled), no schedule runs in this process",
            $"Guard = {guard}",
            string.Create(invariant, $"Scheduling:DefaultLease = {scheduler.Options.DefaultLease}"),
        ];

        var schedules = new List<ScheduleDescription>(scheduler.Schedules.Count);
        foreach (var definition in scheduler.Schedules)
        {
            var state = scheduler.GetState(definition.Name);
            var options = definition.Options;
            var lease = options.Exclusive
                ? options.Lease ?? scheduler.Options.DefaultLease
                : (TimeSpan?)null;
            schedules.Add(
                new ScheduleDescription(
                    definition.Name,
                    definition.Trigger.Description,
                    options.Exclusive,
                    lease,
                    options.Timeout,
                    state.NextDue,
                    state.LastOutcome
                )
            );
            lines.Add(
                string.Create(
                    invariant,
                    $"{definition.Name}: {definition.Trigger.Description}; exclusive = {options.Exclusive}"
                        + $"{(lease is { } l ? $", lease {l}" : "")}"
                        + $"; timeout = {(options.Timeout is { } t ? t.ToString() : "none")}"
                        + $"; next due = {(state.NextDue is { } due ? due.ToString("O", invariant) : state.Running ? "(running)" : "(not scheduled)")}"
                        + $"; last outcome = {(state.LastOutcome is { } outcome ? SchedulingDiagnostics.OutcomeName(outcome) : "(none)")}"
                        + $"{(state.ClaimHeldUntil is { } held ? $"; claim held until {held.ToString("O", invariant)}" : "")}"
                )
            );
        }

        return new SchedulingDescription(scheduler.Enabled, guard, schedules, lines);
    }
}

/// <summary>What <see cref="SchedulingProbe.Describe"/> reports.</summary>
/// <param name="Enabled"><see cref="SchedulingOptions.Enabled"/>.</param>
/// <param name="Guard">The guard type name, or <c>(none)</c>.</param>
/// <param name="Schedules">One entry per schedule, in registration order.</param>
/// <param name="Lines">Human-readable lines, each naming the option that decided it.</param>
public sealed record SchedulingDescription(
    bool Enabled,
    string Guard,
    IReadOnlyList<ScheduleDescription> Schedules,
    IReadOnlyList<string> Lines
);

/// <summary>One schedule as the probe reports it.</summary>
/// <param name="Name">The schedule name.</param>
/// <param name="Trigger"><see cref="ScheduleTrigger.Description"/>.</param>
/// <param name="Exclusive"><see cref="ScheduleOptions.Exclusive"/>.</param>
/// <param name="Lease">The effective claim lease, or <see langword="null"/> when not exclusive.</param>
/// <param name="Timeout"><see cref="ScheduleOptions.Timeout"/>.</param>
/// <param name="NextDue">When the next run is due, if known.</param>
/// <param name="LastOutcome">How the most recent run ended, if any.</param>
public sealed record ScheduleDescription(
    string Name,
    string Trigger,
    bool Exclusive,
    TimeSpan? Lease,
    TimeSpan? Timeout,
    DateTimeOffset? NextDue,
    ScheduleRunOutcome? LastOutcome
);
