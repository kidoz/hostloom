using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.Scheduling;

/// <summary>
/// Meter and activity source for <c>HostLoom.Scheduling</c>. The schedule name travels as a
/// tag, never in an instrument name.
/// </summary>
public static class SchedulingDiagnostics
{
    /// <summary>Activity source name to enable when configuring OpenTelemetry tracing.</summary>
    public const string ActivitySourceName = "HostLoom.Scheduling";

    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.Scheduling";

    /// <summary>Tag carrying the schedule name on every instrument and activity.</summary>
    public const string NameTag = "hostloom.schedule.name";

    /// <summary>Tag on <c>hostloom.schedule.run.duration</c>: the lower-case <see cref="ScheduleRunOutcome"/>.</summary>
    public const string OutcomeTag = "hostloom.schedule.outcome";

    /// <summary>Tag on <c>hostloom.schedule.skipped</c>: <c>claimed_elsewhere</c> or <c>guard_failed</c>.</summary>
    public const string ReasonTag = "hostloom.schedule.skip_reason";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    internal static readonly Histogram<double> RunDuration = Meter.CreateHistogram<double>(
        "hostloom.schedule.run.duration",
        "s",
        "Time a scheduled run executed, by outcome.",
        tags: null,
        advice: new InstrumentAdvice<double>
        {
            // Spans and lags: milliseconds up to a day.
            HistogramBucketBoundaries =
            [
                0.001,
                0.005,
                0.01,
                0.05,
                0.1,
                0.5,
                1,
                5,
                10,
                30,
                60,
                300,
                900,
                1800,
                3600,
                7200,
                21600,
                86400,
            ],
        }
    );

    internal static readonly Histogram<double> Lag = Meter.CreateHistogram<double>(
        "hostloom.schedule.lag",
        "s",
        "Time between a run's due time and its start.",
        tags: null,
        advice: new InstrumentAdvice<double>
        {
            // Spans and lags: milliseconds up to a day.
            HistogramBucketBoundaries =
            [
                0.001,
                0.005,
                0.01,
                0.05,
                0.1,
                0.5,
                1,
                5,
                10,
                30,
                60,
                300,
                900,
                1800,
                3600,
                7200,
                21600,
                86400,
            ],
        }
    );

    internal static readonly Counter<long> Skipped = Meter.CreateCounter<long>(
        "hostloom.schedule.skipped",
        "{run}",
        "Runs that did not execute, by reason."
    );

    internal static string OutcomeName(ScheduleRunOutcome outcome) =>
        outcome switch
        {
            ScheduleRunOutcome.Succeeded => "succeeded",
            ScheduleRunOutcome.Failed => "failed",
            ScheduleRunOutcome.TimedOut => "timed_out",
            ScheduleRunOutcome.Canceled => "canceled",
            ScheduleRunOutcome.ClaimLost => "claim_lost",
            ScheduleRunOutcome.Skipped => "skipped",
            _ => "guard_failed",
        };
}
