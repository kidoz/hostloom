using System.Collections.Concurrent;
using System.Diagnostics;
using System.Diagnostics.Metrics;

namespace HostLoom.Leadership;

/// <summary>Meter and activity source for <c>HostLoom.Leadership</c>. The role travels as a tag.</summary>
public static class LeadershipDiagnostics
{
    /// <summary>Activity source name to enable when configuring OpenTelemetry tracing.</summary>
    public const string ActivitySourceName = "HostLoom.Leadership";

    /// <summary>Meter name to enable when configuring OpenTelemetry metrics.</summary>
    public const string MeterName = "HostLoom.Leadership";

    /// <summary>Tag carrying the role on every instrument.</summary>
    public const string RoleTag = "hostloom.leader.role";

    /// <summary>Tag on <c>hostloom.leader.changes</c>: the lower-case <see cref="LeadershipChangeReason"/>.</summary>
    public const string ReasonTag = "hostloom.leader.reason";

    /// <summary>Tag on <c>hostloom.leader.renew.duration</c>: <c>renewed</c>, <c>refused</c>, or <c>failed</c>.</summary>
    public const string OutcomeTag = "hostloom.leader.outcome";

    internal static readonly ActivitySource ActivitySource = new(ActivitySourceName);

    private static readonly Meter Meter = new(MeterName);

    private static readonly ConcurrentDictionary<LeaderElector, byte> Electors = new();

    internal static readonly Counter<long> Changes = Meter.CreateCounter<long>(
        "hostloom.leader.changes",
        "{change}",
        "Leadership transitions, by reason."
    );

    internal static readonly Histogram<double> RenewDuration = Meter.CreateHistogram<double>(
        "hostloom.leader.renew.duration",
        "s",
        "Time a lease renewal took, by outcome."
    );

    // Declared after Meter and Electors on purpose: static initialisers run in textual order.
    private static readonly ObservableGauge<int> IsLeader = Meter.CreateObservableGauge(
        "hostloom.leader.is_leader",
        ObserveLeaders,
        "{state}",
        "1 while this instance leads the role, 0 otherwise."
    );

    internal static void Register(LeaderElector elector) => Electors.TryAdd(elector, 0);

    internal static void Unregister(LeaderElector elector) => Electors.TryRemove(elector, out _);

    internal static string ReasonName(LeadershipChangeReason reason) =>
        reason switch
        {
            LeadershipChangeReason.Acquired => "acquired",
            LeadershipChangeReason.Lost => "lost",
            LeadershipChangeReason.Resigned => "resigned",
            _ => "stopped",
        };

    private static IEnumerable<Measurement<int>> ObserveLeaders()
    {
        _ = IsLeader;
        foreach (var elector in Electors.Keys)
        {
            yield return new Measurement<int>(
                elector.IsLeader ? 1 : 0,
                new KeyValuePair<string, object?>(RoleTag, elector.Role)
            );
        }
    }
}
