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

    /// <summary>
    /// Tag on <c>hostloom.leader.renew.duration</c>: <c>renewed</c>; <c>refused</c> when the
    /// lease is gone, which ends leadership; or <c>failed</c> when the renewal failed while the
    /// lease still runs, which is retried.
    /// </summary>
    public const string OutcomeTag = "hostloom.leader.outcome";

    /// <summary>Tag on <c>hostloom.leader.channel.dropped</c>: the <see cref="LeaderChannel{T}"/> name.</summary>
    public const string ChannelTag = "hostloom.leader.channel";

    /// <summary>Tag on <c>hostloom.leader.channel.dropped</c>: <see cref="FollowerDrop"/> or <see cref="FullDrop"/>.</summary>
    public const string DropReasonTag = "hostloom.leader.channel.reason";

    /// <summary><see cref="DropReasonTag"/> value: the item was written while this instance was not leading.</summary>
    public const string FollowerDrop = "follower";

    /// <summary><see cref="DropReasonTag"/> value: the channel was full and its full mode dropped an item.</summary>
    public const string FullDrop = "full";

    /// <summary><see cref="DropReasonTag"/> value: the item was buffered when leadership ended and <see cref="LeaderChannelOptions.DrainOnLoss"/> discarded it.</summary>
    public const string LossDrop = "loss";

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

    internal static readonly Counter<long> ChannelDropped = Meter.CreateCounter<long>(
        "hostloom.leader.channel.dropped",
        "{item}",
        "Items a leader channel discarded, by reason."
    );

    internal static readonly Counter<long> LoopFaults = Meter.CreateCounter<long>(
        "hostloom.leader.loop.faults",
        "{fault}",
        "Unexpected exceptions the elector loop caught and backed off from."
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
