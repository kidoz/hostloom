using System.Collections.ObjectModel;

namespace HostLoom.Diagnostics;

/// <summary>
/// An immutable view of everything the ledger held when it was taken, plus the components whose
/// recorded choices disagree. Both collections are copied and wrapped on construction, so the
/// decisions and the conflicts computed from them cannot drift apart in a caller's hands. Public
/// and resolvable so a test can assert on the composition directly, instead of capturing log
/// output to observe what registration decided.
/// </summary>
public sealed record CompositionReport(
    IReadOnlyList<CompositionDecision> Decisions,
    IReadOnlyList<CompositionConflict> Conflicts
)
{
    /// <summary>The decisions in the order they were recorded.</summary>
    public IReadOnlyList<CompositionDecision> Decisions { get; } =
        new ReadOnlyCollection<CompositionDecision>([.. Decisions]);

    /// <summary>The components recorded with choices that disagree.</summary>
    public IReadOnlyList<CompositionConflict> Conflicts { get; } =
        new ReadOnlyCollection<CompositionConflict>([.. Conflicts]);

    /// <summary>
    /// The whole composition on one greppable line, in the order the decisions were recorded:
    /// <c>Transport=Kafka | Outbox=(skipped) | Scheduler=Quartz</c>. Every decision appears, so a
    /// component recorded twice shows both choices rather than a silently collapsed one.
    /// </summary>
    public string Describe() =>
        string.Join(" | ", Decisions.Select(decision => $"{decision.Component}={decision.Choice}"));

    // Both members are collections, so synthesized record equality would degrade to reference
    // equality and two reports describing the same composition would never be equal — which
    // would quietly defeat asserting on a report in a test.
    public bool Equals(CompositionReport? other) =>
        other is not null
        && Decisions.SequenceEqual(other.Decisions)
        && Conflicts.SequenceEqual(other.Conflicts);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        foreach (var decision in Decisions)
        {
            hash.Add(decision);
        }

        foreach (var conflict in Conflicts)
        {
            hash.Add(conflict);
        }

        return hash.ToHashCode();
    }
}
