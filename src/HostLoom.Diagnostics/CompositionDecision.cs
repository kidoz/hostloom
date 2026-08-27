using System.Collections.ObjectModel;

namespace HostLoom.Diagnostics;

/// <summary>
/// One choice made while the service collection was being built: which implementation a component
/// resolved to, or that it was deliberately left out. Registration produces a plan and executes
/// nothing, so there is no logger, no bound options, and no filter configuration at the moment the
/// choice is made — this is the record of that choice, reported once the container exists.
/// </summary>
/// <param name="Component">
/// What was decided, for example <c>Transport</c> or <c>Pipeline:document-indexing</c>. Recording
/// the same component twice with different choices is reported as a conflict.
/// </param>
/// <param name="Choice">What it resolved to, or <see cref="Skipped"/>.</param>
/// <param name="Reason">
/// Why, ideally naming the configuration key that decided it. A reason that repeats the choice
/// adds nothing; a reason that names its input turns the log line into an explanation.
/// </param>
/// <param name="Origin">
/// The registration method that recorded the decision, captured automatically from the call site.
/// </param>
public sealed record CompositionDecision(
    string Component,
    string Choice,
    string? Reason = null,
    string? Origin = null
)
{
    /// <summary>
    /// The choice recorded for a component left out on purpose. A log that only reports what was
    /// registered cannot answer "what is missing", because the absent branch writes nothing; this
    /// makes the road not taken as visible as the one that was.
    /// </summary>
    public const string Skipped = "(skipped)";

    /// <summary>Whether this entry marks an absence rather than a registration.</summary>
    public bool IsSkipped => string.Equals(Choice, Skipped, StringComparison.Ordinal);
}

/// <summary>
/// A component recorded more than once with choices that disagree. The ledger cannot know which
/// registration the container ultimately resolves, so it reports the disagreement rather than
/// naming a winner it would have to guess.
/// </summary>
public sealed record CompositionConflict(string Component, IReadOnlyList<string> Choices)
{
    /// <summary>The distinct choices recorded for the component, in the order they were recorded.</summary>
    public IReadOnlyList<string> Choices { get; } = new ReadOnlyCollection<string>([.. Choices]);

    // Synthesized record equality would compare the collection by reference, so two conflicts
    // describing the same disagreement would not be equal. A record that advertises value equality
    // has to deliver it over its contents.
    public bool Equals(CompositionConflict? other) =>
        other is not null
        && string.Equals(Component, other.Component, StringComparison.Ordinal)
        && Choices.SequenceEqual(other.Choices, StringComparer.Ordinal);

    public override int GetHashCode()
    {
        var hash = new HashCode();
        hash.Add(Component, StringComparer.Ordinal);
        foreach (var choice in Choices)
        {
            hash.Add(choice, StringComparer.Ordinal);
        }

        return hash.ToHashCode();
    }
}
