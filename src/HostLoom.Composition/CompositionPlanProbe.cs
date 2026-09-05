using System.Collections.ObjectModel;

namespace HostLoom.Composition;

/// <summary>A candidate deliberately excluded by an authored rule.</summary>
public sealed class CompositionCandidateRejection
{
    /// <summary>Creates a rejection and defensively copies its ordered reasons.</summary>
    public CompositionCandidateRejection(
        Type candidateType,
        CompositionOrigin origin,
        IEnumerable<string> reasons
    )
    {
        ArgumentNullException.ThrowIfNull(candidateType);
        ArgumentNullException.ThrowIfNull(origin);
        ArgumentNullException.ThrowIfNull(reasons);
        var copy = reasons.ToArray();
        if (copy.Length == 0 || copy.Any(string.IsNullOrWhiteSpace))
        {
            throw new ArgumentException(
                "A rejected candidate requires nonempty reasons.",
                nameof(reasons)
            );
        }

        CandidateType = candidateType;
        Origin = origin;
        Reasons = new ReadOnlyCollection<string>(copy);
    }

    /// <summary>The excluded candidate, without member inspection.</summary>
    public Type CandidateType { get; }

    /// <summary>The rule that excluded the candidate.</summary>
    public CompositionOrigin Origin { get; }

    /// <summary>The ordered rejection reasons.</summary>
    public IReadOnlyList<string> Reasons { get; }
}

/// <summary>An immutable view of intended registrations and rejected candidates.</summary>
public sealed class CompositionPlanProbe
{
    internal CompositionPlanProbe(
        string identity,
        CompositionRegistration[] registrations,
        CompositionCandidateRejection[] rejectedCandidates
    )
    {
        Identity = identity;
        Registrations = new ReadOnlyCollection<CompositionRegistration>(registrations);
        RejectedCandidates = new ReadOnlyCollection<CompositionCandidateRejection>(
            rejectedCandidates
        );
    }

    /// <summary>The plan's stable authored identity.</summary>
    public string Identity { get; }

    /// <summary>Intended registrations in supplied order, before application strategies.</summary>
    public IReadOnlyList<CompositionRegistration> Registrations { get; }

    /// <summary>Candidates rejected during declaration processing.</summary>
    public IReadOnlyList<CompositionCandidateRejection> RejectedCandidates { get; }
}
