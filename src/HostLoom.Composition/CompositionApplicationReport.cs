using System.Collections.ObjectModel;
using Microsoft.Extensions.DependencyInjection;

namespace HostLoom.Composition;

/// <summary>The effect of an application decision on a descriptor.</summary>
public enum CompositionApplicationOutcome
{
    /// <summary>The descriptor was added.</summary>
    Added,

    /// <summary>The incoming descriptor was skipped.</summary>
    Skipped,

    /// <summary>The existing descriptor was removed by a replacement.</summary>
    Replaced,
}

/// <summary>A passive record of an application action, in execution order.</summary>
public sealed record CompositionApplicationDecision(
    ServiceDescriptor Descriptor,
    CompositionOrigin Origin,
    CompositionApplicationOutcome Outcome,
    string Reason,
    CompositionOrigin? PreviousOrigin = null
);

/// <summary>Immutable effects of a successful plan application; not a live container inventory.</summary>
public sealed class CompositionApplicationReport
{
    private readonly IReadOnlyList<CompositionApplicationDecision> _decisions;

    internal CompositionApplicationReport(
        string identity,
        IEnumerable<CompositionApplicationDecision> decisions
    )
    {
        Identity = identity;
        _decisions = new ReadOnlyCollection<CompositionApplicationDecision>(decisions.ToArray());
    }

    /// <summary>The applied plan identity.</summary>
    public string Identity { get; }

    /// <summary>Returns the same immutable action snapshot without resolving any service.</summary>
    public IReadOnlyList<CompositionApplicationDecision> Probe() => _decisions;
}
