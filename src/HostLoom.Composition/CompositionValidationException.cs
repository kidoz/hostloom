namespace HostLoom.Composition;

/// <summary>The passive validation phase that rejected a plan.</summary>
public enum CompositionValidationPhase
{
    /// <summary>The explicit plan's own registrations conflict.</summary>
    PlanConstruction,

    /// <summary>The plan conflicts with the target collection.</summary>
    Application,
}

/// <summary>A composition failure with available authored provenance.</summary>
public sealed class CompositionValidationException : InvalidOperationException
{
    internal CompositionValidationException(
        string identity,
        CompositionValidationPhase phase,
        string message,
        CompositionOrigin? origin = null,
        CompositionOrigin? existingOrigin = null
    )
        : base($"Composition plan '{identity}' ({phase}): {message}")
    {
        Identity = identity;
        Phase = phase;
        Origin = origin;
        ExistingOrigin = existingOrigin;
    }

    /// <summary>The rejected plan identity.</summary>
    public string Identity { get; }

    /// <summary>The phase that found the conflict.</summary>
    public CompositionValidationPhase Phase { get; }

    /// <summary>The incoming authored origin, when one applies.</summary>
    public CompositionOrigin? Origin { get; }

    /// <summary>The other origin, when the descriptor came from a known plan.</summary>
    public CompositionOrigin? ExistingOrigin { get; }
}
