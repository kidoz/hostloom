namespace HostLoom.Composition;

/// <summary>Identifies the authored rule responsible for a registration or rejection.</summary>
public sealed record CompositionOrigin
{
    /// <summary>Creates an origin without reading source files or capturing checkout paths.</summary>
    public CompositionOrigin(
        string rule,
        string? group = null,
        string? filePath = null,
        int? line = null,
        string? selector = null
    )
    {
        ArgumentException.ThrowIfNullOrWhiteSpace(rule);
        if (line is <= 0)
        {
            throw new ArgumentOutOfRangeException(nameof(line));
        }

        Rule = rule;
        Group = group;
        FilePath = filePath;
        Line = line;
        Selector = selector;
    }

    /// <summary>The declaring method or rule identifier.</summary>
    public string Rule { get; }

    /// <summary>The normalized candidate source and filter declaration, without runtime evaluation.</summary>
    public string? Selector { get; }

    /// <summary>The optional group within the plan.</summary>
    public string? Group { get; }

    /// <summary>The optional normalized, project-relative source path.</summary>
    public string? FilePath { get; }

    /// <summary>The optional one-based source line.</summary>
    public int? Line { get; }
}
