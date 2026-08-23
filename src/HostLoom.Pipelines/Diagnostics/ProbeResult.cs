using System.Collections.ObjectModel;

namespace HostLoom.Pipelines;

/// <summary>An immutable snapshot of pipeline structure and filter metadata.</summary>
public sealed class ProbeResult
{
    internal ProbeResult(string name, Dictionary<string, object?> properties, List<ProbeResult> children)
    {
        Name = name;
        Properties = new ReadOnlyDictionary<string, object?>(new Dictionary<string, object?>(properties));
        Children = children.AsReadOnly();
    }

    public string Name { get; }
    public IReadOnlyDictionary<string, object?> Properties { get; }
    public IReadOnlyList<ProbeResult> Children { get; }
}
