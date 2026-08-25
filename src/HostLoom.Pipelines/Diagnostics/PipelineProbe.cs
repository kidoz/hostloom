namespace HostLoom.Pipelines;

public static class PipelineProbe
{
    public static ProbeResult Inspect<TContext>(
        IPipe<TContext> pipe,
        CancellationToken cancellationToken = default
    )
        where TContext : class, IPipeContext
    {
        ArgumentNullException.ThrowIfNull(pipe);
        var root = new ProbeNode("pipeline");
        pipe.Probe(new ProbeContext(root, cancellationToken));
        return root.ToResult();
    }

    private sealed class ProbeContext(ProbeNode node, CancellationToken cancellationToken)
        : IProbeContext
    {
        public CancellationToken CancellationToken { get; } = cancellationToken;

        public IProbeContext CreateScope(string name)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(name);
            CancellationToken.ThrowIfCancellationRequested();
            var child = new ProbeNode(name);
            node.Children.Add(child);
            return new ProbeContext(child, CancellationToken);
        }

        public void Set(string key, object? value)
        {
            ArgumentException.ThrowIfNullOrWhiteSpace(key);
            CancellationToken.ThrowIfCancellationRequested();
            node.Properties[key] = value;
        }
    }

    private sealed class ProbeNode(string name)
    {
        public string Name { get; } = name;
        public Dictionary<string, object?> Properties { get; } = [];
        public List<ProbeNode> Children { get; } = [];

        public ProbeResult ToResult() =>
            new(Name, Properties, Children.Select(child => child.ToResult()).ToList());
    }
}
