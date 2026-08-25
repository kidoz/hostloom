using System.Collections.Concurrent;
using HostLoom.Pipelines;

namespace HostLoom.Examples.Pipelines;

/// <summary>
/// One pipeline run processes one batch of documents. The context is the only mutable state the
/// filters share: metrics written by the analyze stage are read by the summarize stage, and the
/// per-document maps are concurrent because analysis fans out over the batch.
/// </summary>
internal sealed class IndexingContext(
    IReadOnlyList<Document> documents,
    int maxAnalysisParallelism = 4,
    CancellationToken cancellationToken = default
) : PipeContext(cancellationToken)
{
    public IReadOnlyList<Document> Documents { get; } = documents;

    /// <summary>Caps intra-filter fan-out so a batch cannot overload a backing service.</summary>
    public int MaxAnalysisParallelism { get; } = maxAnalysisParallelism;
}

internal sealed class Document(string name, string content)
{
    public string Name { get; } = name;

    public string Content { get; } = content;

    public ConcurrentDictionary<string, decimal> Metrics { get; } = new();

    public ConcurrentDictionary<string, decimal?> Insights { get; } = new();
}
