namespace HostLoom.Generators;

/// <summary>The outcome of a <see cref="DistinctBatch.Generate{T}"/> call.</summary>
public enum DistinctBatchStatus
{
    /// <summary>The requested number of distinct values was collected.</summary>
    Success,

    /// <summary>
    /// The attempt budget ran out before enough distinct values were collected. The result
    /// carries the distinct values gathered so far; the caller must not treat it as success.
    /// </summary>
    Exhausted,
}
