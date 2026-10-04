namespace HostLoom.Generators;

/// <summary>
/// The result of a <see cref="DistinctBatch.Generate{T}"/> call. Always inspect
/// <see cref="Status"/> before using <see cref="Values"/>: an exhausted batch returns the
/// distinct values gathered so far, never duplicates and never a silent partial success.
/// </summary>
/// <typeparam name="T">The candidate value type.</typeparam>
public sealed class DistinctBatchResult<T>
{
    internal DistinctBatchResult(DistinctBatchStatus status, IReadOnlyList<T> values, int attempts)
    {
        Status = status;
        Values = values;
        Attempts = attempts;
    }

    /// <summary>Whether the requested number of distinct values was collected.</summary>
    public DistinctBatchStatus Status { get; }

    /// <summary>
    /// The distinct values collected. On <see cref="DistinctBatchStatus.Exhausted"/> this is the
    /// partial batch gathered so far; it never contains duplicates.
    /// </summary>
    public IReadOnlyList<T> Values { get; }

    /// <summary>How many candidate draws were made, including rejected duplicates and exclusions.</summary>
    public int Attempts { get; }
}
