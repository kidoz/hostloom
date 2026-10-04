namespace HostLoom.Generators;

/// <summary>
/// Collects a bounded batch of distinct candidate values. Duplicate detection happens after
/// canonicalization and uses one in-memory <see cref="HashSet{T}"/> entry per accepted value,
/// so memory grows with the batch size. There is no storage coordination: durable uniqueness
/// stays an atomic constraint in the owning service, which must independently retry persistence
/// conflicts.
/// </summary>
public static class DistinctBatch
{
    /// <summary>
    /// Draws candidates until <paramref name="count"/> distinct values are collected
    /// (<see cref="DistinctBatchStatus.Success"/>) or the attempt budget runs out
    /// (<see cref="DistinctBatchStatus.Exhausted"/>). Each attempt passes the caller's
    /// cancellation token to <paramref name="candidate"/>; the drawn value is canonicalized when
    /// <paramref name="canonicalize"/> is supplied, skipped when <paramref name="exclude"/>
    /// returns true, and skipped when it already occurs in the batch under
    /// <paramref name="canonicalComparer"/> (or <see cref="EqualityComparer{T}.Default"/>).
    /// <paramref name="maxAttempts"/> defaults to <paramref name="count"/> × 10 computed with
    /// checked arithmetic and must be at least <paramref name="count"/> when supplied.
    /// </summary>
    /// <typeparam name="T">The candidate value type.</typeparam>
    /// <exception cref="ArgumentNullException"><paramref name="candidate"/> is null.</exception>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="count"/> is outside [1, 100000], or <paramref name="maxAttempts"/> is
    /// less than <paramref name="count"/>.
    /// </exception>
    /// <exception cref="OperationCanceledException">
    /// <paramref name="cancellationToken"/> is canceled; the caller's token is preserved.
    /// </exception>
    public static DistinctBatchResult<T> Generate<T>(
        Func<CancellationToken, T> candidate,
        int count,
        IEqualityComparer<T>? canonicalComparer = null,
        Func<T, T>? canonicalize = null,
        Func<T, bool>? exclude = null,
        int? maxAttempts = null,
        CancellationToken cancellationToken = default
    )
    {
        ArgumentNullException.ThrowIfNull(candidate);
        ArgumentOutOfRangeException.ThrowIfLessThan(count, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(count, 100_000);

        int attemptBudget;
        if (maxAttempts is { } supplied)
        {
            ArgumentOutOfRangeException.ThrowIfLessThan(supplied, count);
            attemptBudget = supplied;
        }
        else
        {
            attemptBudget = checked(count * 10);
        }

        var seen = new HashSet<T>(canonicalComparer ?? EqualityComparer<T>.Default);
        var values = new List<T>(count);
        var attempts = 0;

        while (values.Count < count && attempts < attemptBudget)
        {
            cancellationToken.ThrowIfCancellationRequested();
            attempts++;

            var value = candidate(cancellationToken);
            if (canonicalize is not null)
            {
                value = canonicalize(value);
            }

            if (exclude is not null && exclude(value))
            {
                continue;
            }

            if (seen.Add(value))
            {
                values.Add(value);
            }
        }

        var status =
            values.Count == count ? DistinctBatchStatus.Success : DistinctBatchStatus.Exhausted;
        return new DistinctBatchResult<T>(status, values, attempts);
    }
}
