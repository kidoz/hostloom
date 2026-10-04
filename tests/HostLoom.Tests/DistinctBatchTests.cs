using HostLoom.Generators;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// <see cref="DistinctBatch"/> draws candidates until enough distinct values are collected or
/// the attempt budget runs out. Canonicalization and exclusion happen before dedupe, an
/// exhausted budget returns only the distinct values gathered so far, and the caller's
/// cancellation token is honored.
/// </summary>
public sealed class DistinctBatchTests
{
    [Fact]
    public void Duplicate_candidates_are_skipped_until_distinct_ones_arrive()
    {
        var candidates = new Queue<string>(["AB7", "AB7", "CD9", "EF2"]);

        var result = DistinctBatch.Generate(
            _ => candidates.Dequeue(),
            count: 3,
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(DistinctBatchStatus.Success, result.Status);
        Assert.Equal(["AB7", "CD9", "EF2"], result.Values);
        Assert.Equal(4, result.Attempts);
        Assert.Equal(result.Values.Count, result.Values.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void The_exclude_predicate_is_honored_before_dedupe()
    {
        var candidates = new Queue<string>(["AB7", "SKIP", "CD9"]);

        var result = DistinctBatch.Generate(
            _ => candidates.Dequeue(),
            count: 2,
            exclude: value => value == "SKIP",
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(DistinctBatchStatus.Success, result.Status);
        Assert.Equal(["AB7", "CD9"], result.Values);
        Assert.Equal(3, result.Attempts);
    }

    [Fact]
    public void Canonicalization_collapses_duplicates_before_dedupe()
    {
        var candidates = new Queue<string>(["ab7", "AB7", "cd9"]);

        var result = DistinctBatch.Generate(
            _ => candidates.Dequeue(),
            count: 2,
            canonicalComparer: StringComparer.OrdinalIgnoreCase,
            canonicalize: value => value.ToUpperInvariant(),
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(DistinctBatchStatus.Success, result.Status);
        Assert.Equal(["AB7", "CD9"], result.Values);
        Assert.Equal(3, result.Attempts);
    }

    [Fact]
    public void An_exhausted_budget_returns_only_the_distinct_values_gathered()
    {
        var result = DistinctBatch.Generate(
            _ => "AB7",
            count: 3,
            maxAttempts: 5,
            cancellationToken: TestContext.Current.CancellationToken
        );

        Assert.Equal(DistinctBatchStatus.Exhausted, result.Status);
        Assert.Equal(["AB7"], result.Values);
        Assert.Equal(5, result.Attempts);
        Assert.Equal(result.Values.Count, result.Values.Distinct(StringComparer.Ordinal).Count());
    }

    [Fact]
    public void The_cancellation_token_is_honored()
    {
        using var cts = new CancellationTokenSource();
        cts.Cancel();

        Assert.ThrowsAny<OperationCanceledException>(() =>
            DistinctBatch.Generate(_ => "AB7", count: 3, cancellationToken: cts.Token)
        );
    }

    [Fact]
    public void The_cancellation_token_reaches_the_candidate()
    {
        using var cts = new CancellationTokenSource();
        var result = DistinctBatch.Generate(
            token =>
            {
                cts.Cancel();
                return token.IsCancellationRequested ? "seen" : "missed";
            },
            count: 1,
            cancellationToken: cts.Token
        );

        Assert.Equal(DistinctBatchStatus.Success, result.Status);
        Assert.Equal(["seen"], result.Values);
    }

    [Fact]
    public void Invalid_arguments_are_rejected()
    {
        var token = TestContext.Current.CancellationToken;

        Assert.Throws<ArgumentNullException>(() =>
            DistinctBatch.Generate<string>(null!, count: 1, cancellationToken: token)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DistinctBatch.Generate(_ => "AB7", count: 0, cancellationToken: token)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DistinctBatch.Generate(_ => "AB7", count: -1, cancellationToken: token)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DistinctBatch.Generate(_ => "AB7", count: 100_001, cancellationToken: token)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            DistinctBatch.Generate(_ => "AB7", count: 3, maxAttempts: 2, cancellationToken: token)
        );
    }
}
