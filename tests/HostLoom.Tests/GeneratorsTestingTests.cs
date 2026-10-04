using HostLoom.Generators;
using HostLoom.Generators.Testing;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// The testing package honors its own contracts: <see cref="SeededRandomSource"/> is repeatable
/// per seed, <see cref="ScriptedRandomSource"/> fails loudly when a test under-scripts, and
/// <see cref="AsyncNumericSequence"/> hands out each incremented value exactly once, even under
/// concurrency, and honors cancellation.
/// </summary>
public sealed class GeneratorsTestingTests
{
    [Fact]
    public void Seeded_sources_with_the_same_seed_produce_identical_strings()
    {
        var profile = new RandomStringProfile(RandomAlphabet.AlphaNumeric, 64);

        var first = new RandomStringGenerator(new SeededRandomSource(42)).Generate(profile);
        var second = new RandomStringGenerator(new SeededRandomSource(42)).Generate(profile);

        Assert.Equal(first, second);
    }

    [Fact]
    public void Seeded_sources_with_different_seeds_produce_different_strings()
    {
        var profile = new RandomStringProfile(RandomAlphabet.AlphaNumeric, 64);

        var first = new RandomStringGenerator(new SeededRandomSource(1)).Generate(profile);
        var second = new RandomStringGenerator(new SeededRandomSource(2)).Generate(profile);

        Assert.NotEqual(first, second);
    }

    [Fact]
    public void A_scripted_source_throws_when_the_value_script_is_exhausted()
    {
        var source = new ScriptedRandomSource(7);

        Assert.Equal(7, source.NextInt64(0, 10));
        Assert.Throws<InvalidOperationException>(() => source.NextInt64(0, 10));
    }

    [Fact]
    public void An_empty_int64_range_returns_the_bound_without_consuming_the_script()
    {
        var source = new ScriptedRandomSource(7);

        Assert.Equal(5, source.NextInt64(5, 5));
        Assert.Equal(7, source.NextInt64(0, 10));
    }

    [Fact]
    public void A_scripted_source_rejects_values_outside_the_requested_range()
    {
        var source = new ScriptedRandomSource(4);

        var exception = Assert.Throws<ArgumentOutOfRangeException>(() => source.NextInt32(0, 4));
        Assert.Contains("outside [0, 4)", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void A_scripted_source_throws_when_the_byte_script_is_exhausted()
    {
        var source = new ScriptedRandomSource().WithBytes(1, 2, 3);
        Span<byte> destination = stackalloc byte[3];

        source.Fill(destination);
        Assert.Equal(new byte[] { 1, 2, 3 }, destination.ToArray());
        Assert.Throws<InvalidOperationException>(() => source.Fill(stackalloc byte[1]));
    }

    [Fact]
    public async Task The_sequence_returns_seed_plus_increment_then_successive_increments()
    {
        var token = TestContext.Current.CancellationToken;
        var sequence = new AsyncNumericSequence();

        Assert.Equal(1, await sequence.NextAsync(token));
        Assert.Equal(2, await sequence.NextAsync(token));
        Assert.Equal(3, await sequence.NextAsync(token));

        var custom = new AsyncNumericSequence(seed: 10, increment: 5);

        Assert.Equal(15, await custom.NextAsync(token));
        Assert.Equal(20, await custom.NextAsync(token));
    }

    [Fact]
    public async Task Concurrent_calls_never_receive_the_same_value()
    {
        var sequence = new AsyncNumericSequence();
        var draws = Enumerable
            .Range(0, 1000)
            .Select(_ => sequence.NextAsync(TestContext.Current.CancellationToken).AsTask())
            .ToArray();

        var values = await Task.WhenAll(draws);

        Assert.Equal(1000, values.Distinct().Count());
        Assert.Equal(Enumerable.Range(1, 1000).Select(i => (long)i), values.Order());
    }

    [Fact]
    public async Task The_sequence_honors_cancellation_before_handing_out_a_value()
    {
        var sequence = new AsyncNumericSequence();
        using var cts = new CancellationTokenSource();
        await cts.CancelAsync();

        await Assert.ThrowsAnyAsync<OperationCanceledException>(async () =>
            await sequence.NextAsync(cts.Token)
        );

        // The canceled call did not advance the sequence.
        Assert.Equal(1, await sequence.NextAsync(TestContext.Current.CancellationToken));
    }
}
