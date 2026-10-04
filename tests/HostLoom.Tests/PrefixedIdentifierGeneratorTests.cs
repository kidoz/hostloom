using HostLoom.Generators;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A <see cref="PrefixedIdentifierGenerator"/> pads a caller prefix with random suffix
/// characters to exactly the configured total length. Prefixes are rejected, never sanitized:
/// null, empty, or any character outside ASCII letters and digits fails the call, and the
/// prefix may never consume the minimum random suffix budget.
/// </summary>
public sealed class PrefixedIdentifierGeneratorTests
{
    [Fact]
    public void A_prefix_is_padded_to_exactly_the_total_length()
    {
        var generator = new PrefixedIdentifierGenerator(totalLength: 20);

        var identifier = generator.Generate("ORD");

        Assert.Equal(20, identifier.Length);
        Assert.StartsWith("ORD", identifier, StringComparison.Ordinal);
        Assert.True(identifier[3..].All(RandomAlphabet.AlphaNumeric.Characters.Contains));
    }

    [Fact]
    public void Remaining_random_length_reports_the_suffix_budget()
    {
        var generator = new PrefixedIdentifierGenerator(totalLength: 20);

        Assert.Equal(17, generator.RemainingRandomLength("ORD"));
        // Below the default minimum of 4, but the query itself does not throw; Generate does.
        Assert.Equal(3, generator.RemainingRandomLength("ABCDEFGHIJKLMNOPQ"));
    }

    [Fact]
    public void Null_and_empty_prefixes_are_rejected()
    {
        var generator = new PrefixedIdentifierGenerator(totalLength: 20);

        Assert.Throws<ArgumentNullException>(() => generator.Generate(null!));
        Assert.Throws<ArgumentException>(() => generator.Generate(""));
    }

    [Fact]
    public void Non_ascii_letters_and_punctuation_are_rejected_not_sanitized()
    {
        var generator = new PrefixedIdentifierGenerator(totalLength: 20);

        Assert.Throws<ArgumentException>(() => generator.Generate("Ä"));
        Assert.Throws<ArgumentException>(() => generator.Generate("OR-"));
        Assert.Throws<ArgumentException>(() => generator.RemainingRandomLength("OR D"));
    }

    [Fact]
    public void A_prefix_consuming_the_random_budget_is_rejected()
    {
        var generator = new PrefixedIdentifierGenerator(totalLength: 20);

        // 17 prefix characters leave 3 random characters, below the minimum of 4.
        var exception = Assert.Throws<ArgumentException>(() =>
            generator.Generate("ABCDEFGHIJKLMNOPQ")
        );
        Assert.Contains("17 characters", exception.Message, StringComparison.Ordinal);
    }

    [Fact]
    public void Invalid_constructor_arguments_are_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PrefixedIdentifierGenerator(totalLength: 20, minRandomSuffixLength: 0)
        );
        // Too small: nothing left for a prefix once the minimum suffix is reserved.
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PrefixedIdentifierGenerator(totalLength: 4, minRandomSuffixLength: 4)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new PrefixedIdentifierGenerator(totalLength: 513)
        );
    }
}
