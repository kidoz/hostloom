using HostLoom.Generators;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A <see cref="RandomAlphabet"/> is immutable, ASCII-only, and duplicate-free at construction,
/// and every preset is a versioned compatibility contract: the exact characters and their order
/// are pinned and must never change within a major version.
/// </summary>
public sealed class RandomAlphabetTests
{
    [Fact]
    public void Null_and_empty_alphabets_are_rejected()
    {
        Assert.Throws<ArgumentNullException>(() => new RandomAlphabet(null!));
        Assert.Throws<ArgumentException>(() => new RandomAlphabet(""));
    }

    [Fact]
    public void Duplicate_characters_are_rejected_ordinally()
    {
        var exception = Assert.Throws<ArgumentException>(() => new RandomAlphabet("abca"));
        Assert.Contains("position 3", exception.Message, StringComparison.Ordinal);

        // Case differs, so these are distinct characters under ordinal comparison.
        Assert.Equal("aA", new RandomAlphabet("aA").Characters);
    }

    [Fact]
    public void Non_ascii_characters_are_rejected()
    {
        var exception = Assert.Throws<ArgumentException>(() => new RandomAlphabet("abé"));
        Assert.Contains("position 2", exception.Message, StringComparison.Ordinal);
    }

    [Theory]
    [InlineData("0123456789", "Digits")]
    [InlineData("0123456789abcdef", "HexLower")]
    [InlineData("0123456789ABCDEF", "HexUpper")]
    [InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789", "AlphaNumeric")]
    [InlineData("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_", "UrlSafe")]
    [InlineData("23456789ABCDEFGHJKMNPQRSTUVWXYZ", "HumanReadable")]
    public void Preset_characters_are_pinned_versioned_contracts(string expected, string preset)
    {
        var alphabet = preset switch
        {
            "Digits" => RandomAlphabet.Digits,
            "HexLower" => RandomAlphabet.HexLower,
            "HexUpper" => RandomAlphabet.HexUpper,
            "AlphaNumeric" => RandomAlphabet.AlphaNumeric,
            "UrlSafe" => RandomAlphabet.UrlSafe,
            "HumanReadable" => RandomAlphabet.HumanReadable,
            _ => throw new ArgumentOutOfRangeException(nameof(preset)),
        };

        Assert.Equal(expected, alphabet.Characters);
        Assert.Equal(expected.Length, alphabet.Length);
    }

    [Fact]
    public void Human_readable_excludes_visually_ambiguous_characters()
    {
        Assert.DoesNotContain('0', RandomAlphabet.HumanReadable.Characters);
        Assert.DoesNotContain('O', RandomAlphabet.HumanReadable.Characters);
        Assert.DoesNotContain('1', RandomAlphabet.HumanReadable.Characters);
        Assert.DoesNotContain('I', RandomAlphabet.HumanReadable.Characters);
        Assert.DoesNotContain('L', RandomAlphabet.HumanReadable.Characters);
        Assert.Equal(31, RandomAlphabet.HumanReadable.Length);
    }
}
