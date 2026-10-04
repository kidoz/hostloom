using HostLoom.Generators;
using HostLoom.Generators.Testing;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A <see cref="CouponCodeGenerator"/> emits exactly <c>PartCount</c> parts of
/// <c>PartLength</c> alphabet characters joined by the separator. With check digits enabled the
/// last character of a part is <c>alphabet[(p + Σ (i+1)·a(ci)) mod alphabet.Length]</c>, which
/// makes the check position-aware; <see cref="CouponCodeGenerator.Normalize"/> only trims and
/// uppercases, it never validates.
/// </summary>
public sealed class CouponCodeGeneratorTests
{
    [Fact]
    public void A_scripted_source_forces_a_deterministic_single_part_code()
    {
        var generator = new CouponCodeGenerator(new ScriptedRandomSource(0, 0, 0, 0, 0, 0));
        var profile = new CouponCodeProfile(partLength: 6, partCount: 1, includeCheckDigit: false);

        var code = generator.Generate(profile);

        Assert.Equal(6, code.Length);
        Assert.Equal("222222", code);
        Assert.True(code.All(RandomAlphabet.HumanReadable.Characters.Contains));
        Assert.Equal(code, code.ToUpperInvariant());
    }

    [Fact]
    public void A_one_by_eight_profile_produces_eight_characters()
    {
        var generator = new CouponCodeGenerator(new ScriptedRandomSource(0, 1, 2, 3, 4, 5, 6, 7));
        var profile = new CouponCodeProfile(partLength: 8, partCount: 1, includeCheckDigit: false);

        Assert.Equal("23456789", generator.Generate(profile));
    }

    [Fact]
    public void Multi_part_codes_join_parts_with_the_separator()
    {
        var generator = new CouponCodeGenerator(
            new ScriptedRandomSource(0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11)
        );
        var profile = new CouponCodeProfile(partLength: 4, partCount: 3, includeCheckDigit: false);

        var code = generator.Generate(profile);

        Assert.Equal("2345-6789-ABCD", code);
        var parts = code.Split('-');
        Assert.Equal(3, parts.Length);
        Assert.All(parts, part => Assert.Equal(4, part.Length));
    }

    [Fact]
    public void Normalize_trims_and_uppercases_without_validating()
    {
        // '0', 'O', and '!' are outside the alphabet; normalization still accepts them.
        Assert.Equal("AB-0O!", CouponCodeGenerator.Normalize("  ab-0o! "));
        Assert.Throws<ArgumentNullException>(() => CouponCodeGenerator.Normalize(null!));
    }

    [Fact]
    public void A_fully_scripted_code_with_check_digits_validates()
    {
        var profile = new CouponCodeProfile(partLength: 5, partCount: 2, includeCheckDigit: true);
        var generator = new CouponCodeGenerator(new ScriptedRandomSource(0, 1, 2, 3, 4, 5, 6, 7));

        // Expected check characters computed independently from the documented algorithm.
        var code = generator.Generate(profile);

        Assert.Equal(
            "2345" + CheckCharacter(0, [0, 1, 2, 3]) + "-6789" + CheckCharacter(1, [4, 5, 6, 7]),
            code
        );
        Assert.Equal("2345P-6789Z", code);
        Assert.True(generator.TryValidate(code, profile, out var normalized));
        Assert.Equal(code, normalized);
    }

    [Fact]
    public void Validation_accepts_a_normalized_equivalent_and_echoes_the_normalized_input()
    {
        var profile = new CouponCodeProfile(partLength: 5, partCount: 2, includeCheckDigit: true);
        var generator = new CouponCodeGenerator();

        Assert.True(generator.TryValidate(" 2345p-6789z ", profile, out var normalized));
        Assert.Equal("2345P-6789Z", normalized);
    }

    [Fact]
    public void A_flipped_check_character_fails_validation()
    {
        var profile = new CouponCodeProfile(partLength: 5, partCount: 2, includeCheckDigit: true);
        var generator = new CouponCodeGenerator();

        Assert.False(generator.TryValidate("2345X-6789Z", profile, out var normalized));
        Assert.Null(normalized);
    }

    [Fact]
    public void The_same_characters_in_a_different_part_position_fail_validation()
    {
        var profile = new CouponCodeProfile(partLength: 5, partCount: 2, includeCheckDigit: true);
        var generator = new CouponCodeGenerator();

        // The check digit embeds the part index, so swapping the parts breaks both checks.
        Assert.False(generator.TryValidate("6789Z-2345P", profile, out _));
    }

    [Fact]
    public void Wrong_part_count_length_or_alphabet_characters_fail_validation()
    {
        var profile = new CouponCodeProfile(partLength: 5, partCount: 2, includeCheckDigit: true);
        var generator = new CouponCodeGenerator();

        Assert.False(generator.TryValidate("2345P", profile, out _));
        Assert.False(generator.TryValidate("2345P-6789Z-2345P", profile, out _));
        Assert.False(generator.TryValidate("234P-6789Y", profile, out _));
        // '0' is outside the human-readable alphabet.
        Assert.False(generator.TryValidate("2340P-6789Z", profile, out _));
        Assert.False(generator.TryValidate(null, profile, out _));
        Assert.False(generator.TryValidate("", profile, out _));
    }

    [Fact]
    public void An_alphabet_that_normalization_changes_is_rejected()
    {
        Assert.Throws<ArgumentException>(() =>
            new CouponCodeProfile(alphabet: RandomAlphabet.HexLower)
        );
        Assert.Throws<ArgumentException>(() =>
            new CouponCodeProfile(alphabet: RandomAlphabet.AlphaNumeric)
        );
        Assert.Throws<ArgumentException>(() =>
            new CouponCodeProfile(alphabet: new RandomAlphabet("ABCD "))
        );
    }

    [Fact]
    public void An_uppercase_alphabet_accepts_typed_lowercase()
    {
        var profile = new CouponCodeProfile(
            partLength: 4,
            partCount: 1,
            alphabet: RandomAlphabet.HexUpper,
            includeCheckDigit: true
        );
        var generator = new CouponCodeGenerator(new ScriptedRandomSource(10, 11, 12));

        var code = generator.Generate(profile);

        Assert.Equal("ABC4", code);
        Assert.True(generator.TryValidate(" abc4 ", profile, out var normalized));
        Assert.Equal(code, normalized);
    }

    [Fact]
    public void Null_profiles_are_rejected()
    {
        var generator = new CouponCodeGenerator();
        Assert.Throws<ArgumentNullException>(() => generator.Generate(null!));
        Assert.Throws<ArgumentNullException>(() =>
            generator.TryValidate("2345P-6789Z", null!, out _)
        );
    }

    private static char CheckCharacter(int partIndex, ReadOnlySpan<int> alphabetIndices)
    {
        var alphabet = RandomAlphabet.HumanReadable.Characters;
        var sum = partIndex;
        for (var index = 0; index < alphabetIndices.Length; index++)
        {
            sum += (index + 1) * alphabetIndices[index];
        }

        return alphabet[sum % alphabet.Length];
    }
}
