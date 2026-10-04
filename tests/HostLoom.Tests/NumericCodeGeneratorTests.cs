using System.Globalization;
using HostLoom.Generators;
using HostLoom.Generators.Testing;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A <see cref="NumericCodeGenerator"/> draws from the profile range and formats with the
/// invariant culture only: codes are zero-padded to exactly the profile digits (leading zeros
/// are part of the contract), and the current culture never affects output. A legacy profile
/// that narrows <see cref="NumericCodeProfile.MaxExclusive"/> excludes the final value of the
/// full range.
/// </summary>
public sealed class NumericCodeGeneratorTests
{
    [Fact]
    public void A_scripted_zero_formats_as_all_zero_digits()
    {
        var generator = new NumericCodeGenerator(new ScriptedRandomSource(0));

        Assert.Equal("00000", generator.Generate(new NumericCodeProfile(5)));
    }

    [Fact]
    public void The_full_range_includes_its_final_value()
    {
        var generator = new NumericCodeGenerator(new ScriptedRandomSource(99999));

        Assert.Equal("99999", generator.Generate(new NumericCodeProfile(5)));
    }

    [Fact]
    public void A_legacy_profile_excludes_the_final_value_of_the_full_range()
    {
        var profile = new NumericCodeProfile(5, maxExclusive: 99999);

        // Drawing 99999 is outside [0, 99999), so the strict script rejects it: the endpoint is
        // genuinely unreachable under the legacy profile.
        var rejected = new NumericCodeGenerator(new ScriptedRandomSource(99999));
        Assert.Throws<ArgumentOutOfRangeException>(() => rejected.Generate(profile));

        var accepted = new NumericCodeGenerator(new ScriptedRandomSource(99998));
        Assert.Equal("99998", accepted.Generate(profile));
    }

    [Fact]
    public void Leading_zeros_are_preserved()
    {
        var generator = new NumericCodeGenerator(new ScriptedRandomSource(123));

        Assert.Equal("00123", generator.Generate(new NumericCodeProfile(5)));
    }

    [Fact]
    public void Formatting_is_invariant_under_the_current_culture()
    {
        var previous = CultureInfo.CurrentCulture;
        try
        {
            CultureInfo.CurrentCulture = CultureInfo.GetCultureInfo("tr-TR");

            var padded = new NumericCodeGenerator(new ScriptedRandomSource(42));
            Assert.Equal("00042", padded.Generate(new NumericCodeProfile(5)));

            var identifier = new NumericCodeGenerator(new ScriptedRandomSource(1_234_567_890L));
            Assert.Equal(
                "1234567890",
                identifier.Generate(new NumericIdProfile(1_000_000_000L, 10_000_000_000L))
            );
        }
        finally
        {
            CultureInfo.CurrentCulture = previous;
        }
    }

    [Fact]
    public void A_ten_digit_identifier_never_starts_with_zero()
    {
        var generator = new NumericCodeGenerator();
        var profile = new NumericIdProfile(1_000_000_000L, 10_000_000_000L);

        for (var index = 0; index < 256; index++)
        {
            var value = generator.Generate(profile);
            Assert.Equal(10, value.Length);
            Assert.InRange(value[0], '1', '9');
        }
    }

    [Fact]
    public void Invalid_code_profiles_are_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new NumericCodeProfile(0));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NumericCodeProfile(19));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new NumericCodeProfile(5, maxExclusive: 0)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new NumericCodeProfile(2, maxExclusive: 101)
        );
    }

    [Fact]
    public void Invalid_identifier_profiles_are_rejected()
    {
        Assert.Throws<ArgumentOutOfRangeException>(() => new NumericIdProfile(0, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NumericIdProfile(10, 10));
        Assert.Throws<ArgumentOutOfRangeException>(() => new NumericIdProfile(11, 10));
    }

    [Fact]
    public void Null_profiles_are_rejected()
    {
        var generator = new NumericCodeGenerator();
        Assert.Throws<ArgumentNullException>(() => generator.Generate((NumericCodeProfile)null!));
        Assert.Throws<ArgumentNullException>(() => generator.Generate((NumericIdProfile)null!));
    }
}
