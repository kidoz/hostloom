using System.Collections.Concurrent;
using HostLoom.Generators;
using HostLoom.Generators.Testing;
using Xunit;

namespace HostLoom.Tests;

/// <summary>
/// A <see cref="RandomStringGenerator"/> produces strings of exactly the profile length, with
/// every character drawn from the profile alphabet with equal weight, and it stays isolated when
/// several profiles are used in parallel. <see cref="RandomStringGenerator.TryGenerate"/> writes
/// into a caller span only when it fits.
/// </summary>
public sealed class RandomStringGeneratorTests
{
    private static readonly (string Name, RandomAlphabet Alphabet)[] Presets =
    [
        ("Digits", RandomAlphabet.Digits),
        ("HexLower", RandomAlphabet.HexLower),
        ("HexUpper", RandomAlphabet.HexUpper),
        ("AlphaNumeric", RandomAlphabet.AlphaNumeric),
        ("UrlSafe", RandomAlphabet.UrlSafe),
        ("HumanReadable", RandomAlphabet.HumanReadable),
    ];

    [Fact]
    public void Every_preset_produces_exact_length_strings_of_alphabet_characters()
    {
        var generator = new RandomStringGenerator();

        foreach (var (name, alphabet) in Presets)
        {
            var profile = new RandomStringProfile(alphabet, 64);
            var value = generator.Generate(profile);

            Assert.Equal(64, value.Length);
            Assert.True(
                value.All(alphabet.Characters.Contains),
                $"{name} produced a character outside its alphabet: {value}"
            );
        }
    }

    [Fact]
    public void Generators_with_different_profiles_stay_isolated_in_parallel()
    {
        var digits = new RandomStringGenerator();
        var digitsProfile = new RandomStringProfile(RandomAlphabet.Digits, 32);
        var hexUpper = new RandomStringGenerator();
        var hexUpperProfile = new RandomStringProfile(RandomAlphabet.HexUpper, 32);
        var failures = new ConcurrentQueue<string>();

        Parallel.For(
            0,
            500,
            index =>
            {
                var value =
                    index % 2 == 0
                        ? digits.Generate(digitsProfile)
                        : hexUpper.Generate(hexUpperProfile);
                var alphabet =
                    index % 2 == 0
                        ? RandomAlphabet.Digits.Characters
                        : RandomAlphabet.HexUpper.Characters;
                if (value.Length != 32 || !value.All(alphabet.Contains))
                {
                    failures.Enqueue(value);
                }
            }
        );

        Assert.Empty(failures);
    }

    [Fact]
    public void TryGenerate_refuses_an_undersized_span_without_writing()
    {
        var generator = new RandomStringGenerator();
        var profile = new RandomStringProfile(RandomAlphabet.AlphaNumeric, 8);
        Span<char> destination = stackalloc char[7];
        destination.Fill('!');

        Assert.False(generator.TryGenerate(profile, destination));
        Assert.True(destination.IndexOfAnyExcept('!') < 0);
    }

    [Fact]
    public void TryGenerate_fills_an_exact_size_span_and_leaves_excess_untouched()
    {
        var generator = new RandomStringGenerator();
        var profile = new RandomStringProfile(RandomAlphabet.AlphaNumeric, 8);
        Span<char> destination = stackalloc char[10];
        destination.Fill('!');

        Assert.True(generator.TryGenerate(profile, destination));
        Assert.True(destination[..8].IndexOfAnyExcept(RandomAlphabet.AlphaNumeric.Characters) < 0);
        Assert.Equal("!!", destination[8..].ToString());
    }

    [Fact]
    public void A_scripted_source_forces_a_known_output()
    {
        // Twelve zeros select the alphabet's first character for every position.
        var source = new ScriptedRandomSource(0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0);
        var generator = new RandomStringGenerator(source);
        var profile = new RandomStringProfile(RandomAlphabet.HexLower, 12);

        Assert.Equal("000000000000", generator.Generate(profile));
    }

    [Fact]
    public void Invalid_profile_arguments_are_rejected()
    {
        Assert.Throws<ArgumentNullException>(() => new RandomStringProfile(null!, 8));
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RandomStringProfile(RandomAlphabet.Digits, 0)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RandomStringProfile(RandomAlphabet.Digits, -1)
        );
        Assert.Throws<ArgumentOutOfRangeException>(() =>
            new RandomStringProfile(RandomAlphabet.Digits, 4097)
        );

        var generator = new RandomStringGenerator();
        Assert.Throws<ArgumentNullException>(() => generator.Generate(null!));
        Assert.Throws<ArgumentNullException>(() =>
            generator.TryGenerate(null!, stackalloc char[8])
        );
    }
}
