namespace HostLoom.Generators;

/// <summary>
/// Generates exact-length identifiers that combine a caller-supplied prefix with a random
/// suffix, for provider or wire formats that constrain total length. Prefixes are rejected, never
/// sanitized: a character outside ASCII letters and digits fails the call rather than being
/// silently rewritten. Output length is always exactly the configured total length.
/// </summary>
public sealed class PrefixedIdentifierGenerator
{
    private readonly int _totalLength;
    private readonly int _minRandomSuffixLength;
    private readonly RandomAlphabet _alphabet;
    private readonly IRandomSource _random;

    /// <summary>
    /// Creates a generator whose output is exactly <paramref name="totalLength"/> characters,
    /// with at least <paramref name="minRandomSuffixLength"/> random suffix characters drawn from
    /// <paramref name="alphabet"/> (default <see cref="RandomAlphabet.AlphaNumeric"/>).
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="minRandomSuffixLength"/> is less than 1, or
    /// <paramref name="totalLength"/> is outside [minRandomSuffixLength + 1, 512].
    /// </exception>
    public PrefixedIdentifierGenerator(
        int totalLength,
        int minRandomSuffixLength = 4,
        RandomAlphabet? alphabet = null,
        IRandomSource? random = null
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(minRandomSuffixLength, 1);
        // int.MaxValue + 1 wraps to int.MinValue and would skip the lower bound.
        if (minRandomSuffixLength == int.MaxValue)
        {
            throw new ArgumentOutOfRangeException(
                nameof(minRandomSuffixLength),
                minRandomSuffixLength,
                "The minimum random suffix length leaves no room for a prefix."
            );
        }

        ArgumentOutOfRangeException.ThrowIfLessThan(totalLength, minRandomSuffixLength + 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(totalLength, 512);

        _totalLength = totalLength;
        _minRandomSuffixLength = minRandomSuffixLength;
        _alphabet = alphabet ?? RandomAlphabet.AlphaNumeric;
        _random = random ?? SecureRandomSource.Instance;
    }

    /// <summary>
    /// Returns how many random characters will follow <paramref name="prefix"/> in the output.
    /// A result below the configured minimum random suffix length means
    /// <see cref="Generate"/> will reject this prefix.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// <paramref name="prefix"/> is null, empty, or contains a character that is not an ASCII
    /// letter or digit.
    /// </exception>
    public int RemainingRandomLength(string prefix)
    {
        ValidatePrefixCharacters(prefix);
        return _totalLength - prefix.Length;
    }

    /// <summary>
    /// Returns <paramref name="prefix"/> followed by random characters selected with the same
    /// unbiased per-character selection as <see cref="RandomStringGenerator"/>, padded to exactly
    /// the configured total length. The prefix is rejected, never sanitized, when it is null or
    /// empty, contains a non-ASCII letter or digit, or leaves fewer random characters than the
    /// configured minimum.
    /// </summary>
    /// <exception cref="ArgumentException">
    /// <paramref name="prefix"/> is null, empty, contains a character that is not an ASCII
    /// letter or digit, or is longer than total length minus the minimum random suffix length.
    /// </exception>
    public string Generate(string prefix)
    {
        ValidatePrefixCharacters(prefix);
        var maxPrefixLength = checked(_totalLength - _minRandomSuffixLength);
        if (prefix.Length > maxPrefixLength)
        {
            throw new ArgumentException(
                $"The prefix is {prefix.Length} characters long; at most "
                    + $"{maxPrefixLength} are allowed so at least "
                    + $"{_minRandomSuffixLength} random characters remain.",
                nameof(prefix)
            );
        }

        var suffixLength = _totalLength - prefix.Length;
        var state = (
            Prefix: prefix,
            Characters: _alphabet.Characters,
            SuffixLength: suffixLength,
            Random: _random
        );
        return string.Create(
            _totalLength,
            state,
            static (destination, state) =>
            {
                state.Prefix.AsSpan().CopyTo(destination);
                for (var index = state.Prefix.Length; index < destination.Length; index++)
                {
                    destination[index] = state.Characters[
                        state.Random.NextInt32(0, state.Characters.Length)
                    ];
                }
            }
        );
    }

    private static void ValidatePrefixCharacters(string prefix)
    {
        ArgumentException.ThrowIfNullOrEmpty(prefix);
        for (var index = 0; index < prefix.Length; index++)
        {
            if (!char.IsAsciiLetterOrDigit(prefix[index]))
            {
                throw new ArgumentException(
                    $"Prefix character at position {index} is not an ASCII letter or digit; prefixes are rejected, never sanitized.",
                    nameof(prefix)
                );
            }
        }
    }
}
