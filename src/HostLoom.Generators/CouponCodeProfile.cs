namespace HostLoom.Generators;

/// <summary>
/// Immutable profile for human-readable coupon codes: one or more parts of equal length joined
/// by a separator. When <see cref="IncludeCheckDigit"/> is true, the final character of every
/// part is a check digit computed from the rest of the part, so a part contributes
/// partLength − 1 random characters; collision budgets must use that random search space, not
/// the formatted length.
/// </summary>
public sealed class CouponCodeProfile
{
    /// <summary>
    /// Creates a coupon code profile. The alphabet defaults to
    /// <see cref="RandomAlphabet.HumanReadable"/>. The separator must be an ASCII character that
    /// is not a letter or digit, is not whitespace or a control character, and does not occur in
    /// the alphabet.
    /// </summary>
    /// <exception cref="ArgumentOutOfRangeException">
    /// <paramref name="partCount"/> is outside [1, 6], or <paramref name="partLength"/> is
    /// outside [2, 16].
    /// </exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="separator"/> is not acceptable, or <paramref name="alphabet"/> contains
    /// whitespace or a character that invariant uppercasing changes. Normalization trims and
    /// uppercases before validation, so a generated character must survive that step unchanged.
    /// </exception>
    public CouponCodeProfile(
        int partLength = 6,
        int partCount = 1,
        char separator = '-',
        RandomAlphabet? alphabet = null,
        bool includeCheckDigit = false
    )
    {
        ArgumentOutOfRangeException.ThrowIfLessThan(partCount, 1);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(partCount, 6);
        ArgumentOutOfRangeException.ThrowIfLessThan(partLength, 2);
        ArgumentOutOfRangeException.ThrowIfGreaterThan(partLength, 16);

        var effectiveAlphabet = alphabet ?? RandomAlphabet.HumanReadable;

        if (!char.IsAscii(separator) || char.IsWhiteSpace(separator) || char.IsControl(separator))
        {
            throw new ArgumentException(
                "The coupon separator must be a printable ASCII character.",
                nameof(separator)
            );
        }

        if (char.IsAsciiLetterOrDigit(separator))
        {
            throw new ArgumentException(
                "The coupon separator must not be a letter or a digit.",
                nameof(separator)
            );
        }

        if (effectiveAlphabet.Characters.Contains(separator, StringComparison.Ordinal))
        {
            throw new ArgumentException(
                "The coupon separator must not occur in the alphabet.",
                nameof(separator)
            );
        }

        // Normalize uppercases and trims before validation. A character that either step changes
        // cannot round-trip, so reject it here instead of failing a code this generator just wrote.
        for (var index = 0; index < effectiveAlphabet.Characters.Length; index++)
        {
            var character = effectiveAlphabet.Characters[index];
            if (char.IsWhiteSpace(character) || char.ToUpperInvariant(character) != character)
            {
                throw new ArgumentException(
                    $"Alphabet character at position {index} is whitespace or is changed by invariant uppercasing.",
                    nameof(alphabet)
                );
            }
        }

        PartLength = partLength;
        PartCount = partCount;
        Separator = separator;
        Alphabet = effectiveAlphabet;
        IncludeCheckDigit = includeCheckDigit;
    }

    /// <summary>The exact number of characters in every part, including a check digit when enabled.</summary>
    public int PartLength { get; }

    /// <summary>The exact number of parts in every generated code.</summary>
    public int PartCount { get; }

    /// <summary>The single ASCII character written between parts.</summary>
    public char Separator { get; }

    /// <summary>The alphabet every random character (and every check digit) is drawn from.</summary>
    public RandomAlphabet Alphabet { get; }

    /// <summary>
    /// Whether the final character of every part is a check digit. A part then contributes
    /// <see cref="PartLength"/> − 1 random characters.
    /// </summary>
    public bool IncludeCheckDigit { get; }
}
