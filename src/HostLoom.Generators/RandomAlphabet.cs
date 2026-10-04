namespace HostLoom.Generators;

/// <summary>
/// An immutable, validated set of distinct ASCII characters that random strings are drawn from.
/// Duplicate or non-ASCII characters are rejected at construction, so every character has equal
/// selection weight. The named presets are versioned compatibility contracts: their exact
/// characters and order are pinned by tests and never change within a major version.
/// </summary>
public sealed class RandomAlphabet
{
    /// <summary>
    /// Creates an alphabet from <paramref name="characters"/>.
    /// </summary>
    /// <exception cref="ArgumentNullException"><paramref name="characters"/> is null.</exception>
    /// <exception cref="ArgumentException">
    /// <paramref name="characters"/> is empty, contains a duplicate character (ordinal), or
    /// contains a non-ASCII character. Messages identify the offending position and never echo
    /// the input.
    /// </exception>
    public RandomAlphabet(string characters)
    {
        ArgumentException.ThrowIfNullOrEmpty(characters);

        var seen = new HashSet<char>();
        for (var index = 0; index < characters.Length; index++)
        {
            var character = characters[index];
            if (!char.IsAscii(character))
            {
                throw new ArgumentException(
                    $"Alphabet character at position {index} is not ASCII; alphabets are ASCII-only.",
                    nameof(characters)
                );
            }

            if (!seen.Add(character))
            {
                throw new ArgumentException(
                    $"Alphabet character at position {index} duplicates an earlier character (ordinal comparison).",
                    nameof(characters)
                );
            }
        }

        Characters = characters;
    }

    /// <summary>The alphabet characters in their pinned order.</summary>
    public string Characters { get; }

    /// <summary>The number of characters in the alphabet; the selection weight of each is equal.</summary>
    public int Length => Characters.Length;

    /// <summary>
    /// Digits <c>0123456789</c>. A versioned compatibility contract: the exact characters and
    /// order are pinned by tests.
    /// </summary>
    public static RandomAlphabet Digits { get; } = new("0123456789");

    /// <summary>
    /// Lowercase hexadecimal <c>0123456789abcdef</c>. A versioned compatibility contract: the
    /// exact characters and order are pinned by tests.
    /// </summary>
    public static RandomAlphabet HexLower { get; } = new("0123456789abcdef");

    /// <summary>
    /// Uppercase hexadecimal <c>0123456789ABCDEF</c>. A versioned compatibility contract: the
    /// exact characters and order are pinned by tests.
    /// </summary>
    public static RandomAlphabet HexUpper { get; } = new("0123456789ABCDEF");

    /// <summary>
    /// ASCII letters followed by digits
    /// <c>ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789</c>. A versioned
    /// compatibility contract: the exact characters and order are pinned by tests.
    /// </summary>
    public static RandomAlphabet AlphaNumeric { get; } =
        new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789");

    /// <summary>
    /// URL-safe Base64Url characters
    /// <c>ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_</c>. A versioned
    /// compatibility contract: the exact characters and order are pinned by tests.
    /// </summary>
    public static RandomAlphabet UrlSafe { get; } =
        new("ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_");

    /// <summary>
    /// Thirty-one characters, <c>23456789ABCDEFGHJKMNPQRSTUVWXYZ</c>, chosen to exclude the
    /// visually ambiguous <c>0</c>/<c>O</c>, <c>1</c>/<c>I</c>, and <c>L</c> so codes survive
    /// manual transcription. A versioned compatibility contract: the exact characters and order
    /// are pinned by tests.
    /// </summary>
    public static RandomAlphabet HumanReadable { get; } = new("23456789ABCDEFGHJKMNPQRSTUVWXYZ");
}
